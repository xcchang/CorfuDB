package org.corfudb.runtime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.AddressLoaderRequest;
import org.corfudb.protocols.wireprotocol.ILogData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * The address loader is the layer responsible for loading all addresses from the log unit(s).
 *
 * It provides controlled access to the space of shared addresses for multiple threads.
 */
public class AddressLoader implements Runnable {
    // Pending Reads Queue
    volatile BlockingQueue<Long> pendingReadsQueue;

    // Map for indexing: address to all read requests that include it.
    volatile Map<Long, List<AddressLoaderRequest>> addressToRequests;

    // Read Function
    volatile Function<Set<Long>, Map<Long, ILogData>> readFunction;

    // Write Cache
    @Getter
    final Cache<Long, ILogData> writeCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .recordStats()
            .build();

    LoadingCache<Long, ILogData> readCache;

    volatile boolean shutdown = false;

    Queue<Set<Long>> buffer;

    final int batchSize = 50;
    final int numReaders = 3;

    /**
     * To dispatch tasks for failure or healed nodes detection.
     */
    @Getter
    private final ExecutorService addressLoaderFetchWorker;

    public AddressLoader(Function<Set<Long>, Map<Long, ILogData>> readFunction, LoadingCache<Long, ILogData> readCache) {
        // TODO: should it be bounded?
        this.pendingReadsQueue = new LinkedBlockingQueue<>();
        this.buffer = new LinkedBlockingQueue<>(numReaders);
        this.addressToRequests = new ConcurrentHashMap<>();
        this.readFunction = readFunction;
        this.addressLoaderFetchWorker = Executors.newFixedThreadPool(
                numReaders,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("fetcher-%d")
                        .build()
        );

        this.addressLoaderFetchWorker.submit(this::fetch);
        this.readCache = readCache;
    }

    /**
     * Enqueue addresses to read in order to be consumed by the address loader fetcher.
     *
     * @param addresses list of addresses to be read.
     * @return map of addresses to log data.
     */
    public synchronized CompletableFuture<Map<Long, ILogData>> enqueueReads(List<Long> addresses) {

        // 1. Generate Address Loader Request and Completable Future
        CompletableFuture<Map<Long, ILogData>> cf = new CompletableFuture<>();
        AddressLoaderRequest request = new AddressLoaderRequest(addresses, cf);

        // 2. Add mappings for tracking purposes (service requests as reads are received)
        addresses.forEach(address -> {
            if (this.addressToRequests.containsKey(address)) {
                this.addressToRequests.get(address).add(request);
            } else {
                List<AddressLoaderRequest> requests = new ArrayList<>();
                requests.add(request);
                this.addressToRequests.put(address, requests);
            }
        });

        // 3. Add to queue
        // TODO: if bounded capacity of queue, this needs to change.
        this.pendingReadsQueue.addAll(addresses);

        // 4. Return a future which will complete when all address reads are available for this request
        return cf;
    }

    @Override
    public void run() {
        this.batcher();
    }

    // Batcher: generate batches of read addresses to be sent to consumer threads
    // this is required to maintain unique elements in the queue (avoid reading the same
    // address repeatedly)
    public void batcher() {
        Set<Long> currentBatch = new HashSet<>();
        boolean insert;

        while (!shutdown) {
            int counter = 0;
            Set<Long> fetchBatch = new HashSet<>();
            currentBatch.clear();
            do {
                Long read = pendingReadsQueue.poll();
                if (read != null) {
                    ILogData readValue = this.readCache.getIfPresent(read);
                    if (readValue == null) {
                        if (!currentBatch.contains(read)) {
                            counter++;
                            currentBatch.add(read);
                            fetchBatch.add(read);
                            if (counter % batchSize == 0) {
                                do {
                                    insert = buffer.offer(fetchBatch);
                                } while (!insert);
                                fetchBatch = new HashSet<>();
                            }
                        }
                    }
                } else {
                    if (!fetchBatch.isEmpty()) {
                        do {
                            insert = buffer.offer(fetchBatch);
                        } while (!insert);
                    }
                    break;
                }
            } while (currentBatch.size() < (batchSize * numReaders));
        }
    }

    // Running in a background thread
    public void fetch() {
        try {
            while (!shutdown) {
                if (!this.buffer.isEmpty()) {

                    // 1. Fetch batches of addresses from the queue
                    Set<Long> batch = this.buffer.poll();

                    if (batch != null && !batch.isEmpty()) {
                        // 2. Check if any address is already available in writeCache and remove from the batch
                        // TODO (if I remove maybe I can borrow from another partition).
                        Map<Long, ILogData> dataMap = verifyAddressInWrites(batch);

                        // 3. Read
                        if (!batch.isEmpty()) {
                            Map<Long, ILogData> readsData = this.readFunction.apply(batch);
                            dataMap.putAll(readsData);
                        }

                        // 4. Remove from the queue
                        pendingReadsQueue.removeAll(dataMap.keySet());

                        dataMap.putAll(verifyAddressInWrites(batch));

                        // 5. Dispatch
                        dispatch(dataMap);
                    }
                }
            }
        } catch (Throwable t) {
            System.out.println("******* Error in address loader fetch....");
            t.printStackTrace();
            throw t;
        }
    }

    private Map<Long, ILogData> verifyAddressInWrites(Set<Long> addresses) {
        Map<Long, ILogData> addressesWrites = new HashMap<>();
        for (Long address : addresses) {
            ILogData data = this.writeCache.getIfPresent(address);
            if (data != null) {
                // Address is present in write cache, remove so it can be added to the read cache
                addressesWrites.put(address, data);
                // If we keep around for a moment...
                // writeCache.invalidate(address);
            }
        }
        addresses.removeAll(addressesWrites.keySet());
        return addressesWrites;
    }

    public synchronized void dispatch(Map<Long, ILogData> addressToData) {
        Set<AddressLoaderRequest> requests = new HashSet<>();

        // 1. Get all the requests that require the read addresses and update state of the read with its value.
        addressToData.entrySet().forEach(entry -> {
            List<AddressLoaderRequest> requestsPerAddress  = this.addressToRequests.get(entry.getKey());
            requestsPerAddress.forEach(r -> r.markAddressRead(entry.getKey(), entry.getValue()));
            requests.addAll(requestsPerAddress);
        });

        // 2. Verify if any request is fully fulfilled (i.e., all addresses are read)
        requests.forEach(r -> r.completeIfRequestFulfilled());
    }

    public void stop() {
        shutdown = true;
        this.addressLoaderFetchWorker.shutdownNow();
    }
}
