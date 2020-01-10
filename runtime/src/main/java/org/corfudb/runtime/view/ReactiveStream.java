package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.primitives.Longs;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.CFUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ReactiveStream extends AbstractView { ;
    final long initialDelayMils = 0;
    final long delayMills = 100;
    final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    public final Map<Subscriber, BlockingQueue<Long>> addressQueues = new HashMap<>();
    ImmutableSetMultimap<Subscriber, UUID> subscriberStreamMap =
            ImmutableSetMultimap.<Subscriber, UUID>builder().build();

    volatile long currentTail;

    public interface Subscriber {
        void onNext(Map<UUID, Object> updates);

        void onError(Throwable error);

        void onComplete();
    }

    public ReactiveStream(CorfuRuntime runtime) {
        super(runtime);
        this.currentTail = runtime.getSequencerView().query().getSequence();
        executor.scheduleWithFixedDelay(this::getAddressSpace, initialDelayMils,
                delayMills, TimeUnit.MILLISECONDS);
    }

    public void subscribe(List<UUID> streams, Subscriber subscriber) {
        subscriberStreamMap = ImmutableSetMultimap.<Subscriber, UUID>builder()
                .putAll(subscriberStreamMap)
                .putAll(subscriber, streams).build();
        addressQueues.put(subscriber, new LinkedBlockingDeque<>());
    }

    public void getAddressSpace() {
        final long newTail = runtime.getSequencerView().query().getSequence();
        final List<StreamAddressRange> streamAddressRanges = subscriberStreamMap
                .values().stream()
                .map(streamId -> new StreamAddressRange(streamId, newTail, currentTail))
                .collect(Collectors.toList());

        final StreamsAddressResponse streamsAddressResponse =
                layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .getStreamsAddressSpace(streamAddressRanges)));
        final Map<UUID, StreamAddressSpace> nonEmptyUpdates = streamsAddressResponse
                .getAddressMap().entrySet().stream()
                .filter(entry -> !entry.getValue().getAddressMap().isEmpty())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));


        for (Subscriber subscriber: subscriberStreamMap.keySet()) {
            final Set<UUID> subscriberStreams = subscriberStreamMap.get(subscriber);
            final Map<UUID, StreamAddressSpace> filteredUpdates = nonEmptyUpdates
                    .entrySet().stream()
                    .filter(update-> subscriberStreams.contains(update.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
            if (filteredUpdates.isEmpty()) {
                continue;
            }
            final Roaring64NavigableMap addressAccumulator = new Roaring64NavigableMap();
            filteredUpdates.values().stream()
                    .map(StreamAddressSpace::getAddressMap)
                    .forEach(addressAccumulator::or);
            System.out.println("Adding " +
                    Arrays.toString(addressAccumulator.toArray()) + " to " + subscriber);
            addressQueues.get(subscriber).addAll(Longs.asList(addressAccumulator.toArray()));
        }

        currentTail = newTail;
    }

}
