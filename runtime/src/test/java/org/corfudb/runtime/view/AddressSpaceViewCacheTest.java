package org.corfudb.runtime.view;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.view.AddressSpaceView.AddressSpaceViewCache;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AddressSpaceViewCacheTest {
    private static Random RND = new Random();

    @Test
    public void test() throws InterruptedException {
        //timeouts
        //eviction
        //asMap

        ExecutorService cacheExecutor = Executors.newFixedThreadPool(6);

        CorfuRuntimeParameters params = CorfuRuntimeParameters.builder()
                .numCacheEntries(1)
                .build();

        CacheLoader<Long, String> cacheLoader = new CacheLoader<Long, String>() {
            @Override
            public String load(Long value) {
                Sleep.sleepUninterruptibly(Duration.ofMillis(RND.nextInt(10000)));
                return UUID.randomUUID().toString();
            }

            @Override
            public Map<Long, String> loadAll(Iterable<? extends Long> keys) {
                Sleep.sleepUninterruptibly(Duration.ofMillis(RND.nextInt(10000)));
                HashMap<Long, String> map = new HashMap<>();
                for (Long key : keys) {
                    map.put(key, UUID.randomUUID().toString());
                }
                return map;
            }
        };

        final AddressSpaceViewCache<Long, String> cache = new AddressSpaceViewCache<>(params, cacheLoader);
        ExecutorService executor = Executors.newFixedThreadPool(6);


        Thread t1 = new Thread(() -> {
            while (true) {
                cache.put(RND.nextLong(), UUID.randomUUID().toString());
                cache.put(RND.nextLong(), UUID.randomUUID().toString());
                cache.put(RND.nextLong(), UUID.randomUUID().toString());
                cache.put(RND.nextLong(), UUID.randomUUID().toString());
                cache.put(RND.nextLong(), UUID.randomUUID().toString());
            }
        });
        t1.setName("t1");

        Thread t2 = new Thread(() -> {
            long start = System.currentTimeMillis();
            while (true) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (Duration.ofMillis(System.currentTimeMillis() - start).getSeconds() > 3){
                    System.out.println("invalidate");
                    start = System.currentTimeMillis();
                }
                cache.getUnderlyingCache().invalidateAll();
                cache.getUnderlyingCache().invalidateAll();
                cache.getUnderlyingCache().invalidateAll();
            }
        });
        t2.setName("t2");

        t1.start();
        Thread.sleep(1000);
        System.out.println("Start t2!!!!!!!!!!!!!");
        t2.start();

        Thread.sleep(1_000_000);


        //yay(cache, executor);
    }

    private void yay(AddressSpaceViewCache<Long, String> cache, ExecutorService executor) throws InterruptedException {
        List<CompletableFuture<Void>> allFutures = new ArrayList<>();
        int counter = 0;
        for (int i = 1; i < 1_000_000; i++) {
            int ii = i;

            if (ii % 100 == 0) {
                System.out.println("!!!!Processed: " + ii);
            }

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int j = 1; j < 100; j++) {
                long longI = RND.nextInt(j) + 1;
                long longI2 = longI + RND.nextInt(j) + 1;
                futures.add(CompletableFuture.runAsync(() -> cache.get(longI), executor));
                futures.add(CompletableFuture.runAsync(() -> cache.put(longI, UUID.randomUUID().toString()), executor));
                futures.add(CompletableFuture.runAsync(() -> cache.get(longI), executor));
                futures.add(CompletableFuture.runAsync(() -> {
                    if (ii % (RND.nextInt(100) + 1) == 0) {
                        CacheStats stats = cache.getUnderlyingCache().stats();
                        Sleep.sleepUninterruptibly(Duration.ofMillis(RND.nextInt(10000)));
                        try {
                            Files.write(Paths.get("yay"), String.valueOf(stats.hitRate()).getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }, executor));
                futures.add(CompletableFuture.runAsync(() -> cache.put(longI, UUID.randomUUID().toString()), executor));
                futures.add(CompletableFuture.runAsync(() -> {
                    cache.get(longI);
                    cache.put(longI, UUID.randomUUID().toString());
                }, executor));
                futures.add(CompletableFuture.runAsync(() -> {
                    Sleep.sleepUninterruptibly(Duration.ofMillis(RND.nextInt(10000)));
                    cache.put(longI, UUID.randomUUID().toString());
                }, executor));
                futures.add(CompletableFuture.runAsync(() -> cache.getNextRead(longI2, () -> {
                    List<Long> xx = new ArrayList<>();
                    xx.add(longI);
                    for (int k = 1; k < 100; k++) {
                        Iterator<Long> iterator = cache.getUnderlyingCache().asMap().keySet().iterator();
                        if (iterator.hasNext()) {
                            Long v = iterator.next();
                            xx.add(v);
                        }
                    }

                    try {
                        return cache.getAll(xx);
                    } catch (ExecutionException | UncheckedExecutionException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof RuntimeException) {
                            throw (RuntimeException) cause;
                        } else {
                            throw new RuntimeException(cause);
                        }
                    }
                }), executor));

                futures.add(CompletableFuture.runAsync(() -> {
                    if (ii % 30 == 0) {
                        //cache.resetCaches();
                        cache.clean(entry -> longI / 2 > entry.getKey());
                    }

                    if (ii % 1000 == 0) {
                        cache.resetCaches();
                    }
                }, executor));
            }

            counter++;
            allFutures.addAll(futures);

            if (counter % 100 == 0) {
                CFUtils.allOf(allFutures).join();
                allFutures = new ArrayList<>();
            }
        }
    }

}