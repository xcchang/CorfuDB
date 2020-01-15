

package org.corfudb.universe.scenario;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.reflect.TypeToken;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.corfudb.runtime.collections.ContextAwareMap;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxBuilder;
import samples.protobuf.*;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import samples.protobuf.PersonProfile.Name;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.function.Supplier;

public class CorfuTableBenchmark extends GenericIntegrationTest {

    private static final long WRITE_SIZE = 1_000;
    final int DATA_SIZE_CHAR = 100;
    final int TX_SIZE = 1000;
    @FunctionalInterface
    interface TriFunction<T, U, V, R> {
        R apply(T a, U b, V c);
    }

    private Options getPersistentMapOptions() {
        final int maxSizeAmplificationPercent = 50;
        final Options options = new Options();

        options.setCreateIfMissing(true);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);

        // Set a threshold at which full compaction will be triggered.
        // This is important as it purges tombstoned entries.
        final CompactionOptionsUniversal compactionOptions = new CompactionOptionsUniversal();
        compactionOptions.setMaxSizeAmplificationPercent(maxSizeAmplificationPercent);
        options.setCompactionOptionsUniversal(compactionOptions);
        return options;
    }


    @Test
    public void diskBackedTable() {

        final int metricsPort = 1234;
        workflow(wf -> {
            wf.setupDocker(fixture -> fixture.getSupportServer().metricPorts(Collections.singleton(metricsPort)));

            wf.deploy();
            CorfuCluster corfuCluster = (CorfuCluster) wf.getUniverse().groups().values().stream()
                    .filter(t -> t instanceof CorfuCluster).findFirst().get();
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient(metricsPort);

            final Path persistedCacheLocation = Paths.get("/tmp/disk");
            final String tableName = "diskBackedMap";

            Options options = new Options();
            options.setCreateIfMissing(true);
            options.setCompressionType(CompressionType.NO_COMPRESSION);

            Supplier<ContextAwareMap<String, String>> supplier = () -> new PersistedStreamingMap<>(
                    persistedCacheLocation,
                    getPersistentMapOptions(),
                    Serializers.PRIMITIVE,
                    corfuClient.getRuntime());

            CorfuTable<String, String> table1 = corfuClient.getRuntime().getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setStreamName(tableName)
                    .setArguments(supplier, ICorfuVersionPolicy.MONOTONIC)
                    .open();

            CorfuTable<String, String>  table2 = corfuClient.getRuntime().getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setStreamName("memMap")
                    .open();

            MetricRegistry metricRegistry = corfuClient.getRuntime().getDefaultMetrics();


            TriFunction<Integer, Integer, CorfuTable<String, String>, Void> unitOfWork =
                    (offset, delay, table) -> {
                        Timer writeTimer = metricRegistry.timer("write-duration-" + delay);
                        Timer putTimer = metricRegistry.timer("write-duration-put-" + delay);

                        for (long i = offset * WRITE_SIZE; i < WRITE_SIZE * offset + WRITE_SIZE; i++) {
                            try {
                                Thread.sleep((long)(Math.random() * delay));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            try (Timer.Context context = writeTimer.time()) {
                                StopWatch watch = new StopWatch();
                                watch.start();
                                corfuClient.getRuntime().getObjectsView().TXBegin();
                                try (Timer.Context context2 = putTimer.time()) {
                                    for (int j = 0; j < TX_SIZE; j++) {
                                        table.put( i + "-" + j,
                                                RandomStringUtils.random(DATA_SIZE_CHAR, true, true));
                                    }
                                }
                                corfuClient.getRuntime().getObjectsView().TXEnd();
                                watch.stop();
                                System.err.println(i + "; "  + watch.getTime());
                            } catch (TransactionAbortedException e) {
                                e.printStackTrace();
                            }
                        }
                        return null;
                    };


            Thread t0 = new Thread(() -> unitOfWork.apply(0, 0, table1));
            t0.start();

            Thread t1 = new Thread(() -> unitOfWork.apply(1, 100, table1));
            t1.start();

            Thread t2 = new Thread(() -> unitOfWork.apply(2, 1000, table1));
            t2.start();

            try {
                t0.join();
                t1.join();
                t2.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            corfuClient.shutdown();
        });
    }


    @Test
    public void scanAndFilter() {

        final int metricsPort = 1234;
        workflow(wf -> {
            wf.setupDocker(fixture -> fixture.getSupportServer().metricPorts(Collections.singleton(metricsPort)));

            wf.deploy();
            CorfuCluster corfuCluster = (CorfuCluster) wf.getUniverse().groups().values().stream()
                    .filter(t -> t instanceof CorfuCluster).findFirst().get();
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient(metricsPort);

            final Path persistedCacheLocation = Paths.get("/tmp/disk");
            final String tableName = "diskBackedMap";

            CorfuTable<String, String> memTable = corfuClient.getRuntime().getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setStreamName(tableName)
                    .open();

            MetricRegistry metricRegistry = corfuClient.getRuntime().getDefaultMetrics();

            TriFunction<Integer, Integer, CorfuTable<String, String>, Void> unitOfWork =
                    (offset, delay, table) -> {
                        Timer writeTimer = metricRegistry.timer("write-duration-" + delay);
                        Timer putTimer = metricRegistry.timer("write-duration-put-" + delay);

                        for (long i = offset * WRITE_SIZE; i < WRITE_SIZE * offset + WRITE_SIZE; i++) {
                            try {
                                Thread.sleep((long)(Math.random() * delay));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            try (Timer.Context context = writeTimer.time()) {
                                StopWatch watch = new StopWatch();
                                watch.start();
                                corfuClient.getRuntime().getObjectsView().TXBegin();
                                try (Timer.Context context2 = putTimer.time()) {
                                    for (int j = 0; j < TX_SIZE; j++) {
                                        table.put( i + "-" + j,
                                                RandomStringUtils.random(DATA_SIZE_CHAR, true, true));
                                    }
                                }
                                corfuClient.getRuntime().getObjectsView().TXEnd();
                                watch.stop();
                                System.err.println(i + "; "  + watch.getTime());
                            } catch (TransactionAbortedException e) {
                                e.printStackTrace();
                            }
                        }
                        return null;
                    };

            Thread t0 = new Thread(() -> unitOfWork.apply(0, 0, memTable));
            t0.start();
            try {
                t0.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }

            Double acc = 0.0;
            int iters = 50;
            for (int i = 0; i < iters; i++) {
                StopWatch watch = new StopWatch();
                System.err.println("Starting scan...");
                watch.start();
                memTable.scanAndFilter(entry -> entry.charAt(50) == 'a');
                watch.stop();
                System.err.println("Scanning done in: "  + watch.getTime());
            }

            System.err.println("Average: "  + acc/iters);

            try {
                t0.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }



            corfuClient.shutdown();
        });
    }

    @Test
    public void ufoDisk() {

        final int metricsPort = 1234;
        workflow(wf -> {
            wf.setupDocker(fixture -> fixture.getSupportServer().metricPorts(Collections.singleton(metricsPort)));

            wf.deploy();
            CorfuCluster corfuCluster = (CorfuCluster) wf.getUniverse().groups().values().stream()
                    .filter(t -> t instanceof CorfuCluster).findFirst().get();
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient(metricsPort);

            final Path persistedCacheLocation = Paths.get("/tmp/disk");
            final String tableName = "diskBackedMap";
            final String namespace = "namespace";
            Options options = new Options();
            options.setCreateIfMissing(true);
            options.setCompressionType(CompressionType.NO_COMPRESSION);

            CorfuStore store = new CorfuStore(corfuClient.getRuntime());

            MetricRegistry metricRegistry = corfuClient.getRuntime().getDefaultMetrics();

            try {
                store.openTable(
                        namespace,
                        tableName,
                        Name.class,
                        Name.class,
                        Name.class,
                        TableOptions.builder().persistentDataPath(persistedCacheLocation).build());
            } catch (Exception e) {
                e.printStackTrace();
            }

            TriFunction<Integer, Integer, TxBuilder, Void> unitOfWork =
                    (offset, delay, tx) -> {
                        Timer writeTimer = metricRegistry.timer("write-duration-" + delay);
                        Timer putTimer = metricRegistry.timer("write-duration-put-" + delay);

                        for (long i = offset * WRITE_SIZE; i < WRITE_SIZE * offset + WRITE_SIZE; i++) {
                            try {
                                Thread.sleep((long)(Math.random() * delay));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            try (Timer.Context context = writeTimer.time()) {
                                StopWatch watch = new StopWatch();
                                watch.start();

                                for (int j = 0; j < TX_SIZE; j++) {
                                    Name name = Name.newBuilder()
                                            .setFirstName(RandomStringUtils.random(DATA_SIZE_CHAR))
                                            .build();
                                    tx.create(tableName, name, name, Name.newBuilder().build());
                                }
                                tx.commit();
                                watch.stop();
                                System.err.println(i + "; "  + watch.getTime());
                            } catch (TransactionAbortedException e) {
                                e.printStackTrace();
                            }
                        }
                        return null;
                    };


            Thread t0 = new Thread(() -> unitOfWork.apply(0, 0, store.tx(namespace)));
            t0.start();

            Thread t1 = new Thread(() -> unitOfWork.apply(1, 100, store.tx(namespace)));
            t1.start();

            Thread t2 = new Thread(() -> unitOfWork.apply(2, 1000, store.tx(namespace)));
            t2.start();


            try {
                t0.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            corfuClient.shutdown();
        });
    }
}

