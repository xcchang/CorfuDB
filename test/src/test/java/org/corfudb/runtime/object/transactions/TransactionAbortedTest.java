package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import io.opentracing.Scope;
import io.opentracing.Span;
import org.apache.commons.lang.RandomStringUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.junit.Test;

/**
 * Ensure that the aborted transaction reporting adheres to the contract.
 *
 * This test does the following:
 * 1) Creates two threads.
 * 2) Synchronizes them using a count-down latch to ensure concurrent execution.
 * 3) They both write to the same stream and the key.
 * 4) One of the threads is going to get aborted (TransactionAbortedException).
 * 5) Ensure that the exception correctly reports the offending TX ID, the stream ID and the key.
 */
public class TransactionAbortedTest extends AbstractTransactionContextTest {

    /**
     * In a write after write transaction, concurrent modifications
     * with the same read timestamp should abort.
     */
    @Override
    public void TXBegin() {
        WWTXBegin();
    }

    /*
    @Test
    public void trace() {
        for (int i = 0; i < 500; i++) {
            Map<String, String> map = getRuntime().getObjectsView()
                    .build()
                    .setType(CorfuTable.class)
                    .setStreamName(this.getClass().getSimpleName() + "-" + i)
                    .open();
            map.put("a", "a");
        }

        Map<String, String> map = getRuntime().getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(this.getClass().getSimpleName())
                .open();

        Map<String, String> map2 = getRuntime().getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(this.getClass().getSimpleName() + "x")
                .open();

        for (int sample = 0; sample < 3; sample++) {
            getRuntime().getObjectsView().TXBegin();
            try (final Scope scope = getRuntime().getParameters().getTracer()
                    .buildSpan("get").startActive(true)) {
                map2.get("q");
            }
            for (int i = 0; i < 5; i++) {
                try (final Scope scope = getRuntime().getParameters().getTracer()
                        .buildSpan("put").startActive(true)) {
                    map.put(RandomStringUtils.randomAlphanumeric(17).toUpperCase(),
                            RandomStringUtils.randomAlphanumeric(17).toUpperCase());
                }
            }

            for (int i = 0; i < 5; i++) {
                try (final Scope scope = getRuntime().getParameters().getTracer()
                        .buildSpan("put").startActive(true)) {
                    map2.put(RandomStringUtils.randomAlphanumeric(17).toUpperCase(),
                            RandomStringUtils.randomAlphanumeric(17).toUpperCase());
                }
            }
            try (final Scope scope = getRuntime().getParameters().getTracer()
                    .buildSpan("get").startActive(true)) {
                map.get("q");
            }

            getRuntime().getObjectsView().TXEnd();
        }

    }
*/
    @Test
    public void abortTransactionTest() throws Exception {
        CorfuRuntime runtime = getDefaultRuntime();

        Map<String, String> map = runtime.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(this.getClass().getSimpleName())
                .open();
        final String key = "key";
        final String value = "value";

        t1(this::TXBegin);
        t2(this::TXBegin);

        AtomicLong offendingAddress = new AtomicLong(-1);
        t1(() -> {
            map.put(key, value);
            offendingAddress.set(runtime.getObjectsView().TXEnd());
        }).assertDoesNotThrow(TransactionAbortedException.class);

        t2(() -> {
            try {
                map.put(key, value);
                runtime.getObjectsView().TXEnd();
                return false;
            } catch (TransactionAbortedException tae) {
                // Ensure that the correct stream ID is reported.
                assertThat(tae.getConflictStream()
                        .equals(CorfuRuntime.getStreamID(this.getClass().getSimpleName())));

                // Ensure that the correct offending address is reported.
                assertThat(tae.getOffendingAddress().equals(offendingAddress.get()));

                // Ensure that the correct key is reported.
                final ICorfuSMRProxyInternal proxyInternal =
                        tae.getContext().getWriteSetInfo().getConflicts().keySet().stream().findFirst().get();
                final byte[] keyHash = ConflictSetInfo.generateHashFromObject(proxyInternal, key);
                assertThat(Arrays.equals(keyHash, tae.getConflictKey())).isTrue();
                return true;
            }
        }).assertResult().isEqualTo(true);
    }
}
