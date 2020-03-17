package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuQueue;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Index;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
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
    static final int NUM_KEYS = 10;
    /**
     * In a write after write transaction, concurrent modifications
     * with the same read timestamp should abort.
     */
    @Override
    public void TXBegin() {
        WWTXBegin();
    }

    @Test
    public void abortTransactionTest() throws Exception {
        CorfuRuntime runtime = getDefaultRuntime();

        Map<String, String> map = runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
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


    @Test
    public void testRemaining() {
        CorfuRuntime runtime = getDefaultRuntime();
        runtime.setTransactionLogging(true);

        //String name = ObjectsView.TRANSACTION_STREAM_ID;
        String name = "Transaction_Stream";
        System.out.println("\ntransaction stream 0 " + ObjectsView.TRANSACTION_STREAM_ID);

        System.out.println("transaction stream 1 " + UUID.nameUUIDFromBytes(name.getBytes()));
        name = "span";
        System.out.println("span stream  " + UUID.nameUUIDFromBytes(name.getBytes()));
        Map<String, String> map = runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test0")
                .open();

        CorfuQueue<String>
                corfuQueue = new CorfuQueue<>(runtime, "test1", Serializers.JAVA,
                Index.Registry.empty());

        IStreamView txStream = runtime.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID);
        final String key = "key";
        final String value = "value";

        for(int i = 0; i < NUM_KEYS; i++) {
            map.put(key + i, value +i);
        }

        runtime.getObjectsView().TXBegin();
        map.put(key, value);
        runtime.getObjectsView().TXEnd();

            //generate some data do append to table
        List<ILogData> entries = txStream.remaining();
        System.out.println("\nnumber of entries " + entries.size() + " txPointer " + txStream.getCurrentGlobalPosition() + " tail " + runtime.getAddressSpaceView().getLogTail());


        for(int i = 0; i < NUM_KEYS; i++) {
            map.put(key + i, value +i);
        }

        entries = txStream.remaining();
        System.out.println("\nnumber of entries " + entries.size() + " txPointer " + txStream.getCurrentGlobalPosition() + " tail " + runtime.getAddressSpaceView().getLogTail());

        for (int i = 0; i < 10000; i++) {
            CorfuQueue.CorfuRecordId idC = corfuQueue.enqueue("val " + i);
        }

        entries = txStream.remaining();
        System.out.println("\nnumber of entries " + entries.size() + " txPointer " + txStream.getCurrentGlobalPosition() + " tail " + runtime.getAddressSpaceView().getLogTail());

        entries = txStream.remaining();
        System.out.println("\nnumber of entries " + entries.size() + " txPointer " + txStream.getCurrentGlobalPosition() + " tail " + runtime.getAddressSpaceView().getLogTail());
    }


    @Test
    public  void testIntentPath() {
        String intentPath = "/global-infra/domains/London_Paris/groups/London_paris_grp";
        String domainPath = "/global-infra/domains/London_Paris";
        Boolean result = intentPath.startsWith(domainPath);
        System.out.println(" result " + result);
    }
}
