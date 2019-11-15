package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferProcessor.SlidingWindow;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.InitialBatchStream;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.SimpleStaticPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.StaticPolicyData;
import org.corfudb.runtime.view.Layout;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A class responsible for managing a state transfer on the current node.
 * It executes the state transfer for each non-transferred segment synchronously.
 */
@Slf4j
@Builder
public class StateTransferManager {

    /**
     * A data class that represents a non-empty and bounded segment to be transferred.
     */
    @EqualsAndHashCode
    @Getter
    @ToString
    @Builder
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TransferSegment implements Comparable<TransferSegment> {
        /**
         * Start address of a segment range to transfer, inclusive and non-negative.
         */
        private final long startAddress;
        /**
         * End address of a segment range to transfer, inclusive and non-negative.
         */
        private final long endAddress;

        @Override
        public int compareTo(TransferSegment other) {
            return Long.compare(this.getStartAddress(), other.getStartAddress());
        }

        /**
         * Compute the total number of transferred addresses.
         * {@link #endAddress} and {@link #startAddress} can only be non-negative longs such that
         * {@link #endAddress} >= {@link #startAddress}.
         *
         * @return Sum of the total addresses transferred.
         */
        public long computeTotalTransferred() {
            return endAddress - startAddress + 1L;
        }

        public static class TransferSegmentBuilder {

            public void verify() {
                if (startAddress < 0L || endAddress < 0L) {
                    throw new IllegalStateException(
                            String.format("Start: %s or end: %s " +
                                    "can not be negative.", startAddress, endAddress));
                }
                if (startAddress > endAddress) {
                    throw new IllegalStateException(
                            String.format("Start: %s can not be " +
                                    "greater than end: %s.", startAddress, endAddress));
                }
            }

            public TransferSegment build() {
                verify();
                return new TransferSegment(startAddress, endAddress);
            }
        }
    }

    /**
     * A stream log of the current node.
     */
    @Getter
    @NonNull
    private final StreamLog streamLog;

    /**
     * A size of one batch of transfer.
     */
    @Getter
    @NonNull
    private final int batchSize;

    /**
     * A batch processor that transfers addresses one batch at a time.
     */
    @Getter
    @NonNull
    private final StateTransferProcessor stateTransferProcessor;

    /**
     * Given a range, return the addresses that are currently not present in the stream log.
     *
     * @param rangeStart Start address.
     * @param rangeEnd   End address.
     * @return A list of addresses, currently not present in the stream log.
     */
    ImmutableList<Long> getUnknownAddressesInRange(long rangeStart, long rangeEnd) {
        Set<Long> knownAddresses = streamLog
                .getKnownAddressesInRange(rangeStart, rangeEnd);

        return LongStream.range(rangeStart, rangeEnd + 1L)
                .filter(address -> !knownAddresses.contains(address))
                .boxed()
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * From the initial list of transfer segments create data needed to perform a transfer.
     *
     * @param transferSegments A list of the segments currently present in the system.
     * @param currentLayout A current layout used to calculate transferSegments. s
     * @return Static policy data used to create an initial stream of records.
     */
    StaticPolicyData createTransferData(
            List<TransferSegment> transferSegments, Layout currentLayout) {

        Stream<Long> init = Stream.empty();
        List<Long> allAddresses = transferSegments.stream().map(segment -> {

            List<Long> unknownAddressesInRange =
                    getUnknownAddressesInRange(segment.getStartAddress(),
                            segment.getEndAddress());

            if (unknownAddressesInRange.isEmpty()) {
                log.debug("All addresses are present in a range.");
            }
            return unknownAddressesInRange;
        }).filter(addressList -> !addressList.isEmpty()).reduce(init,
                (stream, list) -> Stream.concat(stream, list.stream()),
                (firstStream, secondStream) -> secondStream)
                .collect(ImmutableList.toImmutableList());
        return new StaticPolicyData(currentLayout, allAddresses, batchSize);
    }

    CompletableFuture<Long> stateTransfer(List<TransferSegment> transferSegments, Layout currentLayout){
        StaticPolicyData staticPolicyData = createTransferData(transferSegments, currentLayout);
        stateTransferProcessor.stateTransfer(staticPolicyData)

    }



}
