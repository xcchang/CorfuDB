package org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.StateTransferProcessor.SlidingWindow;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Data needed for the dynamic policy invocation.
 */
@AllArgsConstructor
@Getter
@ToString
@Builder(toBuilder = true)
public class DynamicPolicyData {
    /**
     * A current tail of a stream (transfer batch requests yet to be processed).
     */
    private final Stream<Optional<TransferBatchRequest>> tail;
    /**
     * A sliding window with the aggregated statistics, data and a recent layout.
     */
    private final SlidingWindow slidingWindow;

    /**
     * The expected size of a {@link #tail}.
     */
    private final long size;

    /**
     * An instance of a corfu runtime.
     */
    private final CorfuRuntime corfuRuntime;
}