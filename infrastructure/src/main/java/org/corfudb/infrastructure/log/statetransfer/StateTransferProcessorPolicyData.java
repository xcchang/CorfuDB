package org.corfudb.infrastructure.log.statetransfer;

import lombok.AllArgsConstructor;
import org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy.DynamicPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy.StaticPolicy;

@AllArgsConstructor
public class StateTransferProcessorPolicyData {
    /**
     * A policy that dictates an initial distribution of batches within a lazy stream, e.g.
     * what addresses will go in every batch,
     * what servers can be used to perform a direct transfer for every batch,
     * what initial latency to use before performing a batch transfer.
     */
    private final StaticPolicy initialDistributionPolicy;

    /**
     * A policy that dictates a dynamic (after invocation) distribution of batches
     * within a tail of a lazy stream, e.g.
     * what addresses will go in every batch,
     * what servers can be used to perform a direct transfer for every batch,
     * what dynamic latency to use before performing a transfer of every batch.
     */
    private final DynamicPolicy dynamicDistributionPolicy;

    /**
     * A policy that dictates how the rest of the batches within a tail of a lazy stream
     * will be handled after the batch processor failed to transfer them, e.g.
     * what batches to remove or reschedule to transfer from the set of active servers.
     */
    private final DynamicPolicy batchProcessorFailureHandlingPolicy;
}
