package org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy;

/**
 * An interface that static policies should implement to define the initial properties of a stream.
 */
@FunctionalInterface
public interface StaticPolicy {
    /**
     * Given a static policy data, produce an initial stream.
     * @param data A static policy data.
     * @return An initial transfer batch request stream.
     */
    InitialBatchStream applyPolicy(StaticPolicyData data);
}
