package org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy;

/**
 * A dynamic policy that slides a window. Updates the remaining stream tail and a window.
 */
public class SlideWindowPolicy implements DynamicPolicy {
    @Override
    public DynamicPolicyData applyPolicy(DynamicPolicyData data) {
        return null;
    }
}
