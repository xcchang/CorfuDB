package org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy.DynamicPolicy;
import org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy.DynamicPolicyData;

@Slf4j
public class UpdateLayoutPolicy implements DynamicPolicy {

    @Override
    public DynamicPolicyData applyPolicy(DynamicPolicyData data) {
        return null;
    }
}
