package org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy.DynamicPolicyData;

@Slf4j
public class FailRestPolicy implements ErrorPolicy<RuntimeException> {
    @Override
    public DynamicPolicyData applyPolicy(ErrorPolicyData<RuntimeException> data) {
        log.info("State transfer: Got unrecoverable error: {}.",
                data.getException().getMessage());
        return null;
    }
}
