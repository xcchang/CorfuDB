package org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy;

import org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy.DynamicPolicyData;

@FunctionalInterface
public interface ErrorPolicy<E extends RuntimeException> {

    DynamicPolicyData applyPolicy(ErrorPolicyData<E> data);
}
