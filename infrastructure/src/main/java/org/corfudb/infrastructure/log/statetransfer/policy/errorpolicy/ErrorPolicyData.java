package org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy.DynamicPolicyData;

@AllArgsConstructor
@Getter
public class ErrorPolicyData<E extends RuntimeException> {

    private final DynamicPolicyData data;
    private final E exception;
}
