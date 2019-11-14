package org.corfudb.infrastructure.log.statetransfer.policy.errorpolicy;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.policy.dynamicpolicy.DynamicPolicyData;
import org.corfudb.runtime.exceptions.WrongEpochException;

@Slf4j
public class WrongEpochExceptionPolicy implements ErrorPolicy<WrongEpochException> {
    @Override
    public DynamicPolicyData applyPolicy(ErrorPolicyData<WrongEpochException> data) {
        log.info("State transfer: Handling wrong epoch exception: {}.",
                data.getException().getMessage());
        DynamicPolicyData dynamicData = data.getData();
        // Invalidate layout
        dynamicData.getCorfuRuntime().invalidateLayout();

        return null;
    }
}
