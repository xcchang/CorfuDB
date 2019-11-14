package org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.runtime.view.Layout;

import java.util.List;

/**
 * A piece of data needed for the static policy invocation.
 */
@AllArgsConstructor
@Getter
public class StaticPolicyData {
    /**
     * A layout before a transfer.
     */
    private final Layout initialLayout;
    /**
     * Total addresses that has to be transferred.
     */
    private final List<Long> addresses;

    /**
     * A default size of one transfer batch.
     */
    private final int defaultBatchSize;
}
