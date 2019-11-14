package org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;

import java.util.stream.Stream;

/**
 * A transfer batch request stream along with it's size.
 */
@AllArgsConstructor
@Getter
public class InitialBatchStream {
    private final Stream<TransferBatchRequest> initialStream;
    private final long initialBatchStreamSize;
}
