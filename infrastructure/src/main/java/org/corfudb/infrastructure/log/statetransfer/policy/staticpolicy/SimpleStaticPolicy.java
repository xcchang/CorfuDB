package org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy;

import com.google.common.collect.Lists;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * A static policy that creates an initial stream without considering a layout.
 * It batches up addresses and creates a stream from them.
 */
public class SimpleStaticPolicy implements StaticPolicy {
    @Override
    public InitialBatchStream applyPolicy(StaticPolicyData data) {
        List<Long> addresses = data.getAddresses();
        int defaultBatchSize = data.getDefaultBatchSize();

        AtomicLong sizeOfStream = new AtomicLong(0L);
        Stream<TransferBatchRequest> initStream = Lists.partition(addresses, defaultBatchSize)
                .stream()
                .map(part ->
                {
                    sizeOfStream.getAndAdd(part.size());
                    return TransferBatchRequest.builder().addresses(part).build();
                });
        return new InitialBatchStream(initStream, sizeOfStream.get());
    }
}
