package org.corfudb.infrastructure.log.statetransfer.policy.staticpolicy;

import com.google.common.collect.Lists;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A static policy that considers the layout, all the addresses to transfer, and a batch size.
 * For every segment of the layout, batches up the addresses,
 * and then distributes the load in the round robin fashion
 * among the log servers of every segment.
 */
public class RoundRobinPolicy implements StaticPolicy {
    @Override
    public InitialBatchStream applyPolicy(StaticPolicyData data) {
        Layout initialLayout = data.getInitialLayout();
        List<Long> addresses = data.getAddresses();
        int defaultBatchSize = data.getDefaultBatchSize();
        // Create a map from layout segment to list of addresses.
        HashMap<LayoutSegment, List<Long>> segmentToAddresses = new HashMap<>();

        for (Long address : addresses) {
            LayoutSegment currentSegment = initialLayout.getSegment(address);
            List<Long> list =
                    segmentToAddresses.computeIfAbsent(currentSegment, segment -> new ArrayList<>());
            list.add(address);
        }

        AtomicLong sizeOfStream = new AtomicLong(0L);

        // For every segment and corresponding list of addresses:
        Stream<TransferBatchRequest> initStream =
                segmentToAddresses.entrySet().stream().flatMap(entry -> {

                    // Partition the addresses.
                    List<List<Long>> partitionsPerLayoutSegment =
                            Lists.partition(entry.getValue(), defaultBatchSize);

                    // Update the total size of a final stream.
                    sizeOfStream.getAndAdd(partitionsPerLayoutSegment.size());
                    // Get all the log servers for this segment.
                    List<String> logServers = entry.getKey().getFirstStripe().getLogServers();
                    // Go over all the partitions for this segment and assign them a destination
                    // log server in the round robin fashion.
                    return IntStream.range(0, partitionsPerLayoutSegment.size())
                            .boxed()
                            .map(index -> {
                                List<Long> batch = partitionsPerLayoutSegment.get(index);
                                String destination = logServers.get(index % logServers.size());
                                return TransferBatchRequest
                                        .builder()
                                        .addresses(batch)
                                        .destination(Optional.of(destination))
                                        .build();
                            });
                });

        return new InitialBatchStream(initStream, sizeOfStream.get());
    }
}
