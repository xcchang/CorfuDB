package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.LongRange;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logunit.AddressMetaDataRangeMsg;
import org.corfudb.protocols.wireprotocol.logunit.TransferQueryResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@Slf4j
public class ZeroCopyTransfer {

    private ZeroCopyTransfer(){

    }

    private static HashMap<String, List<Long>> getDonorsForAddresses(CorfuRuntime runtime, List<Long> addresses){

        // Log unit server to the addresses it's responsible for.
        HashMap<String, List<Long>> donorToAddresses = new HashMap<>();

        for(long address: addresses){
            List<String> logServers = runtime.getLayoutView().getRuntimeLayout()
                    .getLayout()
                    .getStripe(address)
                    .getLogServers();
            String logServer = logServers.get(logServers.size() - 1);
            List<Long> addressList = donorToAddresses.computeIfAbsent(logServer, s -> new ArrayList<>());
            addressList.add(address);
        }

        return donorToAddresses;

    }

    public static void transferPull(Layout layout, String endpoint, CorfuRuntime runtime, Layout.LayoutSegment segment) {
        long trimMark = StateTransfer.setTrimOnNewLogUnit(layout, runtime, endpoint);
        if (trimMark > segment.getEnd()) {
            log.info("ZeroCopyStateTransfer: Nothing to transfer, trimMark {}"
                            + "greater than end of segment {}",
                    trimMark, segment.getEnd());
            return;
        }

        final long segmentStart = Math.max(trimMark, segment.getStart());
        final long segmentEnd = segment.getEnd() - 1;
        log.info("ZeroCopyStateTransfer: Total address range to transfer: [{}-{}] to node {}",
                segmentStart, segmentEnd, endpoint);

        List<Long> allChunks = LongStream.range(segmentStart, segmentEnd + 1).boxed().collect(Collectors.toList());
        log.info("Aggregating chunks");

        Map<String, List<Long>> map = getDonorsForAddresses(runtime, allChunks);

        for(Map.Entry<String, List<Long>> entry: map.entrySet()){
            String donor = entry.getKey();
            log.info("Processing transfer from {} to {}", donor, endpoint);
            List<Long> addressesToTransfer = entry.getValue();

            // retry logic?
            Map<Long, AddressMetaDataRangeMsg.AddressMetaDataMsg> addressMetaDataMap = CFUtils.getUninterruptibly(runtime
                    .getLayoutView()
                    .getRuntimeLayout()
                    .getLogUnitClient(donor)
                    .requestAddressMetaData(addressesToTransfer)).getAddressMetaDataMap();

            log.info("Got metadata from donor {}", donor);

            // initiate transfer
            runtime.getLayoutView()
                    .getRuntimeLayout()
                    .getLogUnitClient(endpoint)
                    .transferRange(addressMetaDataMap, endpoint, donor);

            log.info("Polling for transfer status");
            checkTransferStatus(runtime, endpoint);
        }

    }

    private static void checkTransferStatus(CorfuRuntime runtime, String currentEndpoint){
        boolean stillTransferring = false;
        for(int i = 0; i < 100; i ++){
            Sleep.sleepUninterruptibly(Duration.ofMillis(1000));
            TransferQueryResponse stillTransferringResponse = CFUtils
                    .getUninterruptibly(runtime.getLayoutView()
                            .getRuntimeLayout()
                            .getLogUnitClient(currentEndpoint)
                            .isStillTransferring());
            stillTransferring = stillTransferringResponse.isActive();
            if(!stillTransferring){
                log.info("Done with transfer");
                return;
            }
        }
        if(stillTransferring){
            log.error("Polls exceeded");
            throw new RuntimeException("Polls exceeded");
        }
    }
}
