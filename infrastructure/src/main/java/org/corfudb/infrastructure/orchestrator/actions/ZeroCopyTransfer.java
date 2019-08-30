package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ZeroCopyTransfer {

    private ZeroCopyTransfer(){

    }

    private static Optional<String> getDonorForAddresses(CorfuRuntime runtime, List<Long> addresses){


        List<Set<String>> collect = addresses.stream().map(address -> {
            List<String> logServers = runtime.getLayoutView().getRuntimeLayout()
                    .getLayout()
                    .getStripe(address)
                    .getLogServers();
            return ImmutableSet.copyOf(logServers);
        }).collect(Collectors.toList());

        return collect
                .stream()
                .reduce(Sets::intersection)
                .map(set -> set.iterator().next());

    }

    public static void transferPush(Layout layout,
                                String endpoint,
                                CorfuRuntime runtime,
                                Layout.LayoutSegment segment) {

        int chunkSize = runtime.getParameters().getBulkReadSize();
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

        List<Long> allChunks = new ArrayList<>();
        log.info("Aggregating chunks");
        for (long chunkStart = segmentStart; chunkStart <= segmentEnd
                ; chunkStart += chunkSize) {

            long chunkEnd = Math.min(segmentEnd, chunkStart + chunkSize - 1);

            // Fetch all missing entries in this range [chunkStart - chunkEnd].
            List<Long> chunk = StateTransfer.getMissingEntriesChunk(layout, runtime, endpoint,
                    chunkStart, chunkEnd);

            // Read and write in chunks of chunkSize.
            allChunks.addAll(chunk);
        }

        Optional<String> maybeDonor = getDonorForAddresses(runtime, allChunks);

        if(!maybeDonor.isPresent()){
            log.error("No donor found, return");
            return;
        }
        else{
            log.info("Donor is selected: {}", maybeDonor.get());
        }

        String donor = maybeDonor.get();

        log.info("Getting mappings from local server");

        AddressMetaDataRangeMsg rangeMsg =
                CFUtils.getUninterruptibly(runtime.getLayoutView()
                        .getRuntimeLayout()
                        .getLogUnitClient(donor)
                        .requestAddressMetaData(allChunks));

        log.info("Setting mappings to remote server");

        CFUtils.getUninterruptibly(runtime.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(endpoint)
                .setRemoteMetaData(rangeMsg.getAddressMetaDataMap()));

        log.info("Initiating receive");
        CFUtils.getUninterruptibly(runtime.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(endpoint)
                .initReceive());

        log.info("Initiating transfer");
        CFUtils.getUninterruptibly(runtime.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(donor)
                .initTransfer("localhost", 9999, allChunks));
//
        log.info("Wait while done");
        boolean stillTransferring = false;
        for(int i = 0; i < 10; i ++){
            Sleep.sleepUninterruptibly(Duration.ofMillis(1000));
            TransferQueryResponse stillTransferringResponse = CFUtils
                    .getUninterruptibly(runtime.getLayoutView()
                            .getRuntimeLayout()
                            .getLogUnitClient(donor)
                            .isStillTransferring());
            stillTransferring = stillTransferringResponse.isActive();
            if(!stillTransferring){
                log.info("Done with transfer");
                return;
            }
        }


        if(stillTransferring){
            log.error("Polls exeded");
            throw new RuntimeException("Polls exeded");
        }

    }
}
