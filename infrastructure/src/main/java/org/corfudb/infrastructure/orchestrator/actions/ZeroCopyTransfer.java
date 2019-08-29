package org.corfudb.infrastructure.orchestrator.actions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logunit.AddressMetaDataRangeMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ZeroCopyTransfer {

    private ZeroCopyTransfer(){

    }

    public static void transfer(Layout layout,
                                String currentEndpoint,
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

        log.info("Getting mappings from local server");

        Map<Long, AddressMetaDataRangeMsg.AddressMetaDataMsg> map =
                CFUtils.getUninterruptibly(runtime.getLayoutView()
                        .getRuntimeLayout()
                        .getLogUnitClient(currentEndpoint)
                        .requestAddressMetaData(allChunks));

        log.info("Setting mappings to remote server");

        CFUtils.getUninterruptibly(runtime.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(endpoint)
                .setRemoteMetaData(map));

        log.info("Initiating receive");
        CFUtils.getUninterruptibly(runtime.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(endpoint)
                .initReceive());

        Sleep.sleepUninterruptibly(Duration.ofMillis(1000));

        log.info("Initiating transfer");
        CFUtils.getUninterruptibly(runtime.getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(currentEndpoint)
                .initTransfer(endpoint, 9999, allChunks));

        Sleep.sleepUninterruptibly(Duration.ofMillis(2000));

        log.info("Wait while done");
        boolean stillTransferring = false;
        for(int i = 0; i < 3; i ++){
            Sleep.sleepUninterruptibly(Duration.ofMillis(3000));
            stillTransferring = CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout().getLogUnitClient(currentEndpoint).isStillTransferring());
            if(!stillTransferring){
                return;
            }
        }


        if(stillTransferring){
            log.error("Polls exeded");
            throw new RuntimeException("Polls exeded");
        }
        log.info("Done with transfer");
    }
}
