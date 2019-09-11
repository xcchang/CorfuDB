package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;
import javax.swing.text.Segment;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

/**
 * This action attempts to restore redundancy for all servers across all segments
 * starting from the oldest segment. It then collapses the segments once the set of
 * servers in the 2 oldest subsequent segments are equal.
 * Created by Zeeshan on 2019-02-06.
 */
@Slf4j
public class RestoreRedundancyMergeSegments extends Action {

    public enum Mode{
        PUSH_STATE_TRANSFER,
        PULL_ZERO_COPY
    }

    private final Mode mode;

    private final String endpoint;

    /**
     * Returns set of nodes which are present in the next index but not in the specified segment. These
     * nodes have reduced redundancy and state needs to be transferred only to these before these segments can be
     * merged.
     *
     * @param layout             Current layout.
     * @param layoutSegmentIndex Segment to compare to get nodes with reduced redundancy.
     * @return Set of nodes with reduced redundancy.
     */

    private Set<String> getNodesWithReducedRedundancy(Layout layout, int layoutSegmentIndex) {
        // Get the set of servers present in the next segment but not in the this
        // segment.
        return Sets.difference(
                layout.getSegments().get(layoutSegmentIndex + 1).getAllLogServers(),
                layout.getSegments().get(layoutSegmentIndex).getAllLogServers());
    }

    // For this segment should return a list of stripe indices where endpoint is not present
    private List<Integer> getNonRedundantStripeIndices(Layout.LayoutSegment segment, String endpoint, Set<Integer> stripesForEndpoint){
        List<Integer> list = new ArrayList<>();


        for (int value : stripesForEndpoint) {
            if (!segment.getStripes().get(value).getLogServers().contains(endpoint)) {
                list.add(value);
            }
        }
        Collections.sort(list);
        return list;
    }

    private boolean nodeRestoredForAllStripes(Layout layout, Set<Integer> stripesForEndpoint, String endpoint) {

        // go over all segments of the layout
        // if segment is not present in map -> return false
        // if segment present in the map, find indices of all stripes where endpoint is present
        // the sets should be equal
        for (Layout.LayoutSegment segment : layout.getSegments()) {

            Set<Integer> stripesPresentForCurrentSegment = new HashSet<>();

            for (int i = 0; i < segment.getStripes().size(); i++) {
                if (segment.getStripes().get(i).getLogServers().contains(endpoint)) {
                    stripesPresentForCurrentSegment.add(i);
                }
            }

            if (!stripesPresentForCurrentSegment.equals(stripesForEndpoint)) {
                return false;
            }
        }
        return true;
    }

    // Indices of stripes that should be restored redundantly for this endpoint.
    private Set<Integer> getStripeIndices(Layout layout, String endpoint){
        int segmentSize = layout.getSegments().size();
        Layout.LayoutSegment lastSegment = layout.getSegments().get(segmentSize - 1);
        Set<Integer> set = new HashSet<>();
        for(int i = 0; i < lastSegment.getStripes().size(); i++){
            if(lastSegment.getStripes().get(i).getLogServers().contains(endpoint)){
                set.add(i);
            }
        }
        return set;
    }


    public RestoreRedundancyMergeSegments(String endpoint)
    {
        this.mode = Mode.PUSH_STATE_TRANSFER;
        this.endpoint = endpoint;
    }

    public RestoreRedundancyMergeSegments(Mode mode, String endpoint){
        this.mode = mode;
        this.endpoint = endpoint;
    }

    @Nonnull
    @Override
    public String getName() {
        return "RestoreRedundancyAndMergeSegments";
    }

    @Override
    public void impl(@Nonnull CorfuRuntime runtime) throws Exception {

        // Refresh layout.
        runtime.invalidateLayout();
        Layout layout = runtime.getLayoutView().getLayout();

        // Each segment is compared with the first segment. The data is restored in any new LogUnit nodes and then
        // merged to this segment. This is done for all the segments.
        final int layoutSegmentToMergeTo = 0;

        Set<Integer> redundantStripeIndices = getStripeIndices(layout, endpoint);

        // Catchup all servers across all segments.
        while (!nodeRestoredForAllStripes(layout, redundantStripeIndices, endpoint)) {

            // Currently the state is transferred for the complete segment.
            // TODO: Add stripe specific transfer granularity for optimization.
            // Transfer the replicated segment to the difference set calculated above.
            long transferStart = System.currentTimeMillis();
            switch(mode){
                case PULL_ZERO_COPY:
                    ZeroCopyTransfer.transferPull(layout, endpoint, runtime, layout.getFirstSegment());
                    break;
                default:
                    StateTransfer.transfer(layout, endpoint, runtime, layout.getFirstSegment());

            }
            long transferEnd = System.currentTimeMillis() - transferStart;
            log.info("State transfer took: {}", transferEnd);

            // Merge the 2 segments.
            runtime.getLayoutManagementView().mergeSegments(new Layout(layout));

            // Refresh layout
            runtime.invalidateLayout();
            layout = runtime.getLayoutView().getLayout();
        }
    }
}
