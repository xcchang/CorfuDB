package org.corfudb.infrastructure.paxos;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.Phase2Data;
import org.corfudb.infrastructure.Rank;
import org.corfudb.protocols.wireprotocol.LayoutProposeRequest;
import org.corfudb.protocols.wireprotocol.LayoutProposeResponse;
import org.corfudb.runtime.view.Layout;

import java.util.Optional;
import java.util.UUID;

@Builder
@Slf4j
public class Paxos {

    @Getter
    @NonNull
    private final PaxosDataStore dataStore;

    public OperationResult prepare(Rank prepareRank) {
        //1. phase1 doesn't exists, committed layout doesnt exists
        //2. proposed rank is less than exists one
        //3. highest rank from phase2 if exists. Then why we even can set epoch1?

        Optional<Rank> phase1Rank = dataStore.getPhase1Rank();
        Optional<Phase2Data> phase2Data = dataStore.getPhase2Data();

        if (phase2Data.isPresent()) {
            return new OperationResult(
                    phase1Rank.get(),
                    phase2Data.map(Phase2Data::getLayout).get(),
                    OperationStatus.REJECTED
            );
        }

        // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
        if (phase1Rank.isPresent() && prepareRank.lessThanEqualTo(phase1Rank.get())) {
            log.debug("handleMessageLayoutPrepare: Rejected phase 1 prepare of rank={}, "
                    + "phase1Rank={}", prepareRank, phase1Rank);
            return new OperationResult(phase1Rank.get(), null, OperationStatus.REJECTED);
        }

        // Return the layout with the highest rank proposed before.
        dataStore.setPhase1Rank(prepareRank);

        Rank highestProposedRank = phase2Data
                .map(Phase2Data::getRank)
                .orElse(prepareRank.getEmpty());

        log.debug("handleMessageLayoutPrepare: New phase 1 rank={}", dataStore.getPhase1Rank());
        return new OperationResult(highestProposedRank, null, OperationStatus.OK);
    }

    public Optional<LayoutProposeResponse> propose(LayoutProposeRequest propose, UUID clientId) {
        Optional<Rank> maybePhase1Rank = dataStore.getPhase1Rank();
        Rank proposeRank = new Rank(propose.getRank(), clientId);

        if (!maybePhase1Rank.isPresent()) {
            log.debug("handleMessageLayoutPropose: Rejected phase 2 propose of rank={}, "
                    + "phase1Rank=none", proposeRank);
            return Optional.of(new LayoutProposeResponse(-1));
        }

        // This is a propose. If the rank in the proposal is less than or equal to the highest yet
        // observed prepare rank, reject.
        Rank phase1Rank = maybePhase1Rank.get();
        if (!proposeRank.equals(phase1Rank)) {
            log.debug("handleMessageLayoutPropose: Rejected phase 2 propose of rank={}, "
                    + "phase1Rank={}", proposeRank, phase1Rank);
            return Optional.of(new LayoutProposeResponse(phase1Rank.getRank()));
        }

        Layout proposeLayout = propose.getLayout();
        Optional<Phase2Data> phase2Data = dataStore.getPhase2Data();

        // In addition, if the rank in the propose message is equal to the current phase 2 rank
        // (already accepted message), reject.
        // This can happen in case of duplicate messages.
        if (phase2Data.isPresent() && proposeRank.equals(phase2Data.get().getRank())) {
            Rank phase2Rank = phase2Data.get().getRank();
            log.debug("handleMessageLayoutPropose: Rejected phase 2 propose of rank={}, "
                    + "phase2Rank={}", proposeRank, phase2Rank);
            return Optional.of(new LayoutProposeResponse(phase2Rank.getRank()));
        }

        dataStore.setPhase2Data(new Phase2Data(proposeRank, proposeLayout));
        return Optional.empty();
    }

    @Getter
    @ToString
    @AllArgsConstructor
    public static class OperationResult {
        private final Rank rank;
        private final Layout layout;
        private final OperationStatus status;
    }

    public enum OperationStatus {
        OK, REJECTED
    }

    public enum State {
        INIT, PHASE1, PHASE2, COMMITTED
    }
}
