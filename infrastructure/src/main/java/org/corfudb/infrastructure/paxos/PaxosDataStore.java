package org.corfudb.infrastructure.paxos;

import org.corfudb.infrastructure.Phase2Data;
import org.corfudb.infrastructure.Rank;
import org.corfudb.runtime.view.Layout;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public interface PaxosDataStore {

    Optional<Rank> getPhase1Rank();

    void setPhase1Rank(Rank rank);

    Optional<Phase2Data> getPhase2Data();

    void setPhase2Data(Phase2Data phase2Data);

    Rank getPhase2Rank();

    Layout getProposedLayout();

    class InMemoryPaxosDataStore implements PaxosDataStore {
        private final Map<Long, Rank> phase1Ds = new HashMap<>();
        private final Map<Long, Phase2Data> phase2Ds = new HashMap<>();

        public long epoch;

        @Override
        public Optional<Rank> getPhase1Rank() {
            return Optional.ofNullable(phase1Ds.get(epoch));
        }

        @Override
        public void setPhase1Rank(Rank rank) {
            phase1Ds.put(epoch, rank);
        }

        @Override
        public Optional<Phase2Data> getPhase2Data() {
            return Optional.ofNullable(phase2Ds.get(epoch));
        }

        @Override
        public void setPhase2Data(Phase2Data phase2Data) {
            phase2Ds.put(epoch, phase2Data);
        }

        @Override
        public Rank getPhase2Rank() {
            return getPhase2Data().map(Phase2Data::getRank).orElse(null);
        }

        @Override
        public Layout getProposedLayout() {
            return getPhase2Data().map(Phase2Data::getLayout).orElse(null);
        }
    }
}
