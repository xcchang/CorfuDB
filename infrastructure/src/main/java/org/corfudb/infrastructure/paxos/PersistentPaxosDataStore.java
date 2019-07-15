package org.corfudb.infrastructure.paxos;

import lombok.Builder;
import lombok.NonNull;
import org.corfudb.infrastructure.DataStore;
import org.corfudb.infrastructure.Phase2Data;
import org.corfudb.infrastructure.Rank;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.view.Layout;

import java.util.Optional;

@Builder
public class PersistentPaxosDataStore implements PaxosDataStore {

    public static final String PREFIX_PHASE_1 = "PHASE_1";
    public static final String KEY_SUFFIX_PHASE_1 = "RANK";
    public static final String PREFIX_PHASE_2 = "PHASE_2";
    public static final String KEY_SUFFIX_PHASE_2 = "DATA";

    @NonNull
    private final ServerContext serverContext;

    @NonNull
    private final DataStore dataStore;

    @Override
    public Optional<Rank> getPhase1Rank() {
        Rank rank = dataStore.get(
                Rank.class,
                PREFIX_PHASE_1,
                serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_1
        );

        return Optional.ofNullable(rank);
    }

    @Override
    public void setPhase1Rank(Rank rank) {
        dataStore.put(
                Rank.class,
                PREFIX_PHASE_1,
                serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_1,
                rank
        );
    }

    @Override
    public Optional<Phase2Data> getPhase2Data() {
        Phase2Data phase2 = dataStore.get(
                Phase2Data.class,
                PREFIX_PHASE_2,
                serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_2
        );

        return Optional.ofNullable(phase2);
    }

    @Override
    public void setPhase2Data(Phase2Data phase2Data) {
        dataStore.put(Phase2Data.class, PREFIX_PHASE_2, serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_2, phase2Data);
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
