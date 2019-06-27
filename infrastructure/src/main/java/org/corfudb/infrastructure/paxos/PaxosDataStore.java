package org.corfudb.infrastructure.paxos;

import lombok.Builder;
import lombok.NonNull;
import org.corfudb.infrastructure.AcceptedData;
import org.corfudb.infrastructure.DataStore;
import org.corfudb.infrastructure.AcceptedData;
import org.corfudb.infrastructure.Rank;

import java.util.Optional;

/**
 * Provides access and saves paxos entities: PHASE1 and PHASE2 ranks with values.
 */
@Builder
public class PaxosDataStore {

    public static final String PREFIX_PHASE_1 = "PHASE_1";
    public static final String PREFIX_PHASE_2 = "PHASE_2";
    private static final String KEY_SUFFIX_PHASE_1 = "RANK";
    private static final String KEY_SUFFIX_PHASE_2 = "DATA";

    @NonNull
    private final DataStore dataStore;

    /**
     * Returns phase1 rank for current epoch
     * @param serverEpoch current server epoch
     * @return phase1 rank
     */
    public Optional<Rank> getPhase1Rank(long serverEpoch) {
        Rank rank = dataStore.get(Rank.class, PREFIX_PHASE_1, serverEpoch + KEY_SUFFIX_PHASE_1);
        return Optional.ofNullable(rank);
    }

    /**
     * Saves phase1 rank
     * @param rank phase1 rank
     * @param serverEpoch current server epoch
     */
    public void setPhase1Rank(Rank rank, long serverEpoch) {
        dataStore.put(Rank.class, PREFIX_PHASE_1, serverEpoch + KEY_SUFFIX_PHASE_1, rank);
    }

    /**
     * Returns phase2 data for current server epoch
     * @param serverEpoch server epoch
     * @return phase2 data
     */
    public Optional<AcceptedData> getPhase2Data(long serverEpoch) {
        AcceptedData acceptedData = dataStore.get(
                AcceptedData.class,
                PREFIX_PHASE_2,
                serverEpoch + KEY_SUFFIX_PHASE_2
        );

        return Optional.ofNullable(acceptedData);
    }

    /**
     * Saves phase2 data
     * @param acceptedData accepted data
     * @param serverEpoch current server epoch
     */
    public void setAcceptedData(AcceptedData acceptedData, long serverEpoch) {
        dataStore.put(
                AcceptedData.class,
                PREFIX_PHASE_2,
                serverEpoch + KEY_SUFFIX_PHASE_2,
                acceptedData
        );
    }
}
