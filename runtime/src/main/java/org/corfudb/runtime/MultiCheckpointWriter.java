package org.corfudb.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.CheckpointException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;

/**
 * Checkpoint multiple SMRMaps serially as a prerequisite for a later log trim.
 */
@Slf4j
public class MultiCheckpointWriter<T extends Map> {
    @Getter
    private List<ICorfuSMR<T>> maps = new ArrayList<>();

    /** Add a map to the list of maps to be checkpointed by this class. */
    @SuppressWarnings("unchecked")
    public void addMap(T map) {
        maps.add((ICorfuSMR<T>) map);
    }

    /** Add map(s) to the list of maps to be checkpointed by this class. */

    public void addAllMaps(Collection<T> maps) {
        for (T map : maps) {
            addMap(map);
        }
    }


    /** Checkpoint multiple SMRMaps serially.
     *
     * @param rt CorfuRuntime
     * @param author Author's name, stored in checkpoint metadata
     * @return Global log address of the first record of
     */
    public Token appendCheckpoints(CorfuRuntime rt, String author) {
        log.info("appendCheckpoints: appending checkpoints for {} maps", maps.size());
        final long cpStart = System.currentTimeMillis();

        // TODO(Maithem) should we throw an exception if a new min is not discovered
        Token minSnapshot = Token.UNINITIALIZED;

        for (ICorfuSMR<T> map : maps) {
            UUID streamId = map.getCorfuStreamID();
            final long mapCpStart = System.currentTimeMillis();
            CheckpointWriter<T> cpw = new CheckpointWriter(rt, streamId, author, (T) map);
            ISerializer serializer =
                    ((CorfuCompileProxy<Map>) map.getCorfuSMRProxy())
                            .getSerializer();
            cpw.setSerializer(serializer);
            Token snapshot = cpw.appendCheckpoint();
            minSnapshot = Token.min(minSnapshot, snapshot);
            if (minSnapshot != Token.UNINITIALIZED && minSnapshot.getEpoch() != snapshot.getEpoch()) {
                throw new CheckpointException("Aborting checkpoint because epoch changed from "
                        + minSnapshot.getEpoch() + " to " + snapshot.getEpoch());
            }
            final long mapCpEnd = System.currentTimeMillis();
            log.info("appendCheckpoints: took {} ms to checkpoint {}",
                    mapCpEnd - mapCpStart, streamId);
        }

        final long cpStop = System.currentTimeMillis();
        log.info("appendCheckpoints: took {} ms to append {} checkpoints, min snapshot{}", cpStop - cpStart,
                maps.size(), minSnapshot);
        return minSnapshot;
    }

}
