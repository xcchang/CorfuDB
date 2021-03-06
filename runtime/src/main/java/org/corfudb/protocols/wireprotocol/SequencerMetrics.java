package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.Data;

/**
 * Sequencer metrics for a node.
 *
 * <p>Created by zlokhandwala on 4/12/18.
 */
@Data
public class SequencerMetrics implements ICorfuPayload<SequencerMetrics> {

    public static final SequencerMetrics READY = new SequencerMetrics(SequencerStatus.READY);
    public static final SequencerMetrics NOT_READY = new SequencerMetrics(SequencerStatus.NOT_READY);
    public static final SequencerMetrics UNKNOWN = new SequencerMetrics(SequencerStatus.UNKNOWN);

    public enum SequencerStatus {
        // Sequencer is in READY state, and can dispatch tokens.
        READY,
        // Sequencer is in a NOT_READY state.
        NOT_READY,
        // Unknown state.
        UNKNOWN
    }

    /**
     * Ready state ofN a sequencer to determine its READY/NOT_READY state.
     */
    private final SequencerStatus sequencerStatus;

    public SequencerMetrics(SequencerStatus sequencerStatus) {
        this.sequencerStatus = sequencerStatus;
    }

    public SequencerMetrics(ByteBuf buf) {
        sequencerStatus = SequencerStatus.valueOf(ICorfuPayload.fromBuffer(buf, String.class));
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, sequencerStatus.toString());
    }
}