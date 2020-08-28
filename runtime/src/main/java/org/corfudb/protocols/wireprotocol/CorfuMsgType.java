package org.corfudb.protocols.wireprotocol;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationLeadershipLoss;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;
import org.corfudb.runtime.view.Layout;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;

/**
 * Created by mwei on 8/8/16.
 */
@RequiredArgsConstructor
@AllArgsConstructor
public enum CorfuMsgType {
    // Base Messages
    PING(0, TypeToken.of(CorfuMsg.class), true, true),
    PONG(1, TypeToken.of(CorfuMsg.class), true, false),
    RESET(2, TypeToken.of(CorfuMsg.class), true, true),
    SEAL(3, new TypeToken<CorfuPayloadMsg<Long>>() {}, true, false),
    ACK(4, TypeToken.of(CorfuMsg.class), true, false),
    WRONG_EPOCH(5, new TypeToken<CorfuPayloadMsg<Long>>() {},  true, false),
    NACK(6, TypeToken.of(CorfuMsg.class)),
    VERSION_REQUEST(7, TypeToken.of(CorfuMsg.class), true, true),
    VERSION_RESPONSE(8, new TypeToken<JSONPayloadMsg<VersionInfo>>() {}, true, false),
    NOT_READY(9, TypeToken.of(CorfuMsg.class), true, false),
    WRONG_CLUSTER_ID(28, new TypeToken<CorfuPayloadMsg<WrongClusterMsg>>(){}, true, false),

    // Layout Messages
    LAYOUT_REQUEST(10, new TypeToken<CorfuPayloadMsg<Long>>(){}, true, true),
    LAYOUT_RESPONSE(11, TypeToken.of(LayoutMsg.class), true, false),
    LAYOUT_PREPARE(12, new TypeToken<CorfuPayloadMsg<LayoutPrepareRequest>>(){}, true, false),
    LAYOUT_PREPARE_REJECT(13, new TypeToken<CorfuPayloadMsg<LayoutPrepareResponse>>(){}),
    LAYOUT_PROPOSE(14, new TypeToken<CorfuPayloadMsg<LayoutProposeRequest>>(){}, true, false),
    LAYOUT_PROPOSE_REJECT(15, new TypeToken<CorfuPayloadMsg<LayoutProposeResponse>>(){}),
    LAYOUT_COMMITTED(16, new TypeToken<CorfuPayloadMsg<LayoutCommittedRequest>>(){}, true, false),
    LAYOUT_QUERY(17, new TypeToken<CorfuPayloadMsg<Long>>(){}),
    LAYOUT_BOOTSTRAP(18, new TypeToken<CorfuPayloadMsg<LayoutBootstrapRequest>>(){}, true, true),
    LAYOUT_NOBOOTSTRAP(19, TypeToken.of(CorfuMsg.class), true, false),

    // Sequencer Messages
    TOKEN_REQ(20, new TypeToken<CorfuPayloadMsg<TokenRequest>>(){}),
    TOKEN_RES(21, new TypeToken<CorfuPayloadMsg<TokenResponse>>(){}),
    BOOTSTRAP_SEQUENCER(22, new TypeToken<CorfuPayloadMsg<SequencerRecoveryMsg>>(){}),
    SEQUENCER_TRIM_REQ(23, new TypeToken<CorfuPayloadMsg<Long>>() {}),
    SEQUENCER_METRICS_REQUEST(24, TypeToken.of(CorfuMsg.class), true, false),
    SEQUENCER_METRICS_RESPONSE(25, new TypeToken<CorfuPayloadMsg<SequencerMetrics>>(){}, true, false),
    STREAMS_ADDRESS_REQUEST(26, new TypeToken<CorfuPayloadMsg<StreamsAddressRequest>>(){}),
    STREAMS_ADDRESS_RESPONSE(27, new TypeToken<CorfuPayloadMsg<StreamsAddressResponse>>(){}),

    // Logging Unit Messages
    WRITE(30, new TypeToken<CorfuPayloadMsg<WriteRequest>>() {}),
    READ_REQUEST(31, new TypeToken<CorfuPayloadMsg<ReadRequest>>() {}),
    READ_RESPONSE(32, new TypeToken<CorfuPayloadMsg<ReadResponse>>() {}),
    INSPECT_ADDRESSES_REQUEST(36, new TypeToken<CorfuPayloadMsg<InspectAddressesRequest>>() {}),
    INSPECT_ADDRESSES_RESPONSE(37, new TypeToken<CorfuPayloadMsg<InspectAddressesResponse>>() {}),
    PREFIX_TRIM(38, new TypeToken<CorfuPayloadMsg<TrimRequest>>() {}),
    TAIL_REQUEST(41, new TypeToken<CorfuPayloadMsg<TailsRequest>>(){}),
    TAIL_RESPONSE(42, new TypeToken<CorfuPayloadMsg<TailsResponse>>(){}),
    COMPACT_REQUEST(43, TypeToken.of(CorfuMsg.class), true, false),
    FLUSH_CACHE(44, TypeToken.of(CorfuMsg.class), true, false),
    TRIM_MARK_REQUEST(45, TypeToken.of(CorfuMsg.class)),
    TRIM_MARK_RESPONSE(46, new TypeToken<CorfuPayloadMsg<Long>>(){}),
    RESET_LOGUNIT(47, new TypeToken<CorfuPayloadMsg<Long>>(){}, true, false),
    LOG_ADDRESS_SPACE_REQUEST(48, TypeToken.of(CorfuMsg.class)),
    LOG_ADDRESS_SPACE_RESPONSE(49, new TypeToken<CorfuPayloadMsg<StreamsAddressResponse>>(){}),

    WRITE_OK(50, TypeToken.of(CorfuMsg.class)),
    ERROR_TRIMMED(51, TypeToken.of(CorfuMsg.class)),
    ERROR_OVERWRITE(52, new TypeToken<CorfuPayloadMsg<Integer>>(){}, true, false),
    ERROR_OOS(53, TypeToken.of(CorfuMsg.class)),
    ERROR_RANK(54, TypeToken.of(CorfuMsg.class)),
    ERROR_NOENTRY(55, TypeToken.of(CorfuMsg.class)),
    RANGE_WRITE(56, new TypeToken<CorfuPayloadMsg<RangeWriteMsg>>(){}),
    ERROR_DATA_CORRUPTION(57, new TypeToken<CorfuPayloadMsg<Long>>(){}),
    ERROR_DATA_OUTRANKED(58, TypeToken.of(CorfuMsg.class)),
    ERROR_VALUE_ADOPTED(59,new TypeToken<CorfuPayloadMsg<ReadResponse>>() {}),

    // EXTRA CODES
    LAYOUT_ALREADY_BOOTSTRAP(60, TypeToken.of(CorfuMsg.class), true, false),
    LAYOUT_PREPARE_ACK(61, new TypeToken<CorfuPayloadMsg<LayoutPrepareResponse>>(){}, true, false),
    RESTART(62, TypeToken.of(CorfuMsg.class), true, true),
    KEEP_ALIVE(63, TypeToken.of(CorfuMsg.class), true, true),
    COMMITTED_TAIL_REQUEST(64, TypeToken.of(CorfuMsg.class)),
    COMMITTED_TAIL_RESPONSE(65, new TypeToken<CorfuPayloadMsg<Long>>(){}),
    UPDATE_COMMITTED_TAIL(66, new TypeToken<CorfuPayloadMsg<Long>>(){}),

    // Management Messages
    MANAGEMENT_BOOTSTRAP_REQUEST(70, new TypeToken<CorfuPayloadMsg<Layout>>(){}, true, true),
    MANAGEMENT_NOBOOTSTRAP_ERROR(71, TypeToken.of(CorfuMsg.class), true, false),
    MANAGEMENT_ALREADY_BOOTSTRAP_ERROR(72, TypeToken.of(CorfuMsg.class), true, false),
    MANAGEMENT_HEALING_DETECTED(73, new TypeToken<CorfuPayloadMsg<DetectorMsg>>(){}, true, false),
    MANAGEMENT_FAILURE_DETECTED(74, new TypeToken<CorfuPayloadMsg<DetectorMsg>>(){}, true, false),
    ORCHESTRATOR_REQUEST(77, new TypeToken<CorfuPayloadMsg<OrchestratorMsg>>() {}, true, true),
    ORCHESTRATOR_RESPONSE(78, new TypeToken<CorfuPayloadMsg<OrchestratorResponse>>() {}, true, false),
    MANAGEMENT_LAYOUT_REQUEST(79, TypeToken.of(CorfuMsg.class), true, false),

    // Handshake Messages
    HANDSHAKE_INITIATE(80, new TypeToken<CorfuPayloadMsg<HandshakeMsg>>() {}, true, false),
    HANDSHAKE_RESPONSE(81, new TypeToken<CorfuPayloadMsg<HandshakeResponse>>() {}, true, false),

    NODE_STATE_REQUEST(82, TypeToken.of(CorfuMsg.class)),
    NODE_STATE_RESPONSE(83, new TypeToken<CorfuPayloadMsg<NodeState>>(){}, true, false),

    FAILURE_DETECTOR_METRICS_REQUEST(84, TypeToken.of(CorfuMsg.class)),
    FAILURE_DETECTOR_METRICS_RESPONSE(85, new TypeToken<CorfuPayloadMsg<NodeState>>(){}, true, false),

    KNOWN_ADDRESS_REQUEST(86, new TypeToken<CorfuPayloadMsg<KnownAddressRequest>>() {}),
    KNOWN_ADDRESS_RESPONSE(87, new TypeToken<CorfuPayloadMsg<KnownAddressResponse>>() {}),

    ERROR_SERVER_EXCEPTION(200, new TypeToken<CorfuPayloadMsg<ExceptionMsg>>() {}, true, false),

    LOG_REPLICATION_ENTRY(201, new TypeToken<CorfuPayloadMsg<LogReplicationEntry>>() {}, true, true),
    LOG_REPLICATION_METADATA_REQUEST(202, TypeToken.of(CorfuMsg.class), true, true),
    LOG_REPLICATION_METADATA_RESPONSE(203, new TypeToken<CorfuPayloadMsg<LogReplicationMetadataResponse>>() {}, true, true),
    LOG_REPLICATION_QUERY_LEADERSHIP(204, TypeToken.of(CorfuMsg.class), true, true),
    LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE(205, new TypeToken<CorfuPayloadMsg<LogReplicationQueryLeaderShipResponse>>(){}, true, true),
    LOG_REPLICATION_LEADERSHIP_LOSS(206, new TypeToken<CorfuPayloadMsg<LogReplicationLeadershipLoss>>(){}, true, true),
    ;


    public final int type;
    public final TypeToken<? extends CorfuMsg> messageType;
    public boolean ignoreEpoch = false;
    public boolean ignoreClusterId = false;

    public <T> CorfuPayloadMsg<T> payloadMsg(T payload) {
        // todo:: maybe some typechecking here (performance impact?)
        return new CorfuPayloadMsg<T>(this, payload);
    }

    public CorfuMsg msg() {
        return new CorfuMsg(this);
    }

    @FunctionalInterface
    interface MessageConstructor<T> {
        T construct();
    }

    @Getter(lazy = true)
    private final MessageConstructor<? extends CorfuMsg> constructor = resolveConstructor();

    public byte asByte() {
        return (byte) type;
    }

    /** A lookup representing the context we'll use to do lookups. */
    private static java.lang.invoke.MethodHandles.Lookup lookup = MethodHandles.lookup();

    /** Generate a lambda pointing to the constructor for this message type. */
    @SuppressWarnings("unchecked")
    private MessageConstructor<? extends CorfuMsg> resolveConstructor() {
        // Grab the constructor and get convert it to a lambda.
        try {
            Constructor t = messageType.getRawType().getConstructor();
            MethodHandle mh = lookup.unreflectConstructor(t);
            MethodType mt = MethodType.methodType(Object.class);
            try {
                return (MessageConstructor<? extends CorfuMsg>) LambdaMetafactory.metafactory(
                        lookup, "construct",
                        MethodType.methodType(MessageConstructor.class),
                        mt, mh, mh.type())
                        .getTarget().invokeExact();
            } catch (Throwable th) {
                throw new RuntimeException(th);
            }
        } catch (NoSuchMethodException nsme) {
            throw new RuntimeException("CorfuMsgs must include a no-arg constructor!");
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}