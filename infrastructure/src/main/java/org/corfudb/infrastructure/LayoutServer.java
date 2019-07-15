package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.paxos.Paxos;
import org.corfudb.infrastructure.paxos.Paxos.OperationResult;
import org.corfudb.infrastructure.paxos.PersistentPaxosDataStore;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.protocols.wireprotocol.LayoutCommittedRequest;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutPrepareRequest;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.protocols.wireprotocol.LayoutProposeRequest;
import org.corfudb.protocols.wireprotocol.LayoutProposeResponse;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The layout server serves layouts, which are used by clients to find the
 * Corfu infrastructure.
 *
 * <p>For replication and high availability, the layout server implements a
 * basic Paxos protocol. The layout server functions as a Paxos acceptor,
 * and accepts proposals from clients consisting of a rank and desired
 * layout. The protocol consists of three rounds:
 *
 * <p>1)   Prepare(rank) - Clients first contact each server with a rank.
 * If the server responds with ACK, the server promises not to
 * accept any requests with a rank lower than the given rank.
 * If the server responds with LAYOUT_PREPARE_REJECT, the server
 * informs the client of the current high rank and the request is
 * rejected.
 *
 * <p>2)   Propose(rank,layout) - Clients then contact each server with
 * the previously prepared rank and the desired layout. If no other
 * client has sent a prepare with a higher rank, the layout is
 * persisted, and the server begins serving that layout to other
 * clients. If the server responds with LAYOUT_PROPOSE_REJECT,
 * either another client has sent a prepare with a higher rank,
 * or this was a propose of a previously accepted rank.
 *
 * <p>3)   Committed(rank, layout) - Clients then send a hint to each layout
 * server that a new rank has been accepted by a quorum of
 * servers.
 *
 * <p>Created by mwei on 12/8/15.
 */
//TODO Finer grained synchronization needed for this class.
//TODO Need a janitor to cleanup old phases data and to fill up holes in layout history.
@Slf4j
public class LayoutServer extends AbstractServer {

    @Getter
    private final ServerContext serverContext;

    @Getter
    private final Paxos paxos;

    /**
     * Handler for this server.
     */
    @Getter
    private final CorfuMsgHandler handler =
            CorfuMsgHandler.generateHandler(MethodHandles.lookup(), this);

    private final ExecutorService executor;

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    @Override
    public ExecutorService getExecutor(CorfuMsgType corfuMsgType) {
        return executor;
    }

    @Override
    public List<ExecutorService> getExecutors() {
        return Collections.singletonList(executor);
    }

    /**
     * Returns new LayoutServer for context.
     *
     * @param serverContext context object providing settings and objects
     */
    public LayoutServer(@Nonnull ServerContext serverContext) {
        this.serverContext = serverContext;
        executor = Executors.newFixedThreadPool(serverContext.getLayoutServerThreadCount(),
                new ServerThreadFactory("layoutServer-", new ServerThreadFactory.ExceptionHandler()));

        PersistentPaxosDataStore paxosDs = PersistentPaxosDataStore.builder()
                .dataStore(serverContext.getDataStore())
                .serverContext(serverContext)
                .build();
        this.paxos = Paxos.builder().dataStore(paxosDs).build();

        if (serverContext.installSingleNodeLayoutIfAbsent()) {
            setLayoutInHistory(getCurrentLayout());
        }
    }

    private boolean isBootstrapped(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (getCurrentLayout() == null) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_NOBOOTSTRAP));
            return false;
        }
        return true;
    }

    // Helper Methods

    /**
     * Handle a layout request message.
     *
     * @param msg              corfu message containing LAYOUT_REQUEST
     * @param ctx              netty ChannelHandlerContext
     * @param r                server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_REQUEST)
    public synchronized void handleMessageLayoutRequest(CorfuPayloadMsg<Long> msg,
                                                    ChannelHandlerContext ctx, IServerRouter r) {
        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }
        long epoch = msg.getPayload();
        if (epoch <= serverContext.getServerEpoch()) {
            r.sendResponse(ctx, msg, new LayoutMsg(getCurrentLayout(), CorfuMsgType
                    .LAYOUT_RESPONSE));
        } else {
            // else the client is somehow ahead of the server.
            //TODO figure out a strategy to deal with this situation
            long serverEpoch = serverContext.getServerEpoch();
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.warn("handleMessageLayoutRequest: Message Epoch {} ahead of Server epoch {}",
                    epoch, serverEpoch);
        }
    }

    /**
     * Sets the new layout if the server has not been bootstrapped with one already.
     *
     * @param msg corfu message containing LAYOUT_BOOTSTRAP
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_BOOTSTRAP)
    public synchronized void handleMessageLayoutBootstrap(
            @NonNull CorfuPayloadMsg<LayoutBootstrapRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {

        if (getCurrentLayout() == null) {
            log.info("handleMessageLayoutBootstrap: Bootstrap with new layout={}, {}",
                    msg.getPayload().getLayout(), msg);
            setCurrentLayout(msg.getPayload().getLayout());
            serverContext.setServerEpoch(getCurrentLayout().getEpoch(), r);
            //send a response that the bootstrap was successful.
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } else {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("handleMessageLayoutBootstrap: Got a request to bootstrap a server which is "
                    + "already bootstrapped, rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP));
        }
    }

    /**
     * Accepts a prepare message if the rank is higher than any accepted so far.
     *
     * @param msg corfu message containing LAYOUT_PREPARE
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    // TODO this can work under a separate lock for this step as it does not change the global
    // components
    @ServerHandler(type = CorfuMsgType.LAYOUT_PREPARE)
    public synchronized void handleMessageLayoutPrepare(
            @NonNull CorfuPayloadMsg<LayoutPrepareRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {

        // Check if the prepare is for the correct epoch
        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        Rank prepareRank = new Rank(msg.getPayload().getRank(), msg.getClientID());

        OperationResult result = paxos.prepare(prepareRank);

        switch (result.getStatus()){
            case OK:
                // Return the layout with the highest rank proposed before.
                log.debug("handleMessageLayoutPrepare: New phase 1 rank={}", result.getRank());
                r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PREPARE_ACK.payloadMsg(new
                        LayoutPrepareResponse(result.getRank().getRank(), result.getLayout())));
                break;
            case REJECTED:
                // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
                log.debug("handleMessageLayoutPrepare: Rejected phase 1 prepare of rank={}, "
                        + "phase1Rank={}", prepareRank, result.getRank());
                r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PREPARE_REJECT.payloadMsg(new
                        LayoutPrepareResponse(result.getRank().getRank(), result.getLayout())));
                break;
        }
    }

    /**
     * Accepts a proposal for which it had accepted in the prepare phase.
     * A minor optimization is to reject any duplicate propose messages.
     *
     * @param msg corfu message containing LAYOUT_PROPOSE
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_PROPOSE)
    public synchronized void handleMessageLayoutPropose(
            @NonNull CorfuPayloadMsg<LayoutProposeRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {

        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        Optional<LayoutProposeResponse> result = paxos.propose(msg.getPayload(), msg.getClientID());
        if(!result.isPresent()){
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        }

        result.ifPresent(resp -> {
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(resp));
        });
    }


    /**
     * Force layout enables the server to bypass consensus
     * and accept a new layout.
     *
     * @param msg              corfu message containing LAYOUT_FORCE
     * @param ctx              netty ChannelHandlerContext
     * @param r                server router
     */
    private synchronized void forceLayout(@Nonnull CorfuPayloadMsg<LayoutCommittedRequest> msg,
                                               @Nonnull ChannelHandlerContext ctx,
                                               @Nonnull IServerRouter r) {
        LayoutCommittedRequest req = msg.getPayload();

        if (req.getEpoch() != getServerEpoch()) {
            // return can't force old epochs
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            log.warn("forceLayout: Trying to force a layout with an old epoch. Layout {}, " +
                    "current epoch {}", req.getLayout(), getServerEpoch());
            return;
        }

        setCurrentLayout(req.getLayout());
        serverContext.setServerEpoch(req.getLayout().getEpoch(), r);
        log.warn("forceLayout: Forcing new layout {}", req.getLayout());
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }


    /**
     * Accepts any committed layouts for the current epoch or newer epochs.
     * As part of the accept, the server changes it's current layout and epoch.
     *
     * @param msg corfu message containing LAYOUT_COMMITTED
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    // TODO If a server does not get SET_EPOCH layout commit message cannot reach it
    // TODO as this message is not set to ignore EPOCH.
    // TODO How do we handle holes in history if we let in layout commit message. Maybe we have a
    // hole filling process
    @ServerHandler(type = CorfuMsgType.LAYOUT_COMMITTED)
    public synchronized void handleMessageLayoutCommit(
            @NonNull CorfuPayloadMsg<LayoutCommittedRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {

        if (msg.getPayload().getForce()) {
            forceLayout(msg, ctx, r);
            return;
        }

        Layout commitLayout = msg.getPayload().getLayout();
        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        setCurrentLayout(commitLayout);
        serverContext.setServerEpoch(msg.getPayload().getEpoch(), r);
        log.info("New layout committed: {}", commitLayout);
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }


    public Layout getCurrentLayout() {
        Layout layout = serverContext.getCurrentLayout();
        if (layout != null) {
            return new Layout(layout);
        } else {
            return null;
        }
    }

    /**
     * Sets the current layout in context DataStore.
     *
     * @param layout layout to set
     */
    public void setCurrentLayout(Layout layout) {
        serverContext.setCurrentLayout(layout);
        // set the layout in history as well
        setLayoutInHistory(layout);
    }

    public void setLayoutInHistory(Layout layout) {
        serverContext.setLayoutInHistory(layout);
    }

    private long getServerEpoch() {
        return serverContext.getServerEpoch();
    }
}
