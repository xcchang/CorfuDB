package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LogReplicationMetadataResponse;
import org.corfudb.protocols.wireprotocol.LogReplicationQueryLeadershipResponse;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class LogReplicationServer extends AbstractServer {

    final ServerContext serverContext;

    private final ExecutorService executor;

    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    public LogReplicationServer(@Nonnull ServerContext context) {
        this.serverContext = context;
        executor = Executors.newFixedThreadPool(1,
                new ServerThreadFactory("LogReplicationServer-", new ServerThreadFactory.ExceptionHandler()));
    }

    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandler().handle(msg, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    /**
     * Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.PING)
    private void ping(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("PING received by Replication Server");
        r.sendResponse(ctx, msg, CorfuMsgType.PONG.msg());
    }

    public LogReplicationQueryLeadershipResponse getLeadership() {
        String ipAddress = null;
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            ipAddress = inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            log.error("caught an exception {}", e);
        }

        return new LogReplicationQueryLeadershipResponse(0, ipAddress);
    }

    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP_REQUEST)
    private void handleQueryLeadership(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("query leadership {}", msg);
        LogReplicationQueryLeadershipResponse response = getLeadership();
        r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_LEADERSHIP_RESPONSE.payloadMsg(response));
    }


    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_METADATA_REQUEST)
    private void handleQueryMetaData(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("query leadership {}", msg);
        LogReplicationMetadataResponse response = null;
        // TODO call persistentmetadata api
        r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_METADATA_RESPONSE.payloadMsg(response));
    }

}
