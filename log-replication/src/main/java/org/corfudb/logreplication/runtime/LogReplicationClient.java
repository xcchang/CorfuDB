package org.corfudb.logreplication.runtime;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LogReplicationMetadataResponse;
import org.corfudb.protocols.wireprotocol.LogReplicationQueryLeadershipResponse;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class LogReplicationClient implements IClient {

    @Getter
    @Setter
    private IClientRouter router;

    @Setter
    private PriorityLevel priorityLevel = PriorityLevel.NORMAL;

    public LogReplicationClient(IClientRouter router) {
        this.router = router;
    }

    <T> CompletableFuture<T> sendMessageWithFuture(CorfuMsg msg) {
        return router.sendMessageAndGetCompletable(msg.setPriorityLevel(priorityLevel));
    }

    public String getHost() {
        return getRouter().getHost();
    }

    public Integer getPort() {
        return getRouter().getPort();
    }

    public CompletableFuture<Boolean> ping() {
        System.out.println("Ping 0 !!!!!! ");
        return getRouter().sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.PING).setEpoch(0));
    }

    public CompletableFuture<LogReplicationQueryLeadershipResponse> queryLeadership() {
        return sendMessageWithFuture(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP_REQUEST.msg());
    }

    public CompletableFuture<LogReplicationMetadataResponse> queryMetadata() {
        return sendMessageWithFuture(CorfuMsgType.LOG_REPLICATION_METADATA_REQUEST.msg());
    }
}
