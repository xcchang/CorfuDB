package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;
import org.corfudb.protocols.wireprotocol.LogReplicationMetadataResponse;
import org.corfudb.protocols.wireprotocol.LogReplicationQueryLeadershipResponse;
import org.corfudb.runtime.RuntimeParameters;
import org.corfudb.logreplication.infrastructure.LogReplicationConfig.NodeInfo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

@Slf4j
/**
 * Try to get hold of lease and find both Primary and Standby leader.
 */
public class CorfuReplicationDiscoveryService implements Runnable {

    private final CorfuReplicationServerNode logReplicationServer;

    String ipAddress;
    @Getter
    LogReplicationConfig config;
    NodeInfo receiverLeaderNode;
    NodeInfo myNodeInfo;


    public CorfuReplicationDiscoveryService(CorfuReplicationServerNode logReplicationServer) {
        this.logReplicationServer = logReplicationServer;
        config = new LogReplicationConfig();
    }

    @Override
    public void run() {
        //TODO: call siteManager API to determine if current node is PRIMARY or STANDBY
        // for now, read from a SiteConfig configuration file that has primary and standby
        // nodes information and ipaddresses
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            ipAddress = inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            log.error("caught an exception {} ", e);
        }

        if (true)
            return;
        // the configuration will determine my roletype
        myNodeInfo = config.getNodeInfo(ipAddress);

        //TODO: Try to acquire the lease (Srinivas)

        //TODO: Query lease holder to see if I am the lease holder
        myNodeInfo.setLockHolder(true);

        switch (myNodeInfo.getRoleType()) {
            case PrimarySite:
                if (myNodeInfo.isLockHolder()) {
                    setupReceiverRuntime();
                    discoverReceiverLockHolder();

                    // query the leader node and get the receiver metadata information
                    //TODO: according to the response, generate a fullsync or delta sync request.
                    try {
                        LogReplicationMetadataResponse response =  receiverLeaderNode.getRuntime().getClient().queryMetadata().get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }

                    //CorfuReplicationServer.startLogReplication();
                }
                break;
            case BackupSite:
                if (myNodeInfo.isLockHolder()) {
                    ;;
                    //CorfuReplicationServer.startLogApply();
                }
                break;
            default:
                log.error("wrong RoleType {}", myNodeInfo);
        }
    }

    void setupReceiverRuntime() {
        for (NodeInfo nodeInfo : config.getBackupSite().getNodeInfos()) {
            LogReplicationRuntime runtime = new LogReplicationRuntime(RuntimeParameters.builder().build());
            runtime.connect(nodeInfo.getIpAddress() + ":" + nodeInfo.getPortNum());
            nodeInfo.setRuntime(runtime);
        }
    }

    void discoverReceiverLockHolder () {
        Exception result = null;
        ArrayList<LogReplicationQueryLeadershipResponse> leadershipResponses = new ArrayList<>();

        //setup receiver nodes runtime and do a query of leadership at the same time.
        while (leadershipResponses.size() == 0) {
            for (NodeInfo nodeInfo : config.getBackupSite().getNodeInfos()) {
                LogReplicationRuntime runtime = nodeInfo.getRuntime();
                try {
                    leadershipResponses.add(runtime.getClient().queryLeadership().get());
                } catch (ExecutionException | InterruptedException e) {
                    log.warn("caught an exception {}", e);
                }
            }
        }

        //process leader responses and pickup the recent leader's ipaddress
        LogReplicationQueryLeadershipResponse leader = leadershipResponses.get(0);
        for (LogReplicationQueryLeadershipResponse response : leadershipResponses) {
            if (response.getEpoch() > leader.getEpoch()) {
                leader = response;
            }
        }

        //setup the leaderNode with the correct nodeInfo
        receiverLeaderNode = config.getNodeInfo(config.getBackupSite(), leader.getIpAddress());
    }
}
