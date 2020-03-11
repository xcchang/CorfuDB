package org.corfudb.logreplication.infrastructure;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Properties;

@Slf4j
public class LogReplicationConfig {
    private static final String config_file = "/config/corfu/corfu_replication_config.properties";
    private static final String DEFAULT_PRIMARY_PORT_NUM = "9010";
    private static final String DEFAULT_BACKUP_PORT_NUM = "9020";
    private static final String DEFAULT_PRIMARY_IP = "localhost";
    private static final String DEFAULT_BACKUP_IP = "localhost";
    private static final String DEFAULT_PRIMARY_SITE_NAME = "primary_site";
    private static final String DEFAULT_BACKUP_SITE_NAME = "backup_site";
    private static final int NUM_NODES_PER_CLUSTER = 3;


    private static final String PRIMARY_SITE_NAME = "primary_site";
    private static final String BACKUP_SITE_NAME = "backup_site";
    private static final String LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM = "LOG_REPLICATION_SERVICE_SENDER_PORT_NUM";
    private static final String LOG_REPLICATION_SERVICE_BACKUP_PORT_NUM = "LOG_REPLICATION_SERVICE_RECEIVER_PORT_NUM ";

    private static final String PRIMARY_SITE_NODE = "primary_site_node";
    private static final String BACKUP_SITE_NODE = "backup_site_node";

    @Getter
    SiteInfo primarySite;

    @Getter
    SiteInfo backupSite;

    public void LogReplicationConfig() {
        readConfig();
    }

    private void readConfig() {
        try {
            File configFile = new File(config_file);
            FileReader reader = new FileReader(configFile);

            Properties props = new Properties();
            props.load(reader);

            /**
             * PrimarySite string
             * portnumber 20
             * primarynode1 ip
             * primarynode2 ip
             * primarynode3 ip
             *
             * BackupSite string
             * backupnode1 ip
             * backupnode2 ip
             * backupnode3 ip
             */

            // Setup primary site information
            primarySite = new SiteInfo(props.getProperty(PRIMARY_SITE_NAME, DEFAULT_PRIMARY_SITE_NAME));
            String portNum = props.getProperty(LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM, DEFAULT_PRIMARY_PORT_NUM);
            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String ipAddress = props.getProperty(PRIMARY_SITE_NODE +"i", DEFAULT_PRIMARY_IP);
                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.PrimarySite);
                primarySite.nodeInfos.add(nodeInfo);
            }

            // Setup backup site information
            backupSite = new SiteInfo(props.getProperty(BACKUP_SITE_NAME, DEFAULT_BACKUP_SITE_NAME));
            portNum = props.getProperty(LOG_REPLICATION_SERVICE_BACKUP_PORT_NUM, DEFAULT_BACKUP_PORT_NUM);
            for (int i = 0; i < 3; i++) {
                String ipAddress = props.getProperty(BACKUP_SITE_NODE +"i", DEFAULT_BACKUP_IP);
                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.BackupSite);
                backupSite.nodeInfos.add(nodeInfo);
            }

            reader.close();
            log.info("Primary Site Info {}; Backup Site Info {}", primarySite, backupSite);
        } catch (Exception e) {
            log.warn("Caught an exception while reading the config file: {}", e.getCause());
        }
    }

    NodeInfo getNodeInfo(String ipaddress) {
        NodeInfo nodeInfo = getNodeInfo(primarySite, ipaddress);

        if (nodeInfo == null) {
            nodeInfo = getNodeInfo(backupSite, ipaddress);
        }

        if (nodeInfo == null) {
            log.warn("There is no nodeInfo for ipaddress {} ", ipaddress);
        }

        return nodeInfo;
    }

    NodeInfo getNodeInfo(SiteInfo siteInfo, String ipaddress) {
        for (NodeInfo nodeInfo: siteInfo.getNodeInfos()) {
            if (nodeInfo.getIpAddress() == ipaddress) {
                return nodeInfo;
            }
        }

        log.warn("There is no nodeInfo for ipaddress {} ", ipaddress);
        return null;
    }

    static class SiteInfo {
        String corfuClusterName;
        @Getter
        ArrayList<NodeInfo> nodeInfos;

        SiteInfo(String name) {
            corfuClusterName = name;
            nodeInfos = new ArrayList<>();
        }
    }

    @Data
    public class NodeInfo {
        RoleType roleType;
        String ipAddress;
        String portNum;
        boolean lockHolder;
        LogReplicationRuntime runtime;

        NodeInfo(String ipAddress, String portNum, RoleType type) {
            this.ipAddress = ipAddress;
            this.portNum = portNum;
            this.roleType = type;
            this.lockHolder = false;
        }
    }

    enum RoleType {
        PrimarySite,
        BackupSite
    }
}

