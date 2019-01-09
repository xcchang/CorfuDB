package org.corfudb.universe.dynamic;

import java.util.*;

public interface Stats {
    //Table status related field names
    public static final String CORRECT_TABLES = "TablesCorrect";
    public static final String INCORRECT_TABLES = "TablesIncorrect";
    public static final String PARTIALLY_CORRECT_TABLES = "TablesPartiallyCorrect";
    public static final String UNAVAILABLE_TABLES = "TablesUnavailable";
    
    //Cluster status related field names
    public static final String DEGRADED_CLUSTER_STATUS = "DegradedClusterStatus";
    public static final String STABLE_CLUSTER_STATUS = "StableClusterStatus";
    public static final String UNAVAILABLE_CLUSTER_STATUS = "UnavailableClusterStatus";
    
    //Server status related field names
    public static final String LAYOUT_SERVERS = "LayoutServers";
    public static final String LOG_UNIT_SERVERS = "LogUnitServers";
    public static final String PRIMARY_SEQUENCERS = "PrimarySequencers";
    public static final String DB_SYNCING_SERVERS = "DBSyncingServers";
    public static final String DOWN_SERVERS = "DownServers";
    public static final String UP_SERVERS = "UpServers";
    
    //Table access related field names
    public static final String GET_THROUGHPUT_AVERAGE = "GetThroughputAverage";
    public static final String PUT_THROUGHPUT_AVERAGE = "PutThroughputAverage";
    
    //Node performance related field names
    //Cpu metrics
    public static final String TOTAL_CPU_USAGE_AVERAGE = "TotalCpuUsageAverage";
    public static final String CPU_USAGE_IN_KERNELMODE_AVERAGE = "CpuUsageInKernelmodeAverage";
    public static final String CPU_USAGE_IN_USERMODE_AVERAGE = "CpuUsageInUsermodeAverage";
    public static final String SYSTEM_CPU_USAGE_AVERAGE = "SystemCpuUsageAverage";
    //Memory metrics
    public static final String MEMORY_USAGE_AVERAGE = "MemoryUsageAverage";
    public static final String MEMORY_MAX_USAGE_AVERAGE = "MemoryMaxUsageAverage";
    public static final String MEMORY_FAILCNT_AVERAGE = "MemoryFailcntAverage";
    public static final String MEMORY_LIMIT_AVERAGE = "MemoryLimitAverage";
    //Network metrics
    public static final String NET_RX_BYTES_AVERAGE = "NetRxBytesAverage";
    public static final String NET_RX_DROPPED_AVERAGE = "NetRxDroppedAverage";
    public static final String NET_RX_ERRORS_AVERAGE = "NetRxErrorsAverage";
    public static final String NET_RX_PACKETS_AVERAGE = "NetRxPacketsAverage";
    public static final String NET_TX_BYTES_AVERAGE = "NetTxBytesAverage";
    public static final String NET_TX_DROPPED_AVERAGE = "NetTxDroppedAverage";
    public static final String NET_TX_ERRORS_AVERAGE = "NetTxErrorsAverage";
    public static final String NET_TX_PACKETS_AVERAGE = "NetTxPacketsAverage";
    
    //Setup related field names
    public static final String CLIENTS_TOTAL = "Clients";
    public static final String SERVERS_TOTAL = "Servers";
    public static final String TABLES_TOTAL = "Tables";
    public static final String ELAPSED_TIME = "ElapsedTime";

    //Prefixes
    public static final String CLIENT_PREFIX = "Client";
    public static final String CLIENT_SERVER_PREFIX = "ClientServer";
    public static final String TABLE_STATS_PREFIX = "TableStat";
    public static final String SERVER_PREFIX = "Server";

    public static final String SEPARATOR = "_";

    public static final List<String> SHOW_LIST = Arrays.asList(CORRECT_TABLES, INCORRECT_TABLES, UNAVAILABLE_TABLES,
            DEGRADED_CLUSTER_STATUS, STABLE_CLUSTER_STATUS, UNAVAILABLE_CLUSTER_STATUS);


    static LinkedHashMap<String, String> getPrintableFlatStats(String prefix, Stats objectWithStats){
        LinkedHashMap<String, Object> stats = objectWithStats.getStats();
        LinkedHashMap<String, String> flatPrintableStats = new LinkedHashMap<>();
        recursiveFlatStats(prefix, stats, flatPrintableStats);
        return flatPrintableStats;
    }

    static void recursiveFlatStats(String prefix, LinkedHashMap<String, Object> inputMap,
                                   LinkedHashMap<String, String> outputMap){
        for(Map.Entry<String, Object> e: inputMap.entrySet()){
            String key = prefix.isEmpty() ? e.getKey() : prefix + SEPARATOR + e.getKey();
            if(e.getValue() instanceof LinkedHashMap)
                recursiveFlatStats(key, (LinkedHashMap)e.getValue(), outputMap);
            else
                outputMap.put(key, e.getValue().toString());
        }
    }

    static String compareStats(String name, String realValueStr, String desireValueStr){
        String result;
        try{
            double realValue = Double.parseDouble(realValueStr);
            double desireValue = Double.parseDouble(desireValueStr);
            double resultNumber = realValue - desireValue;
            result = Double.toString(resultNumber);
        }catch (Exception ex){
            result = String.format("%s - %s", realValueStr, desireValueStr);
        }
        return result;
    }

    public LinkedHashMap<String, Object> getStats();
}
