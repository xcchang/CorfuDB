package org.corfudb.universe.dynamic.state;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerStats;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.dynamic.Stats;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Represents the base state of a node (server or client) in a point of time.
 * Contains cpu, memory and network metrics
 */
@Getter
@ToString
@Slf4j
public abstract class NodeState implements Cloneable, Stats {

    /**
     * Controls if network stats should be monitored.
     * At this momento the network stats always return null, that's why it is disabled.
     */
    public static boolean ENABLE_NETWORK_STATS = false;

    /**
     * Controller that allows to obtain system metrics from the host of the node
     */
    private DockerClient hostController;

    /**
     * Period for the system metrics monitor
     */
    private long monitorPeriod;

    /**
     * Time units of the period for the system metrics monitor
     */
    private TimeUnit monitorPeriodUnit;

    /**
     * Thread executor for node monitoring
     */
    private ScheduledExecutorService monitorExecutor;

    //CPU metrics
    private double totalCpuUsageAverage;
    private double cpuUsageInKernelmodeAverage;
    private double cpuUsageInUsermodeAverage;
    private double systemCpuUsageAverage;
    //CPU records
    private final List<Long> totalCpuUsageRecords = new ArrayList<>();
    private final List<Long> cpuUsageInKernelmodeRecords = new ArrayList<>();
    private final List<Long> cpuUsageInUsermodeRecords = new ArrayList<>();
    private final List<Long> systemCpuUsageRecords = new ArrayList<>();

    //Memory metrics
    private double memoryUsageAverage;
    private double memoryMaxUsageAverage;
    private double memoryFailcntAverage;
    private double memoryLimitAverage;
    //Memory records
    private final List<Long> memoryUsageRecords = new ArrayList<>();
    private final List<Long> memoryMaxUsageRecords = new ArrayList<>();
    private final List<Long> memoryFailcntRecords = new ArrayList<>();
    private final List<Long> memoryLimitRecords = new ArrayList<>();

    //Network metrics
    private double netRxBytesAverage;
    private double netRxDroppedAverage;
    private double netRxErrorsAverage;
    private double netRxPacketsAverage;
    private double netTxBytesAverage;
    private double netTxDroppedAverage;
    private double netTxErrorsAverage;
    private double netTxPacketsAverage;
    //Network records
    private final List<Long> netRxBytesRecords = new ArrayList<>();
    private final List<Long> netRxDroppedRecords = new ArrayList<>();
    private final List<Long> netRxErrorsRecords = new ArrayList<>();
    private final List<Long> netRxPacketsRecords = new ArrayList<>();
    private final List<Long> netTxBytesRecords = new ArrayList<>();
    private final List<Long> netTxDroppedRecords = new ArrayList<>();
    private final List<Long> netTxErrorsRecords = new ArrayList<>();
    private final List<Long> netTxPacketsRecords = new ArrayList<>();

    public void startMetricsRecording() {
        //CPU records
        totalCpuUsageRecords.clear();
        cpuUsageInKernelmodeRecords.clear();
        cpuUsageInUsermodeRecords.clear();
        systemCpuUsageRecords.clear();
        //Memory records
        memoryUsageRecords.clear();
        memoryMaxUsageRecords.clear();
        memoryFailcntRecords.clear();
        memoryLimitRecords.clear();
        //Network records
        netRxBytesRecords.clear();
        netRxDroppedRecords.clear();
        netRxErrorsRecords.clear();
        netRxPacketsRecords.clear();
        netTxBytesRecords.clear();
        netTxDroppedRecords.clear();
        netTxErrorsRecords.clear();
        netTxPacketsRecords.clear();
        //StartServerEvent monitor
        try {
            this.monitorExecutor = Executors.newSingleThreadScheduledExecutor();
            this.monitorExecutor.scheduleAtFixedRate(() -> {
                try {
                    ContainerStats containerStats = hostController.stats(this.getName());
                    //Cpu metrics
                    totalCpuUsageRecords.add(containerStats.cpuStats().cpuUsage().totalUsage());
                    cpuUsageInKernelmodeRecords.add(containerStats.cpuStats().cpuUsage().usageInKernelmode());
                    cpuUsageInUsermodeRecords.add(containerStats.cpuStats().cpuUsage().usageInUsermode());
                    systemCpuUsageRecords.add(containerStats.cpuStats().systemCpuUsage());
                    //Memory metrics
                    memoryUsageRecords.add(containerStats.memoryStats().usage());
                    memoryMaxUsageRecords.add(containerStats.memoryStats().maxUsage());
                    memoryFailcntRecords.add(containerStats.memoryStats().failcnt());
                    memoryLimitRecords.add(containerStats.memoryStats().limit());
                    //Network metrics
                    if(ENABLE_NETWORK_STATS) {
                        netRxBytesRecords.add(containerStats.network().rxBytes());
                        netRxDroppedRecords.add(containerStats.network().rxDropped());
                        netRxErrorsRecords.add(containerStats.network().rxErrors());
                        netRxPacketsRecords.add(containerStats.network().rxPackets());
                        netTxBytesRecords.add(containerStats.network().txBytes());
                        netTxDroppedRecords.add(containerStats.network().txDropped());
                        netTxErrorsRecords.add(containerStats.network().txErrors());
                        netTxPacketsRecords.add(containerStats.network().txPackets());
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    log.warn("Failed getting serverStats for node: {}", getName(), e);
                }
            }, 0, monitorPeriod, monitorPeriodUnit);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Computes a average of the records value considering the previous average as
     * one initial record.
     * This computation is equivalent to a weighted moving average wich give more
     * weight to the current records.
     *
     * @param oldAverageValue the previous average compute with this function.
     * @param newRecords      new records to average.
     * @return the new average.
     */
    private static double getWMAverage(double oldAverageValue, List<Long> newRecords, String metricName) {
        try{
            if(newRecords.removeIf(e -> e == null)){
                log.info(String.format("Null records found for metric %s", metricName));
            }
            newRecords.add(new Float(oldAverageValue).longValue());
            double newAverageValue = newRecords.stream().mapToLong(d -> d).average().orElse(0.0d);
            return newAverageValue;
        }
        catch (Exception ex){
            log.error(String.format("Error while computing average for metric %s", metricName), ex);
            return oldAverageValue;
        }
    }

    public void stopMetricsRecording() {
        if(this.monitorExecutor != null)
            this.monitorExecutor.shutdownNow();
        //CPU records
        totalCpuUsageAverage = getWMAverage(totalCpuUsageAverage, totalCpuUsageRecords, "totalCpuUsage");
        cpuUsageInKernelmodeAverage = getWMAverage(cpuUsageInKernelmodeAverage, cpuUsageInKernelmodeRecords, "cpuUsageInKernelmode");
        cpuUsageInUsermodeAverage = getWMAverage(cpuUsageInUsermodeAverage, cpuUsageInUsermodeRecords, "cpuUsageInUsermode");
        systemCpuUsageAverage = getWMAverage(systemCpuUsageAverage, systemCpuUsageRecords, "systemCpuUsage");
        //Memory records
        memoryUsageAverage = getWMAverage(memoryUsageAverage, memoryUsageRecords, "memoryUsage");
        memoryMaxUsageAverage = getWMAverage(memoryMaxUsageAverage, memoryMaxUsageRecords, "memoryMaxUsage");
        memoryFailcntAverage = getWMAverage(memoryFailcntAverage, memoryFailcntRecords, "memoryFailcnt");
        memoryLimitAverage = getWMAverage(memoryLimitAverage, memoryLimitRecords, "memoryLimit");
        //Network records
        if(ENABLE_NETWORK_STATS) {
            netRxBytesAverage = getWMAverage(netRxBytesAverage, netRxBytesRecords, "netRxBytes");
            netRxDroppedAverage = getWMAverage(netRxDroppedAverage, netRxDroppedRecords, "netRxDropped");
            netRxErrorsAverage = getWMAverage(netRxErrorsAverage, netRxErrorsRecords, "netRxErrors");
            netRxPacketsAverage = getWMAverage(netRxPacketsAverage, netRxPacketsRecords, "netRxPackets");
            netTxBytesAverage = getWMAverage(netTxBytesAverage, netTxBytesRecords, "netTxBytes");
            netTxDroppedAverage = getWMAverage(netTxDroppedAverage, netTxDroppedRecords, "netTxDropped");
            netTxErrorsAverage = getWMAverage(netTxErrorsAverage, netTxErrorsRecords, "netTxErrors");
            netTxPacketsAverage = getWMAverage(netTxPacketsAverage, netTxPacketsRecords, "netTxPackets");
        }
    }

    protected void updateClone(NodeState clone) {
        //Cpu metrics
        clone.totalCpuUsageAverage = totalCpuUsageAverage;
        clone.cpuUsageInKernelmodeAverage = cpuUsageInKernelmodeAverage;
        clone.cpuUsageInUsermodeAverage = cpuUsageInUsermodeAverage;
        clone.systemCpuUsageAverage = systemCpuUsageAverage;
        //Memory metrics
        clone.memoryUsageAverage = memoryUsageAverage;
        clone.memoryMaxUsageAverage = memoryMaxUsageAverage;
        clone.memoryFailcntAverage = memoryFailcntAverage;
        clone.memoryLimitAverage = memoryLimitAverage;
        //Network metrics
        if(ENABLE_NETWORK_STATS) {
            clone.netRxBytesAverage = netRxBytesAverage;
            clone.netRxDroppedAverage = netRxDroppedAverage;
            clone.netRxErrorsAverage = netRxErrorsAverage;
            clone.netRxPacketsAverage = netRxPacketsAverage;
            clone.netTxBytesAverage = netTxBytesAverage;
            clone.netTxDroppedAverage = netTxDroppedAverage;
            clone.netTxErrorsAverage = netTxErrorsAverage;
            clone.netTxPacketsAverage = netTxPacketsAverage;
        }
    }

    /**
     * Summary of the state in form of a map field->value.
     *
     * @return Summary table of the state.
     */
    public LinkedHashMap<String, Object> getStats() {
        LinkedHashMap<String, Object> sortedAggregations = new LinkedHashMap<>();
        //Cpu metrics
        sortedAggregations.put(TOTAL_CPU_USAGE_AVERAGE, totalCpuUsageAverage);
        sortedAggregations.put(CPU_USAGE_IN_KERNELMODE_AVERAGE, cpuUsageInKernelmodeAverage);
        sortedAggregations.put(CPU_USAGE_IN_USERMODE_AVERAGE, cpuUsageInUsermodeAverage);
        sortedAggregations.put(SYSTEM_CPU_USAGE_AVERAGE, systemCpuUsageAverage);
        //Memory metrics
        sortedAggregations.put(MEMORY_USAGE_AVERAGE, memoryUsageAverage);
        sortedAggregations.put(MEMORY_MAX_USAGE_AVERAGE, memoryMaxUsageAverage);
        sortedAggregations.put(MEMORY_FAILCNT_AVERAGE, memoryFailcntAverage);
        sortedAggregations.put(MEMORY_LIMIT_AVERAGE, memoryLimitAverage);
        //Network metrics
        if(ENABLE_NETWORK_STATS) {
            sortedAggregations.put(NET_RX_BYTES_AVERAGE, netRxBytesAverage);
            sortedAggregations.put(NET_RX_DROPPED_AVERAGE, netRxDroppedAverage);
            sortedAggregations.put(NET_RX_ERRORS_AVERAGE, netRxErrorsAverage);
            sortedAggregations.put(NET_RX_PACKETS_AVERAGE, netRxPacketsAverage);
            sortedAggregations.put(NET_TX_BYTES_AVERAGE, netTxBytesAverage);
            sortedAggregations.put(NET_TX_DROPPED_AVERAGE, netTxDroppedAverage);
            sortedAggregations.put(NET_TX_ERRORS_AVERAGE, netTxErrorsAverage);
            sortedAggregations.put(NET_TX_PACKETS_AVERAGE, netTxPacketsAverage);
        }
        return sortedAggregations;
    }

    /**
     * Name of the node in the dynamics
     *
     * @return Name of the node
     */
    public abstract String getName();

    /**
     * Aggregates every metric of each node in one single map
     *
     * @return Map with a key for each metric
     */
    public static LinkedHashMap<String, Object> getStatsForNodeStateCollection(List<NodeState> collection) {
        LinkedHashMap<String, Object> sortedAggregations = new LinkedHashMap<>();
        for (NodeState ss : collection.stream().sorted(Comparator.comparing(NodeState::getName)).
                collect(Collectors.toList())) {
            sortedAggregations.put(SERVER_PREFIX + ss.getName(), ss.getStats());
        }
        return sortedAggregations;
    }

    public NodeState(DockerClient hostController, long monitorPeriod, TimeUnit monitorPeriodUnit) {
        this.hostController = hostController;
        this.monitorPeriod = monitorPeriod;
        this.monitorPeriodUnit = monitorPeriodUnit;
    }
}
