package org.corfudb.universe.dynamic;

import com.spotify.docker.client.exceptions.DockerCertificateException;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.LongevityListener;
import org.corfudb.universe.dynamic.events.*;
import org.corfudb.universe.dynamic.rules.UniverseRule;
import org.corfudb.universe.dynamic.state.*;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.util.Sleep;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.universe.dynamic.state.State.*;

/**
 * Simulates the behavior of a corfu cluster and its clients during a period of time given.
 * General speaking, any dynamical system (including finite state machines) can be approximated with:
 * - Events
 * - Operator that combine events to produce complex events
 * - Rules over the events and its combinations
 * - State that describe the system as whole
 * - Dynamic that generate the events (or combinations of them), ensure the rules and
 * produce transitions over the state based in the events.
 * This is the approach follow here, the method {@link Dynamic::run} implements a event loop with
 * logic described above.
 *
 * Created by edilmo on 11/06/18.
 */
@Slf4j
public abstract class Dynamic extends UniverseInitializer {

    /**
     * Minimum amount of nodes that must be up in each iteration of the dynamic
     */
    public static final int MINIMUM_NODES_UP = 1;

    /**
     * Default period for the system metrics monitor
     */
    public static final long DEFAULT_MONITORING_PERIOD = 1000;

    /**
     * Default time units of the period for the system metrics monitor
     */
    public static final TimeUnit DEFAULT_TIME_UNITS = TimeUnit.MILLISECONDS;

    /**
     * Amount of corfu tables handle for each client in the universe.
     */
    public static final int DEFAULT_AMOUNT_OF_CORFU_CLIENTS = 3;

    /**
     * Amount of corfu servers in the universe.
     */
    private static final int DEFAULT_AMOUNT_OF_CORFU_SERVERS = 3;

    /**
     * Random number generator used for the creation of
     * random intervals of time and generation of events.
     */
    protected final Random randomNumberGenerator = new Random(210580);

    /**
     * Name of the CSV file where all details are stored.
     */
    private Path outputFilePath;

    /**
     * The cluster of corfu nodes to use.
     */
    protected CorfuCluster corfuCluster;

    /**
     * Amount of nodes defined in the cluster.
     */
    protected int numNodes;

    /**
     * Fixture of parameters used in the creation of clients.
     */
    protected ClientParams clientFixture;

    /**
     * The max amount of milli seconds that the dynamic can live.
     */
    protected final long longevity;

    /**
     * Indicates whether save the result of iterations made before a listener is registered
     */
    protected boolean waitForListener = false;

    /**
     * List of results of iterations made.
     * It's used only before a listener is registered.
     */
    protected Map<Long, List<String>> savedIterations = new HashMap<>();

    /**
     * Clients used during the life of the dynamic.
     * <p>
     * Clients are created during the initialization and keep it fixed up to the end.
     */
    protected final List<CorfuClientInstance> corfuClients = new ArrayList<>();

    /**
     * Corfu nodes present in the cluster.
     */
    protected final LinkedHashMap<String, CorfuServer> corfuServers = new LinkedHashMap<>();

    /**
     * Represents the expected state of the universe in a point of time.
     */
    protected State desireState = new State();

    /**
     * Represents the real state of the universe in a point of time.
     */
    protected final State realState = new State();

    /**
     * The amount correctness errors seen
     */
    protected long correctnessErrors = 0;

    /**
     * The amount liveness errors seen
     */
    protected long livenessErrors = 0;

    /**
     * The time when the test starts
     */
    protected long startTime = System.currentTimeMillis();

    /**
     * List of object to call back with the report of each iteration of the dynamics
     */
    private List<LongevityListener> listeners = new ArrayList<LongevityListener>();

    private List<String> reportColumns = new ArrayList<String>();

    public void registerListener(LongevityListener listener) {
        this.listeners.add(listener);
        List<List<String>> data = this.savedIterations.values().stream().collect(Collectors.toList());
        listener.setReportColumns(this.reportColumns, data);
        this.savedIterations.clear();
        this.waitForListener = false;
    }

    public void unregisterListener(LongevityListener listener){
        this.listeners.removeIf(e -> e.getId().equals(listener.getId()));
    }

    /**
     * Time in milliseconds before update the state.
     */
    protected abstract long getDelayForStateUpdate();

    /**
     * Time in milliseconds between the composite event that just happened and the next one.
     */
    protected abstract long getIntervalBetweenEvents();

    /**
     * Generate a composite event to materialize in the universe.
     * The events generated have not guaranty of execution. They are allowed
     * to execute if they are compliant with the {@link UniverseRule}.
     *
     * @return Composite event generated.
     */
    protected abstract UniverseEventOperator generateCompositeEvent();

    /**
     * Materialize the execution of a composite event if it is compliant with the {@link UniverseRule}.
     *
     * @param compositeEvent Composite event to materialize.
     * @return Whether the composite event was materialized or not.
     */
    private boolean materializeCompositeEvent(UniverseEventOperator compositeEvent) {
        try {
            //Check the pre rules that any composite event must be compliant with
            boolean isPreRulesCompliant = true;
            for (UniverseRule preRule : UniverseRule.getPreCompositeEventRules()) {
                if (!preRule.check(compositeEvent, this.desireState, this.realState)) {
                    isPreRulesCompliant = false;
                    break;
                }
            }
            if (!isPreRulesCompliant)
                return false;
            //The next desire state is based in changes over the current real state
            State desireStateCopy = (State) this.realState.clone();
            compositeEvent.applyDesirePartialTransition(desireStateCopy);
            //Check the post rules that any composite event must be compliant with
            boolean isPostRulesCompliant = true;
            for (UniverseRule postRule : UniverseRule.getPostCompositeEventRules()) {
                if (!postRule.check(compositeEvent, desireStateCopy, this.realState)) {
                    isPostRulesCompliant = false;
                    break;
                }
            }
            if (!isPostRulesCompliant)
                return false;
            //Make the desire state equal to the computed desired state
            this.desireState = desireStateCopy;
            //StartServerEvent the system metrics monitor for each responsive server in the desire state
            for (String serverName : this.desireState.getResponsiveServers()) {
                this.realState.getServers().get(serverName).startMetricsRecording();
            }
            //Run the composite event
            compositeEvent.executeRealPartialTransition(this.realState);
        }catch (Exception ex){
            log.error(String.format("Error materializing event: %s", compositeEvent.getObservationDescription()), ex);
        }
        finally {
            //StopServerEvent the monitoring of the system metrics
            for (String serverName : this.desireState.getResponsiveServers()) {
                this.realState.getServers().get(serverName).stopMetricsRecording();
            }
            //update the real state
            this.updateRealState();
        }
        return true;
    }

    /**
     * Updates the real state using the tools provided by the universe.
     */
    private void updateRealState() {
        try {
            Sleep.MILLISECONDS.sleepUninterruptibly(this.getDelayForStateUpdate());
        } catch (Exception ex) {
            log.error("Error invoking Thread.sleep.", ex);
        }
        try {
            for (CorfuClientInstance cci : this.corfuClients) {
                //Update all client related fields
                ClientState clientState = this.realState.getClients().get(cci.getName());
                Layout layout = cci.getCorfuClient().getLayout();
                ClusterStatusReport clusterStatusReport = cci.getCorfuClient().
                        getManagementView().getClusterStatus();
                clientState.setClusterStatus(clusterStatusReport.getClusterStatus());
                Map<String, ClusterStatusReport.NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();
                for (ClientServerState clientServerState : clientState.getServers().values()) {
                    String endpoint = clientServerState.getNodeEndpoint();
                    if (statusMap.containsKey(endpoint)) {
                        clientServerState.setStatus(statusMap.get(endpoint));
                        clientServerState.setLayoutServer(layout.getLayoutServers().contains(endpoint));
                        clientServerState.setLogUnitServer(layout.getSegments().stream()
                                .flatMap(seg -> seg.getAllLogServers().stream())
                                .anyMatch(s -> s.equals(endpoint)));
                        clientServerState.setPrimarySequencer(layout.getPrimarySequencer().equals(endpoint));
                    } else {
                        log.error(String.format("Expected server not found in cluster status report. Node Name: %s - Node Endpoint: %s",
                                clientServerState.getServerName(), endpoint));
                    }
                }
            }
        }catch (Exception ex){
            log.error("Error updating real state.", ex);
        }
    }

    public static final String REPORT_FIELD_EVENT = "Event";
    public static final String REPORT_FIELD_DISCARDED_EVENTS = "Discarded Events";
    public static final String REPORT_REAL_STATE_PREFIX = "Real";
    public static final String REPORT_DESIRE_STATE_PREFIX = "Desire";

    /**
     * Report the the desire state and real state after each iteration of the event generation loop.
     *
     * @param compositeEvent Composite event that change the states.
     */
    private void reportStateDifference(UniverseEventOperator compositeEvent, int discardedEvents, boolean writeColumns) {
        long realTablesIncorrect = 0;
        long desireTablesIncorrect = 0;
        long realUnavailableClusterStatus = 0;
        long desireUnavailableClusterStatus = 0;
        LinkedHashMap<String, String> realStatePrintableFlatStats = Stats.getPrintableFlatStats(REPORT_REAL_STATE_PREFIX,
                this.realState);
        LinkedHashMap<String, String> desireStatePrintableFlatStats = Stats.getPrintableFlatStats(REPORT_DESIRE_STATE_PREFIX,
                this.desireState);
        LinkedHashMap<String, String> report = new LinkedHashMap<>();
        long elapsedTime = System.currentTimeMillis() - this.startTime;
        report.put(Stats.ELAPSED_TIME, Long.toString(elapsedTime));
        String eventDescription = compositeEvent.getObservationDescription();
        log.info(eventDescription);
        report.put(REPORT_FIELD_EVENT, eventDescription);
        log.info("Real state summary:");
        for (Map.Entry<String, String> entry : realStatePrintableFlatStats.entrySet()) {
            boolean show = Stats.SHOW_LIST.stream().anyMatch(s -> entry.getKey().endsWith(s));
            if(show)
                log.info(String.format("%s: %s", entry.getKey(), entry.getValue()));
            if(entry.getKey().endsWith(INCORRECT_TABLES))
                realTablesIncorrect = Long.parseLong(entry.getValue());
            else if(entry.getKey().endsWith(UNAVAILABLE_CLUSTER_STATUS))
                realUnavailableClusterStatus = Long.parseLong(entry.getValue());
            report.put(entry.getKey(), entry.getValue());
        }
        log.info("Desire state summary:");
        for (Map.Entry<String, String> entry : desireStatePrintableFlatStats.entrySet()) {
            boolean show = Stats.SHOW_LIST.stream().anyMatch(s -> entry.getKey().endsWith(s));
            String statNameInRealState = entry.getKey().replaceFirst(REPORT_DESIRE_STATE_PREFIX, REPORT_REAL_STATE_PREFIX);
            if(show && realStatePrintableFlatStats.containsKey(statNameInRealState)){
                String statName = entry.getKey().substring(entry.getKey().lastIndexOf(Stats.SEPARATOR));
                String valueInRealState = realStatePrintableFlatStats.get(statNameInRealState);
                String valueComparison = Stats.compareStats(statName, valueInRealState, entry.getValue());
                log.info(String.format("Deviation for %s: %s", statName, valueComparison));
            }
            if(entry.getKey().endsWith(INCORRECT_TABLES))
                desireTablesIncorrect = Long.parseLong(entry.getValue());
            else if(entry.getKey().endsWith(UNAVAILABLE_CLUSTER_STATUS))
                desireUnavailableClusterStatus = Long.parseLong(entry.getValue());
            report.put(entry.getKey(), entry.getValue());
        }
        if(discardedEvents > 0)
            log.info(String.format("%s: %s", REPORT_FIELD_DISCARDED_EVENTS, discardedEvents));
        report.put(REPORT_FIELD_DISCARDED_EVENTS, Integer.toString(discardedEvents));
        report.put(Stats.SERVERS_TOTAL, Integer.toString(corfuServers.size()));
        report.put(Stats.CLIENTS_TOTAL, Integer.toString(corfuClients.size()));
        report.put(Stats.TABLES_TOTAL, Integer.toString(realState.getData().size()));
        this.correctnessErrors += Math.abs(realTablesIncorrect - desireTablesIncorrect);
        this.livenessErrors += Math.abs(realUnavailableClusterStatus - desireUnavailableClusterStatus);
        String elapsedTimeStr = "";
        long elapsedDays = TimeUnit.MILLISECONDS.toDays(elapsedTime);
        if(elapsedDays > 0)
            elapsedTimeStr = Long.toString(elapsedDays) + " days ";
        long elapsedHours = TimeUnit.MILLISECONDS.toHours(elapsedTime) - TimeUnit.DAYS.toHours(elapsedDays);
        if(elapsedHours > 0 || !elapsedTimeStr.isEmpty())
            elapsedTimeStr = elapsedTimeStr + Long.toString(elapsedHours) + " hours ";
        long elapsedMinutes = TimeUnit.MILLISECONDS.toMinutes(elapsedTime) - TimeUnit.DAYS.toMinutes(elapsedDays) -
                TimeUnit.HOURS.toMinutes(elapsedHours);
        if(elapsedMinutes > 0 || !elapsedTimeStr.isEmpty())
            elapsedTimeStr = elapsedTimeStr + Long.toString(elapsedMinutes) + " minutes ";
        long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime) - TimeUnit.DAYS.toSeconds(elapsedDays) -
                TimeUnit.HOURS.toSeconds(elapsedHours) - TimeUnit.MINUTES.toSeconds(elapsedMinutes);
        if(elapsedSeconds > 0 || !elapsedTimeStr.isEmpty())
            elapsedTimeStr = elapsedTimeStr + Long.toString(elapsedSeconds) + " seconds ";
        log.info(String.format("Time elapsed: %s", elapsedTimeStr));
        log.info(String.format("Correctness errors: %d", this.correctnessErrors));
        log.info(String.format("Liveness errors: %d", this.livenessErrors));
        WriteReportToSheet(report, writeColumns);
        if(writeColumns){
            this.reportColumns = report.keySet().stream().collect(Collectors.toList());
            this.listeners.forEach(l -> {
                try{
                    List<List<String>> data = this.savedIterations.values().stream().collect(Collectors.toList());
                    l.setReportColumns(this.reportColumns, data);
                }
                catch (Exception ex){
                    log.error("Error setting report columns to listener", ex);
                }
            });
        }
        List<String> reportValues = report.values().stream().collect(Collectors.toList());
        this.listeners.forEach(l -> {
            try{
                l.reportIteration(reportValues);
            }
            catch (Exception ex){
                log.error("Error reporting iteration to listener", ex);
            }
        });
        if(this.waitForListener && this.listeners.isEmpty()){
            this.savedIterations.put(elapsedTime, reportValues);
        }
    }

    private void WriteReportToSheet(LinkedHashMap<String, String> report, boolean writeColumns) {
        List<String> linesToFile = new ArrayList<>();
        if(writeColumns){
            String header = report.keySet().stream().collect(Collectors.joining(","));
            linesToFile.add(header);
        }
        String row = report.values().stream().collect(Collectors.joining(","));
        linesToFile.add(row);
        try{
            Files.write(outputFilePath, linesToFile, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }catch (Exception ex){
            log.error("Error writing row to output file.", ex);
        }
    }

    /**
     * Run the dynamic defined during the time specified, using a event loop.
     */
    public void run() {
        //check if output file exist
        String outputFilename = "longevity-" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss"));
        outputFilePath = Paths.get(outputFilename + ".csv");
        int counter = 0;
        while(Files.exists(outputFilePath)){
            counter++;
            outputFilePath = Paths.get(outputFilename + counter + ".csv");
        }
        Fixtures.AbstractUniverseFixture fixture = getFixture(DEFAULT_AMOUNT_OF_CORFU_SERVERS);
        boolean firstIteration = true;
        this.numNodes = fixture.getNumNodes();
        this.clientFixture = fixture.getClient();
        this.corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

        for (Node node: corfuCluster.nodes().values()) {
            CorfuServer nodeServer = (CorfuServer) node;
            String nodeEndpoint = nodeServer.getEndpoint();
            String nodeServerHostName = nodeServer.getParams().getName();
            this.corfuServers.put(nodeServerHostName, nodeServer);
            //Update desire state
            ServerState desireServerState =
                    new ServerState(nodeServerHostName, nodeEndpoint, docker, DEFAULT_MONITORING_PERIOD,
                            DEFAULT_TIME_UNITS);
            this.desireState.getServers().put(nodeServerHostName, desireServerState);
            //Update desire state
            ServerState realServerState =
                    new ServerState(nodeServerHostName, nodeEndpoint, docker, DEFAULT_MONITORING_PERIOD,
                            DEFAULT_TIME_UNITS);
            this.realState.getServers().put(nodeServerHostName, realServerState);
        }
        for (int i = 0; i < DEFAULT_AMOUNT_OF_CORFU_CLIENTS; i++) {
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            String clientName = corfuClient.getParams().getName() + i;
            CorfuClientInstance corfuClientInstance = new CorfuClientInstance(i, clientName, corfuClient);
            this.corfuClients.add(corfuClientInstance);
            ClientState desireClientState = new ClientState(clientName, clientName, docker,
                    DEFAULT_MONITORING_PERIOD, DEFAULT_TIME_UNITS);
            this.desireState.getClients().put(clientName, desireClientState);
            for(ServerState ss: this.desireState.getServers().values()){
                ClientServerState clientServerState = new ClientServerState(ss.getName(), ss.getNodeEndpoint());
                desireClientState.getServers().put(ss.getName(), clientServerState);
            }
            ClientState realClientState = new ClientState(clientName, clientName, docker,
                    DEFAULT_MONITORING_PERIOD, DEFAULT_TIME_UNITS);
            this.realState.getClients().put(clientName, realClientState);
            for(ServerState ss: this.realState.getServers().values()){
                ClientServerState clientServerState = new ClientServerState(ss.getName(), ss.getNodeEndpoint());
                realClientState.getServers().put(ss.getName(), clientServerState);
            }
            for (CorfuTableDataGenerationFunction corfuTableDataGenerator : corfuClientInstance.getCorfuTables()) {
                TableState desireTableState =
                        new TableState(corfuTableDataGenerator.getTableStreamName());
                this.desireState.getData().put(desireTableState.getTableStreamName(), desireTableState);
                ClientTableAccessStats desireClientTableAccessStats =
                        new ClientTableAccessStats(corfuTableDataGenerator.getTableStreamName());
                desireClientState.getTableAccessStats().put(desireClientTableAccessStats.getTableStreamName(),
                        desireClientTableAccessStats);
                TableState realTableState =
                        new TableState(corfuTableDataGenerator.getTableStreamName());
                this.realState.getData().put(desireTableState.getTableStreamName(), realTableState);
                ClientTableAccessStats realClientTableAccessStats =
                        new ClientTableAccessStats(corfuTableDataGenerator.getTableStreamName());
                realClientState.getTableAccessStats().put(realClientTableAccessStats.getTableStreamName(),
                        realClientTableAccessStats);
            }
        }
        int discardedEvents = 0;
        this.startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - this.startTime) < this.longevity) {
            UniverseEventOperator compositeEvent = this.generateCompositeEvent();
            if (this.materializeCompositeEvent(compositeEvent)) {
                this.reportStateDifference(compositeEvent, discardedEvents, firstIteration);
                firstIteration = false;
                discardedEvents = 0;
                try {
                    Sleep.MILLISECONDS.sleepUninterruptibly(this.getIntervalBetweenEvents());
                } catch (Exception ex) {
                    log.error("Error invoking Thread.sleep.", ex);
                }
            } else {
                log.info(String.format("Discarding event: %s", compositeEvent.getObservationDescription()));
                discardedEvents++;
            }
        }
        if(discardedEvents != 0){
            this.reportStateDifference(new UniverseEventOperator.TestEndWithDiscardedEvents(discardedEvents),
                    discardedEvents, firstIteration);
        }
        this.listeners.forEach(l -> {
            try{
                l.finish();
            }
            catch (Exception ex){
                log.error("Error reporting end of test to listener", ex);
            }
        });
    }

    /**
     * Initialize the universe and the dynamic.
     *
     * @throws DockerCertificateException
     */
    @Override
    public void initialize() throws DockerCertificateException {
        super.initialize();
    }

    /**
     * Free resources used by the universe and the dynamic.
     */
    @Override
    public void shutdown() {
        super.shutdown();
    }

    public Dynamic(long longevity, boolean waitForListener) {
        super("LongevityTest");
        this.longevity = longevity;
        this.waitForListener = waitForListener;
    }
}
