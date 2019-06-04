package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Class used to be the base of unit tests on Heterogeneous load balancers. It contains methods to
 * generate rules, region-servers, regions, and so on
 */
public class HeterogeneousTestBase {
    protected static Random rand = new Random();

    private static final String DEFAULT_RULES_TMP_LOCATION = "/tmp/hbase-balancer.rules";
    static HeterogeneousBalancer loadBalancer;
    static Configuration conf;
    private int regionId = 0;
    private static final ServerName master = ServerName.valueOf("fake-master", 0, 1L);

    @BeforeClass
    public static void beforeAllTests() throws Exception {
        createSimpleRulesFile(new ArrayList<String>());
        conf = HBaseConfiguration.create();
        loadBalancer = new HeterogeneousBalancer();
        conf.set(HeterogeneousBalancer.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE, DEFAULT_RULES_TMP_LOCATION);
        loadBalancer.setConf(conf);
        loadBalancer.initialize();
        RegionLocationFinder regionFinder = new RegionLocationFinder();
        regionFinder.setConf(conf);

        MasterServices st = Mockito.mock(MasterServices.class);
        Mockito.when(st.getServerName()).thenReturn(master);
        loadBalancer.setMasterServices(st);

    }

    Map<ServerName, List<HRegionInfo>> createHomongousClusterState(int nbrServers, int nbrRegionsPerRS) {
        Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();

        for (int i = 0; i < nbrServers; i++) {
            ServerName server = randomServer("srv" + i).getServerName();
            List<HRegionInfo> regionsOnServer = randomRegions(nbrRegionsPerRS);
            clusterState.put(server, regionsOnServer);
        }

        return clusterState;
    }

    static void createSimpleRulesFile(List<String> lines) throws IOException {
        cleanup();
        Path file = Paths.get(DEFAULT_RULES_TMP_LOCATION);
        Files.write(file, lines, Charset.forName("UTF-8"));
    }

    @AfterClass
    public static void afterAllTests() throws Exception {
        cleanup();
    }

    protected static void cleanup() {
        File file = new File(DEFAULT_RULES_TMP_LOCATION);
        file.delete();
    }

    private Map<ServerName, ServerLoad> serverMetricsMapFromClusterState(
            Map<ServerName, List<HRegionInfo>> clusterState) {
        final Map<ServerName, ServerLoad> serverMetricsMap = new TreeMap<>();

        for (Map.Entry<ServerName, List<HRegionInfo>> entry: clusterState.entrySet()) {
            serverMetricsMap.put(entry.getKey(), mockServerLoad(entry.getKey(), entry.getValue()));
        }
        return serverMetricsMap;
    }

    void testBalance(Map<ServerName, List<HRegionInfo>> clusterState, int expected) throws IOException {

        final Map<ServerName, ServerLoad> serverMetricsMap = serverMetricsMapFromClusterState(clusterState);

        ClusterStatus clusterStatus = mock(ClusterStatus.class);
        when(clusterStatus.getServers()).thenReturn(clusterState.keySet());
        when(clusterStatus.getLoad(Mockito.any(ServerName.class)))
                .thenAnswer(new Answer<ServerLoad>() {
                    @Override
                    public ServerLoad answer(InvocationOnMock invocation) throws Throwable {
                        return serverMetricsMap.get(invocation.getArguments()[0]);
                    }
                });

        loadBalancer.setClusterStatus(clusterStatus);
        List<RegionPlan> plans = loadBalancer.balanceCluster(clusterState);
        System.out.println("moving " + plans.size() + " regions");
        Assert.assertEquals(expected, plans.size());
    }

    private ServerLoad mockServerLoad(ServerName server,
                                      List<HRegionInfo> regionsOnServer) {
        ServerLoad serverMetrics = mock(ServerLoad.class);
        Map<byte[], RegionLoad> regionLoadMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        for(HRegionInfo info : regionsOnServer){
            RegionLoad rl = mock(RegionLoad.class);
            when(rl.getReadRequestsCount()).thenReturn(0L);
            when(rl.getWriteRequestsCount()).thenReturn(0L);
            when(rl.getMemStoreSizeMB()).thenReturn(0);
            when(rl.getStorefileSizeMB()).thenReturn(0);
            regionLoadMap.put(info.getRegionName(), rl);
        }
        when(serverMetrics.getRegionsLoad()).thenReturn(regionLoadMap);
        return serverMetrics;
    }


    private Queue<ServerName> serverQueue = new LinkedList<ServerName>();

    ServerAndLoad randomServer(final String host) {
        if (!this.serverQueue.isEmpty()) {
            ServerName sn = this.serverQueue.poll();
            return new ServerAndLoad(sn, 0);
        }
        int port = rand.nextInt(60000);
        long startCode = rand.nextLong();
        ServerName sn = ServerName.valueOf(host, port, startCode);
        return new ServerAndLoad(sn, 0);
    }

    private Queue<HRegionInfo> regionQueue = new LinkedList<HRegionInfo>();

    List<HRegionInfo> randomRegions(int numRegions) {
        return randomRegions(numRegions, -1);
    }

    private List<HRegionInfo> randomRegions(int numRegions, int numTables) {
        List<HRegionInfo> regions = new ArrayList<HRegionInfo>(numRegions);
        byte[] start = new byte[16];
        byte[] end = new byte[16];
        rand.nextBytes(start);
        rand.nextBytes(end);
        for (int i = 0; i < numRegions; i++) {
            if (!regionQueue.isEmpty()) {
                regions.add(regionQueue.poll());
                continue;
            }
            Bytes.putInt(start, 0, numRegions << 1);
            Bytes.putInt(end, 0, (numRegions << 1) + 1);
            TableName tableName =
                    TableName.valueOf("table" + (numTables > 0 ? rand.nextInt(numTables) : i));
            HRegionInfo hri = new HRegionInfo(tableName, start, end, false, regionId++);
            regions.add(hri);
        }
        return regions;
    }

    protected List<ServerAndLoad> randomServers(int numServers, int numRegionsPerServer) {
        List<ServerAndLoad> servers = new ArrayList<ServerAndLoad>(numServers);
        for (int i = 0; i < numServers; i++) {
            servers.add(randomServer("srv" + i, numRegionsPerServer));
        }
        return servers;
    }

    protected ServerAndLoad randomServer(final String host, final int numRegionsPerServer) {
        if (!this.serverQueue.isEmpty()) {
            ServerName sn = this.serverQueue.poll();
            return new ServerAndLoad(sn, numRegionsPerServer);
        }
        int port = rand.nextInt(60000);
        long startCode = rand.nextLong();
        ServerName sn = ServerName.valueOf(host, port, startCode);
        return new ServerAndLoad(sn, numRegionsPerServer);
    }

    List<ServerName> getListOfServerNames(final List<ServerAndLoad> sals) {
        List<ServerName> list = new ArrayList<ServerName>();
        for (ServerAndLoad e : sals) {
            list.add(e.getServerName());
        }
        return list;
    }

    /**
     * Asserts a valid retained assignment plan.
     * <p>
     * Must meet the following conditions:
     * <ul>
     * <li>Every input region has an assignment, and to an online server
     * <li>If a region had an existing assignment to a server with the same
     * address a a currently online server, it will be assigned to it
     * </ul>
     * @param existing
     * @param servers
     * @param assignment
     */
    void assertRetainedAssignment(Map<HRegionInfo, ServerName> existing,
                                  List<ServerName> servers, Map<ServerName, List<HRegionInfo>> assignment) {
        // Verify condition 1, every region assigned, and to online server
        Set<ServerName> onlineServerSet = new TreeSet<ServerName>(servers);
        Set<HRegionInfo> assignedRegions = new TreeSet<HRegionInfo>();
        for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
            assertTrue("Region assigned to server that was not listed as online",
                    onlineServerSet.contains(a.getKey()));
            assignedRegions.addAll(a.getValue());
        }
        assertEquals(existing.size(), assignedRegions.size());

        // Verify condition 2, if server had existing assignment, must have same
        Set<String> onlineHostNames = new TreeSet<String>();
        for (ServerName s : servers) {
            onlineHostNames.add(s.getHostname());
        }

        for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
            ServerName assignedTo = a.getKey();
            for (HRegionInfo r : a.getValue()) {
                ServerName address = existing.get(r);
                if (address != null && onlineHostNames.contains(address.getHostname())) {
                    // this region was prevously assigned somewhere, and that
                    // host is still around, then it should be re-assigned on the
                    // same host
                    assertEquals(address.getHostname(), assignedTo.getHostname());
                }
            }
        }
    }
}
