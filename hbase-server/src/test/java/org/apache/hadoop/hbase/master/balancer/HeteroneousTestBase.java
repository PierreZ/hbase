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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Class used to be the base of unit tests on Heteroneous load balancers. It contains methods to
 * generate rules, region-servers, regions, and so on
 */
public class HeteroneousTestBase {
    protected static Random rand = new Random();

    private static final String DEFAULT_RULES_TMP_LOCATION = "/tmp/hbase-balancer.rules";
    static HeteroneousBalancer loadBalancer;
    static Configuration conf;
    private int regionId = 0;
    private static final ServerName master = ServerName.valueOf("fake-master", 0, 1L);

    @BeforeClass
    public static void beforeAllTests() throws Exception {
        createSimpleRulesFile(new ArrayList<String>());
        conf = HBaseConfiguration.create();
        loadBalancer = new HeteroneousBalancer();
        conf.set(HeteroneousBalancer.HBASE_MASTER_BALANCER_HETERONEOUS_RULES_FILE, DEFAULT_RULES_TMP_LOCATION);
        loadBalancer.setConf(conf);
        loadBalancer.initialize();
        RegionLocationFinder regionFinder = new RegionLocationFinder();
        regionFinder.setConf(conf);

        MasterServices st = Mockito.mock(MasterServices.class);
        Mockito.when(st.getServerName()).thenReturn(master);
        loadBalancer.setMasterServices(st);

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
}
