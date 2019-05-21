package org.apache.hadoop.hbase.master.balancer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHeteroneousBalancer {
	private static final Logger LOG = LoggerFactory.getLogger(BalancerTestBase.class);
	private static final String DEFAULT_RULES_TMP_LOCATION = "/tmp/hbase-balancer.rules";
	private static HeteroneousBalancer loadBalancer;
	private static Configuration conf;
	private int regionId = 0;
	private int serverId = 1;
	private static final ServerName master = ServerName.valueOf("fake-master", 0, 1L);

	@BeforeClass
	public static void beforeAllTests() throws Exception {
		createSimpleRulesFile(new ArrayList<>());
		conf = HBaseConfiguration.create();
		loadBalancer = new HeteroneousBalancer();
		loadBalancer.initialize();
		RegionLocationFinder regionFinder = new RegionLocationFinder();
		regionFinder.setConf(conf);

		MasterServices st = Mockito.mock(MasterServices.class);
		Mockito.when(st.getServerName()).thenReturn(master);
		loadBalancer.setMasterServices(st);

		conf.set(HeteroneousBalancer.HBASE_MASTER_BALANCER_HETERONEOUS_RULES_FILE, DEFAULT_RULES_TMP_LOCATION);
		loadBalancer.setConf(conf);
	}

	@AfterClass
	public static void afterAllTests() throws Exception {
		cleanup();
	}

	private static void cleanup() {
		File file = new File(DEFAULT_RULES_TMP_LOCATION);
		file.delete();
	}

	private static void createSimpleRulesFile(List<String> lines) throws IOException {
		cleanup();
		Path file = Paths.get(DEFAULT_RULES_TMP_LOCATION);
		Files.write(file, lines, Charset.forName("UTF-8"));
	}

	@Test
	public void test1() throws IOException {
		createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "srv[3-5] 120"));

		// mock cluster State
		Map<ServerName, List<RegionInfo>> clusterState = new HashMap<ServerName, List<RegionInfo>>();
		ServerName serverA = randomServer(3).getServerName();
		ServerName serverB = randomServer(3).getServerName();
		ServerName serverC = randomServer(3).getServerName();
		List<RegionInfo> regionsOnServerA = randomRegions(20);
		List<RegionInfo> regionsOnServerB = randomRegions(20);
		List<RegionInfo> regionsOnServerC = randomRegions(50);
		clusterState.put(serverA, regionsOnServerA);
		clusterState.put(serverB, regionsOnServerB);
		clusterState.put(serverC, regionsOnServerC);
		// mock ClusterMetrics
		Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
		serverMetricsMap.put(serverA, mockServerMetricsWithCpRequests(serverA, regionsOnServerA, 0));
		serverMetricsMap.put(serverB, mockServerMetricsWithCpRequests(serverB, regionsOnServerB, 0));
		serverMetricsMap.put(serverC, mockServerMetricsWithCpRequests(serverC, regionsOnServerC, 0));
		ClusterMetrics clusterStatus = mock(ClusterMetrics.class);
		when(clusterStatus.getLiveServerMetrics()).thenReturn(serverMetricsMap);
		loadBalancer.setClusterMetrics(clusterStatus);

		Map<TableName, Map<ServerName, List<RegionInfo>>> c = mockClusterServersWithTables(clusterState);

		loadBalancer.setClusterLoad(c);

		List<RegionPlan> plans = loadBalancer.balanceCluster(clusterState);
		System.out.println("moving " + plans.size() + " regions");
		for (RegionPlan regionPlan: plans) {
			System.out.println("moving " + regionPlan.getRegionName() + " from " + regionPlan.getSource().getHostnameLowerCase() + " to " + regionPlan.getDestination().getHostnameLowerCase());
		}
	}

	@Test
	public void test2() throws IOException {
		createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "srv[3-5] 120"));

		// mock cluster State
		Map<ServerName, List<RegionInfo>> clusterState = new HashMap<ServerName, List<RegionInfo>>();
		ServerName serverA = randomServer(3).getServerName();
		ServerName serverB = randomServer(3).getServerName();
		ServerName serverC = randomServer(3).getServerName();
		List<RegionInfo> regionsOnServerA = randomRegions(50);
		List<RegionInfo> regionsOnServerB = randomRegions(0);
		List<RegionInfo> regionsOnServerC = randomRegions(0);
		clusterState.put(serverA, regionsOnServerA);
		clusterState.put(serverB, regionsOnServerB);
		clusterState.put(serverC, regionsOnServerC);
		// mock ClusterMetrics
		Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
		serverMetricsMap.put(serverA, mockServerMetricsWithCpRequests(serverA, regionsOnServerA, 0));
		serverMetricsMap.put(serverB, mockServerMetricsWithCpRequests(serverB, regionsOnServerB, 0));
		serverMetricsMap.put(serverC, mockServerMetricsWithCpRequests(serverC, regionsOnServerC, 0));
		ClusterMetrics clusterStatus = mock(ClusterMetrics.class);
		when(clusterStatus.getLiveServerMetrics()).thenReturn(serverMetricsMap);
		loadBalancer.setClusterMetrics(clusterStatus);

		Map<TableName, Map<ServerName, List<RegionInfo>>> c = mockClusterServersWithTables(clusterState);

		loadBalancer.setClusterLoad(c);

		List<RegionPlan> plans = loadBalancer.balanceCluster(clusterState);
		System.out.println("Result: moving " + plans.size() + " regions");
		for (RegionPlan regionPlan: plans) {
			System.out.println("moving " + regionPlan.getRegionName() + " from " + regionPlan.getSource().getHostnameLowerCase() + " to " + regionPlan.getDestination().getHostnameLowerCase());
		}
	}

	protected HashMap<TableName, Map<ServerName, List<RegionInfo>>> mockClusterServersWithTables(Map<ServerName, List<RegionInfo>> clusterServers) {
		HashMap<TableName, Map<ServerName, List<RegionInfo>>> result = new HashMap<>();
		for (Map.Entry<ServerName, List<RegionInfo>> entry : clusterServers.entrySet()) {
			ServerName sal = entry.getKey();
			List<RegionInfo> regions = entry.getValue();
			for (RegionInfo hri : regions){
				Map<ServerName, List<RegionInfo>> servers = result
						.computeIfAbsent(hri.getTable(), k -> new HashMap<>());
				List<RegionInfo> hrilist = servers.computeIfAbsent(sal, k -> new ArrayList<>());
				hrilist.add(hri);
			}
		}
		for(Entry<TableName, Map<ServerName, List<RegionInfo>>> entry : result.entrySet()){
			for(ServerName srn : clusterServers.keySet()){
				if (!entry.getValue().containsKey(srn)) entry.getValue().put(srn, new ArrayList<>());
			}
		}
		return result;
	}

	// TODO: useless
	private ServerMetrics mockServerMetricsWithCpRequests(ServerName server,
			List<RegionInfo> regionsOnServer, long cpRequestCount) {
		ServerMetrics serverMetrics = mock(ServerMetrics.class);
		Map<byte[], RegionMetrics> regionLoadMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
		for(RegionInfo info : regionsOnServer){
			RegionMetrics rl = mock(RegionMetrics.class);
			when(rl.getReadRequestCount()).thenReturn(0L);
			when(rl.getWriteRequestCount()).thenReturn(0L);
			when(rl.getMemStoreSize()).thenReturn(Size.ZERO);
			when(rl.getStoreFileSize()).thenReturn(Size.ZERO);
			regionLoadMap.put(info.getRegionName(), rl);
		}
		when(serverMetrics.getRegionMetrics()).thenReturn(regionLoadMap);
		return serverMetrics;
	}

	private Queue<ServerName> serverQueue = new LinkedList<>();

	protected ServerAndLoad randomServer(final int numRegionsPerServer) {
		if (!this.serverQueue.isEmpty()) {
			ServerName sn = this.serverQueue.poll();
			return new ServerAndLoad(sn, numRegionsPerServer);
		}
		Random rand = ThreadLocalRandom.current();
		String host = "srv" + serverId++;
		int port = rand.nextInt(60000);
		long startCode = rand.nextLong();
		ServerName sn = ServerName.valueOf(host, port, startCode);
		return new ServerAndLoad(sn, numRegionsPerServer);
	}

	private Queue<RegionInfo> regionQueue = new LinkedList<>();

	protected List<RegionInfo> randomRegions(int numRegions) {
		return randomRegions(numRegions, -1);
	}

	protected List<RegionInfo> randomRegions(int numRegions, int numTables) {
		List<RegionInfo> regions = new ArrayList<>(numRegions);
		byte[] start = new byte[16];
		byte[] end = new byte[16];
		Random rand = ThreadLocalRandom.current();
		rand.nextBytes(start);
		rand.nextBytes(end);
		for (int i = 0; i < numRegions; i++) {
			if (!regionQueue.isEmpty()) {
				regions.add(regionQueue.poll());
				continue;
			}
			Bytes.putInt(start, 0, numRegions << 1);
			Bytes.putInt(end, 0, (numRegions << 1) + 1);
			//TableName tableName = TableName.valueOf("table" + (numTables > 0 ? rand.nextInt(numTables) : i));
			TableName tableName = TableName.valueOf("table1");
			RegionInfo hri = RegionInfoBuilder.newBuilder(tableName)
					.setStartKey(start)
					.setEndKey(end)
					.setSplit(false)
					.setRegionId(regionId++)
					.build();
			regions.add(hri);
		}
		return regions;
	}

}
