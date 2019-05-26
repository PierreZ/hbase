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
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestHeteroneousBalancer {

	protected static Random rand = new Random();

	private static final String DEFAULT_RULES_TMP_LOCATION = "/tmp/hbase-balancer.rules";
	private static HeteroneousBalancer loadBalancer;
	private static Configuration conf;
	private int regionId = 0;
	private int serverId = 1;
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

	private static void createSimpleRulesFile(List<String> lines) throws IOException {
		cleanup();
		Path file = Paths.get(DEFAULT_RULES_TMP_LOCATION);
		Files.write(file, lines, Charset.forName("UTF-8"));
	}

	@AfterClass
	public static void afterAllTests() throws Exception {
		cleanup();
	}

	private static void cleanup() {
		File file = new File(DEFAULT_RULES_TMP_LOCATION);
		file.delete();
	}

	@Test
	public void testTwoServersAndOneOverloaded() throws IOException {

		createSimpleRulesFile(Arrays.asList("srv1 3", "srv2 20"));
		Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
		ServerName serverA = randomServer("srv1").getServerName();
		ServerName serverB = randomServer("srv2").getServerName();
		List<HRegionInfo> regionsOnServerA = randomRegions(4);
		List<HRegionInfo> regionsOnServerB = randomRegions(9);
		clusterState.put(serverA, regionsOnServerA);
		clusterState.put(serverB, regionsOnServerB);

		testBalance(clusterState, 2);

	}

	@Test
	public void testThreeServersAndOneEmpty() throws IOException {
		createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "srv[3-5] 120"));

		// mock cluster State
		Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
		ServerName serverA = randomServer("srv1").getServerName();
		ServerName serverB = randomServer("srv2").getServerName();
		ServerName serverC = randomServer("srv3").getServerName();
		List<HRegionInfo> regionsOnServerA = randomRegions(10);
		List<HRegionInfo> regionsOnServerB = randomRegions(20);
		List<HRegionInfo> regionsOnServerC = randomRegions(50);
		clusterState.put(serverA, regionsOnServerA);
		clusterState.put(serverB, regionsOnServerB);
		clusterState.put(serverC, regionsOnServerC);

		testBalance(clusterState, 18);
	}

	@Test
	public void testTwoServersAndOneEmpty() throws IOException {
		createSimpleRulesFile(Arrays.asList("srv1 10", "srv2 10"));

		// mock cluster State
		Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
		ServerName serverA = randomServer("srv1").getServerName();
		ServerName serverB = randomServer("srv2").getServerName();
		List<HRegionInfo> regionsOnServerA = randomRegions(10);
		List<HRegionInfo> regionsOnServerB = randomRegions(0);
		clusterState.put(serverA, regionsOnServerA);
		clusterState.put(serverB, regionsOnServerB);

		testBalance(clusterState, 5);
	}

	@Test
	public void testOverloadedServers() throws IOException {
		createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "srv[3-5] 10"));

		// mock cluster State
		Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
		ServerName serverA = randomServer("srv1").getServerName();
		ServerName serverB = randomServer("srv2").getServerName();
		ServerName serverC = randomServer("srv3").getServerName();
		List<HRegionInfo> regionsOnServerA = randomRegions(20);
		List<HRegionInfo> regionsOnServerB = randomRegions(20);
		List<HRegionInfo> regionsOnServerC = randomRegions(50);
		clusterState.put(serverA, regionsOnServerA);
		clusterState.put(serverB, regionsOnServerB);
		clusterState.put(serverC, regionsOnServerC);

		testBalance(clusterState, 0);
	}
	@Test
	public void testSwapLoad() throws IOException {
		createSimpleRulesFile(Arrays.asList("srv1 100", "srv2 1000"));

		// mock cluster State
		Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
		ServerName serverA = randomServer("srv1").getServerName();
		ServerName serverB = randomServer("srv2").getServerName();
		List<HRegionInfo> regionsOnServerA = randomRegions(900);
		List<HRegionInfo> regionsOnServerB = randomRegions(5);
		clusterState.put(serverA, regionsOnServerA);
		clusterState.put(serverB, regionsOnServerB);

		testBalance(clusterState, 817);
	}

	@Test
	public void testSlightlyOverloaded() throws IOException {
		createSimpleRulesFile(Arrays.asList("srv1 100", "srv2 100"));

		// mock cluster State
		Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
		ServerName serverA = randomServer("srv1").getServerName();
		ServerName serverB = randomServer("srv2").getServerName();
		List<HRegionInfo> regionsOnServerA = randomRegions(102);
		List<HRegionInfo> regionsOnServerB = randomRegions(97);
		clusterState.put(serverA, regionsOnServerA);
		clusterState.put(serverB, regionsOnServerB);

		testBalance(clusterState, 2);
	}

	private void testBalance(Map<ServerName, List<HRegionInfo>> clusterState, int expected) throws IOException {

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
		//for (RegionPlan regionPlan: plans) {
		//System.out.println("moving " + regionPlan.getRegionName() + " from " + regionPlan.getSource().getHostname() + " to " + regionPlan.getDestination().getHostname());
		//}
		Assert.assertEquals(expected, plans.size());
	}


	@Test
	public void testStartup() throws IOException {
		createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "srv[3-5] 50"));
		List<HRegionInfo> regions = randomRegions(20);

		ServerName serverA = randomServer("srv1").getServerName();
		ServerName serverB = randomServer("srv2").getServerName();
		ServerName serverC = randomServer("srv3").getServerName();

		List<ServerName> servers = Arrays.asList(serverA, serverB, serverC);

		int total = 0;
		Map<ServerName, List<HRegionInfo>> plans = loadBalancer.roundRobinAssignment(regions, servers);
		Assert.assertEquals(3, plans.size());
		List<HRegionInfo> regionsOnServerA = plans.get(serverA);
		Assert.assertTrue( regionsOnServerA.size() <= 7);
		total += regionsOnServerA.size();

		List<HRegionInfo> regionsOnServerB = plans.get(serverB);
		Assert.assertTrue( regionsOnServerB.size() <= 7);
		total += regionsOnServerB.size();

		List<HRegionInfo> regionsOnServerC = plans.get(serverC);
		Assert.assertTrue( regionsOnServerB.size() <= 7);
		total += regionsOnServerC.size();


		Assert.assertEquals(20, total);
	}

	private Map<ServerName, ServerLoad> serverMetricsMapFromClusterState(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		final Map<ServerName, ServerLoad> serverMetricsMap = new TreeMap<>();

		for (Map.Entry<ServerName, List<HRegionInfo>> entry: clusterState.entrySet()) {
			serverMetricsMap.put(entry.getKey(), mockServerLoad(entry.getKey(), entry.getValue()));
		}
		return serverMetricsMap;
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

	protected ServerAndLoad randomServer(final String host) {
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

	protected List<HRegionInfo> randomRegions(int numRegions) {
		return randomRegions(numRegions, -1);
	}

	protected List<HRegionInfo> randomRegions(int numRegions, int numTables) {
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
