package org.apache.hadoop.hbase.master.balancer;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Pair;

public class HeteroneousBalancer implements LoadBalancer {

	public static final String HBASE_MASTER_BALANCER_HETERONEOUS_RULES_FILE = "hbase.master.balancer.heteroneous.rules.file";
	private ClusterStatus clusterStatus;
	private Map<ServerName, Integer> nbrRegionsPerRS = new HashMap<>();
	private Map<ServerName, Float> loadPerRS = new HashMap<>();
	private float avgLoadOverall;
	Random rand = new Random();


	// This is a cache, used to not go through all the regexp map all the time
	private Map<ServerName, Integer> limitPerRS;

	public HeteroneousBalancer() {
		limitPerRS = new HashMap<>();
		limitPerRule = new HashMap<>();
		this.regionFinder = new RegionLocationFinder();
	}

	// Contains the rules, key is the regexp for ServerName, value is the limit
	private Map<Pattern, Integer> limitPerRule;
	private int defaultNumberOfRegions = 200;
	private MasterServices masterServices;
	private String rulesPath;
	private RegionLocationFinder regionFinder;
	private int maxRegionsMoveablePerRS = 1000;
	private Configuration conf;


	@Override
	public void setClusterStatus(ClusterStatus st) {
		this.clusterStatus = st;
		this.regionFinder.setClusterStatus(st);
		loadRules();
		updateRegionLoad();
	}

	private synchronized void updateRegionLoad() {

		int sumLimit = 0;
		int nbrRegions = 0;

		this.nbrRegionsPerRS.clear();
		this.limitPerRS.clear();
		this.loadPerRS.clear();

		for (ServerName sn : clusterStatus.getServers()) {
			int nbrRegionOnRS = this.clusterStatus.getLoad(sn).getRegionsLoad().size();
			nbrRegionsPerRS.put(sn, nbrRegionOnRS);

			int limit = findLimitForRS(sn);
			sumLimit += limit;
			float rsLoad = (float) nbrRegionOnRS / (float) limit;
			nbrRegions += nbrRegionOnRS;
			loadPerRS.put(sn, rsLoad);

			System.out.println(sn.getHostname() + ": " + nbrRegionOnRS + "/" + limit + " regions (" + rsLoad * 100 + "%)");
		}
		this.avgLoadOverall = (float) nbrRegions / sumLimit;
		System.out.println("According to current rules, cluster is full at " + this.avgLoadOverall * 100 + "%");

	}

	private int findLimitForRS(ServerName serverName) {
		// checking cache first
		if (limitPerRS.containsKey(serverName)) {
			return limitPerRS.get(serverName);
		}

		boolean matched = false;
		int limit = -1;
		for (Entry<Pattern, Integer> entry: limitPerRule.entrySet()) {
			if (entry.getKey().matcher(serverName.getHostname()).matches()) {
				matched = true;
				limit = entry.getValue();
				break;
			}
		}

		if (!matched) {
			limit = defaultNumberOfRegions;
		}

		// Feeding cache
		limitPerRS.put(serverName, limit);

		return limit;
	}

	@Override
	public void setMasterServices(MasterServices masterServices) {
		this.masterServices = masterServices;
	}

	@Override
	public List<RegionPlan> balanceCluster(TableName tableName,
			Map<ServerName, List<HRegionInfo>> clusterState) throws HBaseIOException {
		return balanceCluster(clusterState);
	}

	@Override
	public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState)
			throws HBaseIOException {

		loadRules();

		System.out.println("starting balancer");

		HashMap<ServerName, Integer> futureNbrOfRegions = new HashMap<>(this.nbrRegionsPerRS);
		HashMap<ServerName, Float> futureLoad = new HashMap<>(this.loadPerRS);

		List<RegionPlan> regionPlans = new ArrayList<>();

		for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
			int limit = findLimitForRS(entry.getKey());
			float rsLoad = (float) entry.getValue().size() / (float) limit;

			if (rsLoad > avgLoadOverall || rsLoad > 1) {

				System.out.println(entry.getKey().getHostname() + " has a too much load(" + rsLoad + ">" + avgLoadOverall + ")");

				// we have more regions than expected, moving some
				int nbrRegionsToMove = estimateNumberOfRegionsToMove(entry.getKey(), futureLoad, futureNbrOfRegions);
				List<HRegionInfo> regionsToMove = chooseRegionsToMove(entry.getKey(), entry.getValue(), nbrRegionsToMove);

				int regionsMoved = 0;

				for (HRegionInfo region: regionsToMove) {
					Pair<HRegionInfo, ServerName> regionInfo = findPotentialRegionServer(entry.getKey(), region, futureLoad, futureNbrOfRegions);

					if (null != regionInfo) {

						// We need to recompute cluster usage with the future move
						recomputeMaps(futureNbrOfRegions, futureLoad, entry.getKey(), regionInfo);

						regionPlans.add(new RegionPlan(
								regionInfo.getFirst(),
								entry.getKey(),
								regionInfo.getSecond()));
						regionsMoved++;
					} else {
						System.out.println("could not find a potential RS");
					}
				}
				System.out.println("moved "+ regionsMoved + " out of " + nbrRegionsToMove + " for " + entry.getKey().getHostname());
			}
		}
		return regionPlans;
	}

	private List<HRegionInfo> chooseRegionsToMove(final ServerName serverName, List<HRegionInfo> regionInfos, int nbrRegionsToMove) {

		Collections.sort(regionInfos, new Comparator<HRegionInfo>() {
			@Override
			public int compare(HRegionInfo region1, HRegionInfo region2) {
				float locality1 = 0;
				float locality2 = 0;
				try {
					locality1 = regionFinder.getCache().get(region1).getBlockLocalityIndex(serverName.getHostname());
					locality2 = regionFinder.getCache().get(region2).getBlockLocalityIndex(serverName.getHostname());
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
				return locality1 < locality2? 0 : 1;
			}
		});

		if (regionInfos.size() < nbrRegionsToMove) {
			return regionInfos;
		}

		return regionInfos.subList(0, nbrRegionsToMove);
	}

	private Pair<HRegionInfo, ServerName> recomputeMaps(HashMap<ServerName, Integer> futureNbrOfRegions, HashMap<ServerName, Float> futureLoad, ServerName source, Pair<HRegionInfo, ServerName>  pair ) {

		if (null != source) {
			recomputeMaps(futureNbrOfRegions, futureLoad, source, -1);
		}
		recomputeMaps(futureNbrOfRegions, futureLoad, pair.getSecond(), +1);

		return pair;
	}

	private void recomputeMaps(HashMap<ServerName, Integer> futureNbrOfRegions, HashMap<ServerName, Float> futureLoad, ServerName serverName, int delta) {
		Integer nbrRegions = futureNbrOfRegions.get(serverName);

		if (null == nbrRegions) {
			nbrRegions = 0;
		}

		nbrRegions += delta;
		futureNbrOfRegions.put(serverName, nbrRegions);

		int limit = findLimitForRS(serverName);
		float rsLoad = (float) nbrRegions / (float) limit;
		futureLoad.put(serverName, rsLoad);
	}

	private Pair<HRegionInfo, ServerName> findPotentialRegionServer(ServerName serverName, HRegionInfo regionInfo, HashMap<ServerName, Float> futureLoad, HashMap<ServerName, Integer> futureNbrRegion) {


		Map.Entry<ServerName, Float> minEntry = null;

		for (Map.Entry<ServerName, Float> entry : futureLoad.entrySet()) {
			if (entry.getKey() == serverName) {
				continue;
			}

			int limit = findLimitForRS(entry.getKey());
			Integer current = futureNbrRegion.get(entry.getKey());
			if (null == current) {
				current = 0;
			}


			if (current + 1 >= limit) {
				continue;
			}

			if (minEntry == null || entry.getValue().compareTo(minEntry.getValue()) <= 0) {
				minEntry = entry;
			}
		}

		if (null == minEntry) {
			return null;
		}

		return new Pair<>(regionInfo, minEntry.getKey());
	}

	private int estimateNumberOfRegionsToMove(ServerName serverName, HashMap<ServerName, Float> futureLoad, HashMap<ServerName, Integer> nbrOfRegions) {

		int nbrRegions = nbrOfRegions.get(serverName);
		if (nbrRegions == 0) {
			return 0;
		}
		int limit = findLimitForRS(serverName);
		int i;
		for (i = 0; i < maxRegionsMoveablePerRS; i++) {
			nbrRegions -= 1;
			float rsLoad = (float) nbrRegions / (float) limit;
			if (rsLoad < this.avgLoadOverall && rsLoad < 1) {
				System.out.println("by moving " + i + " regions on "+ serverName.getHostname() + ", we could go from " + ((float) nbrOfRegions.get(serverName) / (float) limit) * 100 + "% to " + rsLoad * 100 + "%");
				break;
			}
		}
		return i;
	}

	private void loadRules() {
		this.limitPerRule.clear();
		List<String> lines = readFile(this.rulesPath);
		for (String line: lines) {
			String[] splits = line.split(" ");
			if (splits.length == 2) {
				Pattern pattern = Pattern.compile(splits[0]);
				Integer limit = Integer.parseInt(splits[1]);
				this.limitPerRule.put(pattern, limit);
			}
		}
		// cleanup cache
		// TODO: only remove cache when there is a diff
		this.limitPerRS.clear();
	}

	/**
	 * Open and read a file, and return the lines in the file as a list
	 * of Strings.
	 * (Demonstrates Java FileReader, BufferedReader, and Java5.)
	 */
	private List<String> readFile(String filename) {
		List<String> records = new ArrayList<String>();
		try	{
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line;
			while ((line = reader.readLine()) != null)
			{
				records.add(line);
			}
			reader.close();
			return records;
		}
		catch (FileNotFoundException e) {
			System.err.println("file not found, skipping");
			return records;
		}
		catch (Exception e) {
			System.err.format("Exception occurred trying to read '%s'.", filename);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * used during startup
	 * @param regions
	 * @param servers
	 * @return
	 * @throws HBaseIOException
	 */
	@Override
	public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
			List<ServerName> servers) throws HBaseIOException {

		System.out.println("called roundrobin");

		Map<ServerName, List<HRegionInfo>> assigments = new HashMap<>();
		HashMap<ServerName, Integer> futureNbrOfRegions = new HashMap<>(this.nbrRegionsPerRS);
		HashMap<ServerName, Float> futureLoad = new HashMap<>(this.loadPerRS);

		for (ServerName serverName: servers) {
			futureLoad.put(serverName, 0f);
			futureNbrOfRegions.put(serverName, 0);
		}

		for (HRegionInfo region : regions) {
			Pair<HRegionInfo, ServerName> regionInfo = findPotentialRegionServer(null,  region,
					futureLoad, futureNbrOfRegions);

			if (null != regionInfo) {

				if (assigments.containsKey(regionInfo.getSecond())) {
					List<HRegionInfo> list = assigments.get(regionInfo.getSecond());
					list.add(regionInfo.getFirst());
					assigments.put(regionInfo.getSecond(), list);
				} else {
					List<HRegionInfo> list = new ArrayList<>();
					list.add(regionInfo.getFirst());
					assigments.put(regionInfo.getSecond(), list);
				}

				// We need to recompute cluster usage with the future move
				recomputeMaps(futureNbrOfRegions, futureLoad, null, regionInfo);
			}

		}
			return assigments;
	}

	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions,
			List<ServerName> servers) throws HBaseIOException {
		System.out.println("called retainAssigment");
		return null;
	}

	/**
	 * we need to assign fast regions, it is not needed to be intelligent
	 * @param regions
	 * @param servers
	 * @return
	 * @throws HBaseIOException
	 */
	@Override
	public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
			List<ServerName> servers) throws HBaseIOException {
		System.out.println("called immediateAssigment");
		Map<HRegionInfo, ServerName> assignment = new HashMap<>();

		for (HRegionInfo regionInfo: regions) {
			assignment.put(regionInfo, randomAssignment(regionInfo, servers));
		}
		return assignment;
	}

	@Override
	public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers)
			throws HBaseIOException {
		System.out.println("called random");
		return servers.get(rand.nextInt(servers.size()));
	}

	@Override
	public void initialize() throws HBaseIOException {
		loadRules();
	}

	@Override
	public void regionOnline(HRegionInfo regionInfo, ServerName sn) {

	}

	@Override
	public void regionOffline(HRegionInfo regionInfo) {

	}

	@Override
	public void onConfigurationChange(Configuration conf) {
		this.setConf(conf);
	}

	@Override
	public void postMasterStartupInitialize() {

	}

	@Override
	public void updateBalancerStatus(boolean status) {

	}

	@Override
	public void setConf(Configuration configuration) {
		this.conf = configuration;
		this.reloadConfiguration();
	}

	private void reloadConfiguration() {
		this.rulesPath = this.conf.get(HBASE_MASTER_BALANCER_HETERONEOUS_RULES_FILE);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void stop(String why) {

	}

	@Override
	public boolean isStopped() {
		return false;
	}
}
