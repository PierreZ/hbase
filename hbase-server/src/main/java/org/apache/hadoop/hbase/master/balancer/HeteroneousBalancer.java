/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Make decisions about the placement of regions according to rules. This rules are defining
 * how many regions a RegionServer can hold.
 */
public class HeteroneousBalancer implements LoadBalancer {

	public static final String HBASE_MASTER_BALANCER_HETERONEOUS_RULES_FILE = "hbase.master.balancer.heteroneous.rules.file";

	private ClusterMetrics clusterMetrics;
	private Configuration conf;


	// This is a cache, used to not go through all the regexp map all the time
	private Map<ServerName, Integer> limitPerRS;

	// Contains the rules, key is the regexp for ServerName, value is the limit
	private Map<Pattern, Integer> limitPerRule;

	private Map<ServerName, Integer> nbrRegionsPerRS;
	private Map<ServerName, Float> loadPerRS;
	private float avgLoadOverall;

	private int defaultNumberOfRegions;
	private MasterServices services;
	private ServerName masterServerName;
	private RegionLocationFinder regionFinder;
	private boolean maintenanceMode;
	private String rulesPath;
	private int maxRegionsMoveablePerRS = 1000;


	/**
	 * Set the current cluster status.  This allows a LoadBalancer to map host name to a server
	 * @param st
	 */
	@Override
	public void setClusterMetrics(ClusterMetrics st) {
		this.clusterMetrics = st;
	}

	/**
	 * Pass RegionStates and allow balancer to set the current cluster load.
	 * @param clusterLoad
	 */
	@Override
	public void setClusterLoad(Map<TableName, Map<ServerName, List<RegionInfo>>> clusterLoad) {

		loadRules();

		int sum = 0;
		int nbrRegions = 0;

		for(Map.Entry<TableName, Map<ServerName, List<RegionInfo>>> clusterEntry : clusterLoad.entrySet()) {
			for (Map.Entry<ServerName, List<RegionInfo>> entry : clusterEntry.getValue().entrySet()) {

				nbrRegionsPerRS.put(entry.getKey(), entry.getValue().size());

				int limit = findLimitForRS(entry.getKey());
				float rsLoad = (float) entry.getValue().size() / (float) limit;
				sum += rsLoad;

				loadPerRS.put(entry.getKey(), rsLoad);

				nbrRegions++;
				System.out.println(entry.getKey().getHostnameLowerCase() + ": " + entry.getValue().size() + "/" + limit + " regions (" + rsLoad * 100 + "%)");
			}
		}
		this.avgLoadOverall =(float) nbrRegions /(float) sum;

		System.out.println("overall load: " + avgLoadOverall);
	}

	private int findLimitForRS(ServerName serverName) {
		// checking cache first
		if (limitPerRS.containsKey(serverName)) {
			return limitPerRS.get(serverName);
		}

		// If not find in cache, we should search through the map of regexp
		Integer limit = limitPerRule.entrySet()
				.stream()
				.filter(entry -> entry.getKey().matcher(serverName.getHostnameLowerCase()).matches())
				.map(Entry::getValue)
				.findFirst().orElse(defaultNumberOfRegions);

		// Feeding cache
		limitPerRS.put(serverName, limit);

		return limit;
	}

	/**
	 * Set the master service.
	 * @param masterServices
	 */
	@Override
	public void setMasterServices(MasterServices masterServices) {
		masterServerName = masterServices.getServerName();
		this.services = masterServices;
		this.regionFinder.setServices(masterServices);
		if (this.services.isInMaintenanceMode()) {
			this.maintenanceMode = true;
		}
	}

	/**
	 * Perform the major balance operation
	 * @param tableName
	 * @param clusterState
	 * @return List of plans
	 */
	@Override
	public List<RegionPlan> balanceCluster(TableName tableName,
			Map<ServerName, List<RegionInfo>> clusterState) throws HBaseIOException {
		return balanceCluster(clusterState);
	}

	/**
	 * Perform the major balance operation
	 * @param clusterState
	 * @return List of plans
	 */
	@Override
	public List<RegionPlan> balanceCluster(Map<ServerName, List<RegionInfo>> clusterState)
			throws HBaseIOException {

		loadRules();

		HashMap<ServerName, Integer> futureNbrOfRegions = new HashMap<>(this.nbrRegionsPerRS);
		HashMap<ServerName, Float> futureLoad = new HashMap<>(this.loadPerRS);

		List<RegionPlan> regionPlans = new ArrayList<>();

		for (Map.Entry<ServerName, List<RegionInfo>> entry : clusterState.entrySet()) {
			int limit = findLimitForRS(entry.getKey());
			float rsLoad = (float) entry.getValue().size() / (float) limit;

			if (rsLoad > avgLoadOverall) {

				System.out.println(entry.getKey().getHostnameLowerCase() + " has a too much load(" + rsLoad + ">" + avgLoadOverall + ")");

				// we have more regions than expected, moving some
				int nbrRegionsToMove = estimateNumberOfRegionsToMove(entry.getKey(), futureLoad, futureNbrOfRegions);
				List<RegionInfo> regionsToMove = chooseRegionsToMove(entry.getKey(), entry.getValue(), nbrRegionsToMove);

				for (RegionInfo region: regionsToMove) {
					Optional<Pair<RegionInfo, ServerName>> potentialRegionServer = findPotentialRegionServer(region, futureLoad);

					if (potentialRegionServer.isPresent()) {
						Pair<RegionInfo, ServerName> regionInfo = potentialRegionServer.get();

						// We need to recompute cluster usage with the future move
						recomputeMaps(futureNbrOfRegions, futureLoad, entry.getKey(), regionInfo);

						regionPlans.add(new RegionPlan(
								regionInfo.getFirst(),
								entry.getKey(),
								regionInfo.getSecond()));
					}

				}
			}
		}

		return regionPlans;
	}

	private List<RegionInfo> chooseRegionsToMove(ServerName serverName, List<RegionInfo> regionInfos, int nbrRegionsToMove) {
		regionInfos.sort((region1, region2) -> {

			float locality1 = 0;
			float locality2 = 0;
			try {
				locality1 = regionFinder.getCache().get(region1).getBlockLocalityIndex(serverName.getHostname());
				locality2 = regionFinder.getCache().get(region2).getBlockLocalityIndex(serverName.getHostname());
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
			return locality1 < locality2? 0 : 1;
		});

		return regionInfos.subList(0, nbrRegionsToMove);
	}

	private Pair<RegionInfo, ServerName> recomputeMaps(HashMap<ServerName, Integer> futureNbrOfRegions, HashMap<ServerName, Float> futureLoad, ServerName source, Pair<RegionInfo, ServerName>  pair ) {

		recomputeMaps(futureNbrOfRegions, futureLoad, source, -1);
		recomputeMaps(futureNbrOfRegions, futureLoad, pair.getSecond(), +1);

		return pair;
	}

	private void recomputeMaps(HashMap<ServerName, Integer> futureNbrOfRegions, HashMap<ServerName, Float> futureLoad, ServerName serverName, int delta) {
		Integer nbrRegions = futureNbrOfRegions.get(serverName);
		nbrRegions += delta;
		futureNbrOfRegions.put(serverName, nbrRegions);

		int limit = findLimitForRS(serverName);
		float rsLoad = (float) nbrRegions / (float) limit;
		futureLoad.put(serverName, rsLoad);
	}

	private Optional<Pair<RegionInfo, ServerName>> findPotentialRegionServer(RegionInfo regionInfo, HashMap<ServerName, Float> futureLoad) {

		return futureLoad.entrySet()
				.stream()
				.min(Map.Entry.comparingByValue())
				.filter(entry -> entry.getValue() < this.avgLoadOverall)
				.map(entry -> new Pair<>(regionInfo, entry.getKey()));
	}

	// TODO
	private int estimateNumberOfRegionsToMove(ServerName serverName, HashMap<ServerName, Float> futureLoad, HashMap<ServerName, Integer> nbrOfRegions) {

		int nbrRegions = nbrOfRegions.get(serverName);
		int limit = findLimitForRS(serverName);
		int i;
		for (i = 0; i < maxRegionsMoveablePerRS; i++) {
			nbrRegions -= 1;
			float rsLoad = (float) nbrRegions / (float) limit;
			if (rsLoad < this.avgLoadOverall) {
				System.out.println("by moving " + i + " regions on "+ serverName.getHostnameLowerCase() +", we are going from " + ((float) nbrOfRegions.get(serverName) / (float) limit) * 100 + "% to " + rsLoad * 100 + "%");
				break;
			}
		}
		return i;
	}

	/**
	 * Perform a Round Robin assignment of regions.
	 * @param regions
	 * @param servers
	 * @return Map of servername to regioninfos
	 */
	@Override
	public Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
			List<ServerName> servers) throws HBaseIOException {
		HashMap<ServerName, List<RegionInfo>> assignment = new HashMap<>();

		for (RegionInfo regionInfo: regions) {
			ServerName serverName = randomAssignment(regionInfo, servers);
			if (assignment.containsKey(serverName)) {
				assignment.get(serverName).add(regionInfo);
			} else {
				regions = new ArrayList<>();
				regions.add(regionInfo);
				assignment.put(serverName, regions);
			}
		}

		return assignment;
	}

	/**
	 * Assign regions to the previously hosting region server
	 * @param regions
	 * @param servers
	 * @return List of plans
	 */
	@Nullable
	@Override
	public Map<ServerName, List<RegionInfo>> retainAssignment(Map<RegionInfo, ServerName> regions,
			List<ServerName> servers) throws HBaseIOException {

		return null;
	}

	/**
	 * Get a random region server from the list
	 * @param regionInfo Region for which this selection is being done.
	 * @param servers
	 * @return Servername
	 */
	@Override
	public ServerName randomAssignment(RegionInfo regionInfo, List<ServerName> servers)
			throws HBaseIOException {
		return null;
	}

	/**
	 * Initialize the load balancer. Must be called after setters.
	 * @throws HBaseIOException
	 */
	@Override
	public void initialize() throws HBaseIOException {
		this.loadPerRS = new HashMap<>();
		this.limitPerRule = new HashMap<>();
		this.limitPerRS = new HashMap<>();
		this.nbrRegionsPerRS = new HashMap<>();
		this.regionFinder = new RegionLocationFinder();
	}

	/**
	 * Marks the region as online at balancer.
	 * @param regionInfo
	 * @param sn
	 */
	@Override
	public void regionOnline(RegionInfo regionInfo, ServerName sn) {

	}

	/**
	 * Marks the region as offline at balancer.
	 * @param regionInfo
	 */
	@Override
	public void regionOffline(RegionInfo regionInfo) {

	}

	/*
	 * Notification that config has changed
	 * @param conf
	 */
	@Override
	public void onConfigurationChange(Configuration conf) {
		this.setConf(conf);
	}

	/**
	 * If balancer needs to do initialization after Master has started up, lets do that here.
	 */
	@Override
	public void postMasterStartupInitialize() {
		this.rulesPath = this.conf.get(HBASE_MASTER_BALANCER_HETERONEOUS_RULES_FILE);
		this.loadRules();
	}

	private void loadRules() {
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
		try
		{
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
		catch (Exception e)
		{
			System.err.format("Exception occurred trying to read '%s'.", filename);
			e.printStackTrace();
			return null;
		}
	}


	/*Updates balancer status tag reported to JMX*/
	@Override
	public void updateBalancerStatus(boolean status) {

	}

	@Override
	public void setConf(Configuration configuration) {
		this.conf = configuration;
		postMasterStartupInitialize();
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void stop(String why) {

	}

	@Override
	public boolean isStopped() {
		return false;
	}

	public int getDefaultNumberOfRegions() {
		return defaultNumberOfRegions;
	}

	public void setDefaultNumberOfRegions(int defaultNumberOfRegions) {
		this.defaultNumberOfRegions = defaultNumberOfRegions;
	}
}
