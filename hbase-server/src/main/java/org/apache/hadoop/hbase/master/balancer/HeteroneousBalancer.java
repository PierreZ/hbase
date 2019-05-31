package org.apache.hadoop.hbase.master.balancer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hbase.util.Pair;

public class HeteroneousBalancer implements LoadBalancer {


    /**
     * configuration used for the path where the rule file is stored
     */
    static final String HBASE_MASTER_BALANCER_HETERONEOUS_RULES_FILE = "hbase.master.balancer.heteroneous.rules.file";
    private String rulesPath;

    /**
     * Default rule to apply when the rule file is not found. Default to 200.
     */
    private static final String HBASE_MASTER_BALANCER_HETERONEOUS_RULES_DEFAULT = "hbase.master.balancer.heteroneous.rules.default";
    private int defaultNumberOfRegions = 200;

    /**
     * max number of regions moveable in a single call to balance. Default is MAX_INT
     */
    private static final String HBASE_MASTER_BALANCER_HETERONEOUS_MAX_MOVEABLE_REGIONS = "hbase.master.balancer.heteroneous.max.moveable.regions";
    private int maxRegionsMoveablePerRS;

    /**
     * Maps used to keep track of load per RS
     */
    private Map<ServerName, Integer> nbrRegionsPerRS = new HashMap<>();
    private Map<ServerName, Float> loadPerRS = new HashMap<>();

    /**
     * The global overall load: current number of regions / maximum of regions according to the rules
     */
    private float avgLoadOverall;

    /**
     * Contains the rules, key is the regexp for ServerName, value is the limit
     */
    private Map<Pattern, Integer> limitPerRule;

    /**
     * This is a cache, used to not go through all the limitPerRule map all the time when searching for limit
     */
    private Map<ServerName, Integer> limitPerRS;

    private static final Log LOG = LogFactory.getLog(HeteroneousBalancer.class);
    private Random rand = new Random(System.currentTimeMillis());

    private MasterServices masterServices;
    private RegionLocationFinder regionFinder;
    private Configuration conf;
    private ClusterStatus clusterStatus;

    public HeteroneousBalancer() {
        limitPerRS = new HashMap<>();
        limitPerRule = new HashMap<>();
        this.regionFinder = new RegionLocationFinder();
    }

    @Override
    public void setClusterStatus(ClusterStatus st) {
        this.clusterStatus = st;
        this.regionFinder.setClusterStatus(st);
        loadRules();
        updateRegionLoad();
    }

    /**
     * for each servers, we need to go through the number of regions it is holding
     * and the maximum number of regions he should have.
     */
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

            LOG.info(sn.getHostname() + ": " + nbrRegionOnRS + "/" + limit + " regions (" + rsLoad * 100 + "%)");
        }
        this.avgLoadOverall = (float) nbrRegions / sumLimit;
        LOG.info("According to current rules, cluster is full at " + this.avgLoadOverall * 100 + "%");

    }

    /**
     * Find the limit for a ServerName. If not foundm return the default value
     * @param serverName the server we are looking for
     * @return the limit
     */
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

        LOG.info("starting balancer");

        HashMap<ServerName, Integer> futureNbrOfRegions = new HashMap<>(this.nbrRegionsPerRS);
        HashMap<ServerName, Float> futureLoad = new HashMap<>(this.loadPerRS);

        List<RegionPlan> regionPlans = new ArrayList<>();

        for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
            int limit = findLimitForRS(entry.getKey());
            float rsLoad = (float) entry.getValue().size() / (float) limit;

            if (rsLoad > avgLoadOverall || rsLoad > 1) {

                LOG.info(entry.getKey().getHostname() + " has a too much load(" + rsLoad + ">" + avgLoadOverall + ")");

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
                        LOG.warn("could not find a potential RS");
                    }
                }
                LOG.info("moved "+ regionsMoved + " out of " + nbrRegionsToMove + " for " + entry.getKey().getHostname());
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

    private Pair<HRegionInfo, ServerName> findPotentialRegionServer(ServerName serverName,
                                                                    HRegionInfo regionInfo,
                                                                    HashMap<ServerName, Float> futureLoad,
                                                                    HashMap<ServerName, Integer> futureNbrRegion) {


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
                LOG.info("by moving " + i + " regions on "+ serverName.getHostname() + ", we could go from " + ((float) nbrOfRegions.get(serverName) / (float) limit) * 100 + "% to " + rsLoad * 100 + "%");
                break;
            }
        }
        return i;
    }

    private void loadRules() {
        List<String> lines = readFile(this.rulesPath);
        if (lines.size() == 0) {
            return;
        }

        LOG.info("loading rules file '" + this.rulesPath + "'");
        this.limitPerRule.clear();
        for (String line: lines) {
            try {
                if (line.length() == 0) {
                    continue;
                }

                String[] splits = line.split(" ");
                if (splits.length != 2) {
                    throw new IOException("line '" + line + "' is malformated, expected [regexp] [limit]. Skipping line");
                }

                Pattern pattern = Pattern.compile(splits[0]);
                Integer limit = Integer.parseInt(splits[1]);
                this.limitPerRule.put(pattern, limit);
            } catch (IOException | NumberFormatException e) {
                LOG.error(e);
            }
        }
        // cleanup cache
        // TODO: only remove cache when there is a diff
        this.limitPerRS.clear();
    }

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
            LOG.warn("file '" + filename + "' not found, skipping. We will be using default value for every RS which is " + this.defaultNumberOfRegions);
            return records;
        }

        catch (Exception e) {
            LOG.error(e);
            return new ArrayList<String>();
        }
    }

    /**
     * Round robin a list of regions to a list of servers
     */
    @Override
    public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
                                                                   List<ServerName> servers) throws HBaseIOException {

        LOG.info("called roundrobin");

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

    /**
     * Generates a bulk assignment startup plan, attempting to reuse the existing
     * assignment information from META, but adjusting for the specified list of
     * available/online servers available for assignment.
     * <p>
     * Takes a map of all regions to their existing assignment from META. Also
     * takes a list of online servers for regions to be assigned to. Attempts to
     * retain all assignment, so in some instances initial assignment will not be
     * completely balanced.
     * <p>
     * Any leftover regions without an existing server to be assigned to will be
     * assigned randomly to available servers.
     *
     * @param regions regions and existing assignment from meta
     * @param servers available servers
     * @return map of servers and regions to be assigned to them
     */
    @Override
    public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions,
                                                               List<ServerName> servers) throws HBaseIOException {
        Map<ServerName, List<HRegionInfo>> assigments = new HashMap<>();

        ArrayList<HRegionInfo> toBalance = new ArrayList<>();

        for (Map.Entry<HRegionInfo, ServerName> regionEntry: regions.entrySet()) {
            HRegionInfo region = regionEntry.getKey();
            ServerName oldServerName = regionEntry.getValue();

            if (servers.contains(oldServerName)) {
                if (assigments.containsKey(oldServerName)) {
                    assigments.get(oldServerName).add(region);
                } else {
                    List<HRegionInfo> listOfRegions = new ArrayList<>();
                    listOfRegions.add(region);
                    assigments.put(oldServerName, listOfRegions);
                }
            } else {
                // Offline server, we will need to balance them
                toBalance.add(region);
            }
        }

        LOG.info(toBalance.size() + "regions with an offline server, calling roundRobinAssigment on them");

        Map<ServerName, List<HRegionInfo>> regionsLeftAssigments = roundRobinAssignment(toBalance,
                servers);

        // merge all maps
        for (Map.Entry<ServerName, List<HRegionInfo>> regionEntry: regionsLeftAssigments.entrySet()) {
            ServerName serverName = regionEntry.getKey();
            List<HRegionInfo> regionsToAdd = regionEntry.getValue();
            if (assigments.containsKey(serverName)) {
                assigments.get(serverName).addAll(regionsToAdd);
            } else {
                assigments.put(serverName, regionsToAdd);
            }
        }
        return assigments;
    }

    Map<Pattern, Integer> getLimitPerRule() {
        return limitPerRule;
    }

    /**
     * Generates an immediate assignment plan to be used by a new master for
     * regions in transition that do not have an already known destination.
     *
     * Takes a list of regions that need immediate assignment and a list of all
     * available servers. Returns a map of regions to the server they should be
     * assigned to.
     *
     * This method will return quickly and does not do any intelligent balancing.
     * The goal is to make a fast decision not the best decision possible.
     *
     * Currently this is random.
     *
     * @param regions
     * @param servers
     * @return map of regions to the server it should be assigned to
     */
    @Override
    public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
                                                            List<ServerName> servers) throws HBaseIOException {
        LOG.info("called immediateAssigment");
        Map<HRegionInfo, ServerName> assignment = new HashMap<>();

        for (HRegionInfo regionInfo: regions) {
            assignment.put(regionInfo, randomAssignment(regionInfo, servers));
        }
        return assignment;
    }

    /**
     * Used to assign a single region to a random server.
     */
    @Override
    public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers)
            throws HBaseIOException {
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
        LOG.info("reloading configuration for Balancer");
        this.rulesPath = this.conf.get(HBASE_MASTER_BALANCER_HETERONEOUS_RULES_FILE);


        this.defaultNumberOfRegions = this.conf.getInt(HBASE_MASTER_BALANCER_HETERONEOUS_RULES_DEFAULT, 200);
        if (this.defaultNumberOfRegions < 0) {
            LOG.warn("unvalid configuration '" + HBASE_MASTER_BALANCER_HETERONEOUS_RULES_DEFAULT + "'. Setting default");
        }

        this.maxRegionsMoveablePerRS = this.conf.getInt(HBASE_MASTER_BALANCER_HETERONEOUS_MAX_MOVEABLE_REGIONS, Integer.MAX_VALUE);
        if (this.maxRegionsMoveablePerRS < 0) {
            LOG.warn("unvalid configuration '" + HBASE_MASTER_BALANCER_HETERONEOUS_MAX_MOVEABLE_REGIONS+ "'. Setting default");
        }

        this.loadRules();
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
