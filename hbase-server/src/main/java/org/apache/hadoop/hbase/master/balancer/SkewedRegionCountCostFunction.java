package org.apache.hadoop.hbase.master.balancer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class SkewedRegionCountCostFunction extends StochasticLoadBalancer.CostFunction {

    private static final Log LOG = LogFactory.getLog(SkewedRegionCountCostFunction.class);


    /**
     * configuration used for the path where the rule file is stored
     */
    static final String HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE = "hbase.master.balancer.heterogeneous.rules.file";
    private String rulesPath;

    /**
     * Default rule to apply when the rule file is not found. Default to 200.
     */
    private static final String HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_DEFAULT = "hbase.master.balancer.heterogeneous.rules.default";
    private int defaultNumberOfRegions = 200;

    private static final String REGION_COUNT_SKEW_COST_KEY =
            "hbase.master.balancer.stochastic.heterogeneousRegionCountCost";
    private static final float DEFAULT_REGION_COUNT_SKEW_COST = 500;

    private double[] stats = null;

    /**
     * Contains the rules, key is the regexp for ServerName, value is the limit
     */
    private Map<Pattern, Integer> limitPerRule;

    /**
     * This is a cache, used to not go through all the limitPerRule map all the time when searching for limit
     */
    private Map<ServerName, Integer> limitPerRS;

    SkewedRegionCountCostFunction(Configuration conf) {
        super(conf);
        limitPerRS = new HashMap<>();
        limitPerRule = new HashMap<>();
        this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));

        LOG.info("reloading configuration for Balancer");
        this.rulesPath = conf.get(HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE);

        this.defaultNumberOfRegions = conf.getInt(HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_DEFAULT, 200);
        if (this.defaultNumberOfRegions < 0) {
            LOG.warn("unvalid configuration '" + HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_DEFAULT + "'. Setting default");
        }
    }

    @Override
    double cost() {

        this.loadRules();

        if (stats == null || stats.length != cluster.numServers) {
            stats = new double[cluster.numServers];
        }

        int i = 0;
        for (Map.Entry<ServerName, List<HRegionInfo>> clusterStateEntry: this.cluster.clusterState.entrySet()) {
            ServerName sn = clusterStateEntry.getKey();
            int nbrRegions = clusterStateEntry.getValue().size();
            int maxNbrRegions = findLimitForRS(sn);
            stats[i] = nbrRegions / maxNbrRegions;
            i++;
        }

        return costFromArray(stats);
    }

    /**
     * Find the limit for a ServerName. If not foundm return the default value
     * @param serverName the server we are looking for
     * @return the limit
     */
    protected int findLimitForRS(ServerName serverName) {
        // checking cache first
        if (limitPerRS.containsKey(serverName)) {
            return limitPerRS.get(serverName);
        }

        boolean matched = false;
        int limit = -1;
        for (Map.Entry<Pattern, Integer> entry: limitPerRule.entrySet()) {
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

    /**
     * used to load the rule files.
     */
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

                if (line.startsWith("#")) {
                    continue;
                }

                String[] splits = line.split(" ");
                if (splits.length != 2) {
                    throw new IOException("line '" + line + "' is malformated, expected [regexp] [limit]. Skipping line");
                }

                Pattern pattern = Pattern.compile(splits[0]);
                Integer limit = Integer.parseInt(splits[1]);
                this.limitPerRule.put(pattern, limit);
            } catch (IOException | NumberFormatException | PatternSyntaxException e) {
                LOG.error("error on line: " + e);
            }
        }
        // cleanup cache
        // TODO: only remove cache when there is a diff
        this.limitPerRS.clear();
    }

    // used to read the rule files
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
}
