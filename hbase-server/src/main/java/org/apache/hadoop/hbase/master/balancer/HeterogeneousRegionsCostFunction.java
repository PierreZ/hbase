/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class HeterogeneousRegionsCostFunction extends StochasticLoadBalancer.CostFunction {

  private static final Log LOG = LogFactory.getLog(HeterogeneousRegionsCostFunction.class);

  /**
   * configuration used for the path where the rule file is stored.
   */
  static final String HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE = "hbase.master.balancer.heterogeneous.rules.file";
  private String rulesPath;

  /**
   * Default rule to apply when the rule file is not found. Default to 200.
   */
  private static final String HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_DEFAULT = "hbase.master.balancer.heterogeneous.rules.default";
  private int defaultNumberOfRegions = 200;

  /**
   * Cost for the function. Default to 500, can be changed.
   */
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

  /** Called once per LB invocation to give the cost function
   * to initialize it's state, and perform any costly calculation.
   */
  @Override
  void init(BaseLoadBalancer.Cluster cluster) {
    this.cluster = cluster;
    this.loadRules();
  }


  public HeterogeneousRegionsCostFunction(Configuration conf) {
    super(conf);
    limitPerRS = new HashMap<>();
    limitPerRule = new HashMap<>();
    this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));

    this.rulesPath = conf.get(HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE);

    this.defaultNumberOfRegions = conf.getInt(HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_DEFAULT, 200);
    if (this.defaultNumberOfRegions < 0) {
      LOG.warn("unvalid configuration '" + HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_DEFAULT + "'. Setting default");
    }
  }

  @Override
  protected double cost() {

    if (stats == null || stats.length != cluster.numServers) {
      stats = new double[cluster.numServers];
    }

    int i = 0;
    for (Map.Entry<ServerName, List<RegionInfo>> rsEntry: this.cluster.clusterState.entrySet()) {

      ServerName sn = rsEntry.getKey();
      int nbrRegions = rsEntry.getValue().size();

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
          throw new IOException("line '" + line + "' is malformated, " +
                  "expected [regexp] [limit]. Skipping line");
        }

        Pattern pattern = Pattern.compile(splits[0]);
        Integer limit = Integer.parseInt(splits[1]);
        this.limitPerRule.put(pattern, limit);
      } catch (IOException | NumberFormatException | PatternSyntaxException e) {
        LOG.error("error on line: " + e);
      }
    }
    // cleanup cache
    this.limitPerRS.clear();
  }

  // used to read the rule files from either HDFS or local FS
  private List<String> readFile(String filename) {
    List<String> records = new ArrayList<String>();

    if (null == filename) {
     return records;
    }

    Configuration conf = new Configuration();
    Path path = new Path(filename);
    try {
      FileSystem fs = FileSystem.get(conf);
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

      String line;
      while ((line = reader.readLine()) != null) {
        records.add(line);
      }
      reader.close();
      return records;
    } catch (IOException e) {
      LOG.error("cannot read rules file located at ' "+ filename +" ':" + e.getMessage());
    }
    return records;
  }

  public int getNumberOfRulesLoaded() {
    return this.limitPerRule.size();
  }
}
