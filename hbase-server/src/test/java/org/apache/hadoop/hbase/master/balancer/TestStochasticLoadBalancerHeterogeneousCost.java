/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({MasterTests.class, MediumTests.class})
public class TestStochasticLoadBalancerHeterogeneousCost extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestStochasticLoadBalancerHeterogeneousCost.class);

  private static final Logger LOG = LoggerFactory.getLogger(
          TestStochasticLoadBalancerHeterogeneousCost.class);

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    BalancerTestBase.conf = HBaseConfiguration.create();
    BalancerTestBase.conf.setFloat(
            "hbase.master.balancer.stochastic.regionCountCost", 0);
    BalancerTestBase.conf.setFloat(
            "hbase.master.balancer.stochastic.primaryRegionCountCost", 0);
    BalancerTestBase.conf.setFloat(
            "hbase.master.balancer.stochastic.tableSkewCost", 0);
    BalancerTestBase.conf.setBoolean(
            "hbase.master.balancer.stochastic.runMaxSteps", true);
    BalancerTestBase.conf.set(
            StochasticLoadBalancer.COST_FUNCTIONS_COST_FUNCTIONS_KEY,
            HeterogeneousRegionCountCostFunction.class.getName());

    BalancerTestBase.conf.set(
            HeterogeneousRegionCountCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
            TestStochasticLoadBalancerHeterogeneousCostRules.DEFAULT_RULES_TMP_LOCATION);

    BalancerTestBase.loadBalancer = new StochasticLoadBalancer();
    BalancerTestBase.loadBalancer.setConf(BalancerTestBase.conf);
  }

  @Test
  public void testMultipleGroups() throws IOException {
    final List<String> rules = Arrays.asList("rs0 500", "rs1 200", "rs2 300", "rs3 400", "rs4 100");

    final int numNodes = 5;
    final int numRegions = 1250;
    final int numRegionsPerServer = 250;
    this.testHeterogeneousWithCluster(numNodes, numRegions, numRegionsPerServer, rules);
  }

  private void testHeterogeneousWithCluster(final int numNodes,
                                            final int numRegions,
                                            final int numRegionsPerServer,
                                            final List<String> rules) throws IOException {

    TestStochasticLoadBalancerHeterogeneousCostRules.createSimpleRulesFile(rules);
    final Map<ServerName, List<RegionInfo>> serverMap =
            this.createServerMap(numNodes, numRegions, numRegionsPerServer,
                    1, 1);
    this.testWithCluster(serverMap, null, true, false);
  }

  @Override
  protected void testWithCluster(final Map<ServerName, List<RegionInfo>> serverMap,
                                 final RackManager rackManager,
                                 final boolean assertFullyBalanced,
                                 final boolean assertFullyBalancedForReplicas) {
    final List<ServerAndLoad> list = this.convertToList(serverMap);
    LOG.info("Mock Cluster : " + this.printMock(list) + " " + this.printStats(list));

    BalancerTestBase.loadBalancer.setRackManager(rackManager);
    // Run the balancer.

    final List<RegionPlan> plans = BalancerTestBase.loadBalancer.balanceCluster(serverMap);
    assertNotNull(plans);

    // Check to see that this actually got to a stable place.
    if (assertFullyBalanced || assertFullyBalancedForReplicas) {
      // Apply the plan to the mock cluster.
      final List<ServerAndLoad> balancedCluster = this.reconcile(list, plans, serverMap);

      // Print out the cluster loads to make debugging easier.
      LOG.info("Mock Balanced cluster : " + this.printMock(balancedCluster));

      if (assertFullyBalanced) {
        final List<RegionPlan> secondPlans = BalancerTestBase.
                loadBalancer.balanceCluster(serverMap);
        assertNull(secondPlans);

        // retrieving cost function
        final HeterogeneousRegionCountCostFunction cf =
                new HeterogeneousRegionCountCostFunction(conf);
        assertNotNull(cf);

        // checking that we all hosts have a number of regions below their limit
        for (final ServerAndLoad serverAndLoad : balancedCluster) {
          final ServerName sn = serverAndLoad.getServerName();
          final int numberRegions = serverAndLoad.getLoad();
          final int limit = cf.findLimitForRS(sn);
          LOG.debug(sn.getHostname() + ":" + numberRegions + "/" + limit
                  + "(" + ((double) numberRegions / (double) limit * 100) + "%)");
          assertTrue("Host " + sn.getHostname() + " should be below limit",
                  numberRegions < limit);
        }
      }

      if (assertFullyBalancedForReplicas) {
        this.assertRegionReplicaPlacement(serverMap, rackManager);
      }
    }
  }

  @Test
  public void testTenRS() throws IOException {
    final List<String> rules = Arrays.asList("rs[1-3] 100", "rs[4-7] 250", "rs[8-9] 200");

    final int numNodes = 10;
    final int numRegions = 1000;
    final int numRegionsPerServer = 100;
    this.testHeterogeneousWithCluster(
            numNodes,
            numRegions,
            numRegionsPerServer,
            rules);
  }

  private Queue<ServerName> serverQueue = new LinkedList<>();

  protected ServerAndLoad createServer(final int numRegionsPerServer, final String host) {
    if (!this.serverQueue.isEmpty()) {
      ServerName sn = this.serverQueue.poll();
      return new ServerAndLoad(sn, numRegionsPerServer);
    }
    Random rand = ThreadLocalRandom.current();
    int port = rand.nextInt(60000);
    long startCode = rand.nextLong();
    ServerName sn = ServerName.valueOf(host, port, startCode);
    return new ServerAndLoad(sn, numRegionsPerServer);
  }

  protected ServerAndLoad randomServer(final int numRegionsPerServer) {
    Random rand = ThreadLocalRandom.current();
    String host = "srv" + rand.nextInt(Integer.MAX_VALUE);
    return createServer(numRegionsPerServer, host);
  }

}
