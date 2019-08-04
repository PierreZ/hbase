package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Category({ MasterTests.class, MediumTests.class })
public class TestStochasticLoadBalancerHeterogeneousCost extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestStochasticLoadBalancerHeterogeneousCost.class);

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setFloat("hbase.master.balancer.stochastic.regionCountCost", 0);
    conf.set(StochasticLoadBalancer.COST_FUNCTIONS_COST_FUNCTIONS_KEY, HeterogeneousRegionsCostFunction.class.getName());
    conf.set(HeterogeneousRegionsCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE, TestHeterogeneousRegionsCostFunctionRules.DEFAULT_RULES_TMP_LOCATION);
    loadBalancer = new StochasticLoadBalancer();
    loadBalancer.setConf(conf);
  }

  private void testHeterogeneousWithCluster(int numNodes,
                                            int numRegions,
                                            int numRegionsPerServer,
                                            int replication,
                                            int numTables,
                                            List<String> rules) throws IOException {

    TestHeterogeneousRegionsCostFunctionRules.createSimpleRulesFile(rules);
    Map<ServerName, List<RegionInfo>> serverMap =
            createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables, true);
    testWithCluster(serverMap, null, false, false);
  }

  @Test
  public void testStartup() throws IOException {
    List<String> rules = Arrays.asList("rs[1-2] 10", "rs[3-5] 50");

    int numNodes = 5;
    int numRegions = 200;
    int numRegionsPerServer = 4; // all servers except one
    int replication = 1;
    int numTables = 1;
    testHeterogeneousWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, rules);
  }
}
