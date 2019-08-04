package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Category({ MasterTests.class, SmallTests.class })
public class TestHeterogeneousRegionsCostFunctionRules {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestStochasticLoadBalancer.class);

  protected static final String DEFAULT_RULES_TMP_LOCATION = "/tmp/hbase-balancer.rules";

  HeterogeneousRegionsCostFunction costFunction;
  static Configuration conf;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    createSimpleRulesFile(new ArrayList<String>());
    conf = new Configuration();
    conf.set(HeterogeneousRegionsCostFunction.HBASE_MASTER_BALANCER_HETEROGENEOUS_RULES_FILE,
            DEFAULT_RULES_TMP_LOCATION);
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

  @Test
  public void testNoRules() throws IOException {
    cleanup();
    costFunction = new HeterogeneousRegionsCostFunction(conf);
    Assert.assertEquals(0, costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testBadFormatInRules() throws IOException {
    createSimpleRulesFile(new ArrayList<String>());
    costFunction = new HeterogeneousRegionsCostFunction(conf);
    Assert.assertEquals(0, costFunction.getNumberOfRulesLoaded());

    createSimpleRulesFile(Collections.singletonList("bad rules format"));
    costFunction = new HeterogeneousRegionsCostFunction(conf);
    Assert.assertEquals(0, costFunction.getNumberOfRulesLoaded());

    createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "bad_rules format", "a"));
    costFunction = new HeterogeneousRegionsCostFunction(conf);
    Assert.assertEquals(1, costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testTwoRules() throws IOException {
    createSimpleRulesFile(Arrays.asList("^server1$ 10", "^server2 21"));
    costFunction = new HeterogeneousRegionsCostFunction(conf);
    Assert.assertEquals(2, costFunction.getNumberOfRulesLoaded());
  }

  @Test
  public void testBadRegexp() throws IOException {
    createSimpleRulesFile(Arrays.asList("server[ 1"));
    costFunction = new HeterogeneousRegionsCostFunction(conf);
    Assert.assertEquals(0, costFunction.getNumberOfRulesLoaded());
  }

}
