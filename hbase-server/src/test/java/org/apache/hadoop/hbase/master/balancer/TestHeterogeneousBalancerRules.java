package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

@Category(SmallTests.class)
public class TestHeterogeneousBalancerRules extends HeterogeneousTestBase {
    @Test
    public void testNoRules() throws IOException {
        cleanup();
        loadBalancer.setConf(conf);
        Assert.assertEquals(0, loadBalancer.getLimitPerRule().size());
    }

    @Test
    public void testBadFormatInRules() throws IOException {
        createSimpleRulesFile(new ArrayList<String>());
        loadBalancer.setConf(conf);
        Assert.assertEquals(0, loadBalancer.getLimitPerRule().size());

        createSimpleRulesFile(Collections.singletonList("bad rules format"));
        loadBalancer.setConf(conf);
        Assert.assertEquals(0, loadBalancer.getLimitPerRule().size());

        createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "bad_rules format", "a"));
        loadBalancer.setConf(conf);
        Assert.assertEquals(1, loadBalancer.getLimitPerRule().size());
    }

    @Test
    public void testTwoRules() throws IOException {
        createSimpleRulesFile(Arrays.asList("^server1$ 10", "^server2 21"));
        loadBalancer.setConf(conf);
        Assert.assertEquals(2, loadBalancer.getLimitPerRule().size());
    }

    @Test
    public void testBadRegexp() throws IOException {
        createSimpleRulesFile(Arrays.asList("server[ 1"));
        loadBalancer.setConf(conf);
        Assert.assertEquals(0, loadBalancer.getLimitPerRule().size());
    }
}
