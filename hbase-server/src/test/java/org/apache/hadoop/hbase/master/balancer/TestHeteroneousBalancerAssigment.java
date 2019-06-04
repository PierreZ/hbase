package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Category(SmallTests.class)
public class TestHeteroneousBalancerAssigment extends HeteroneousTestBase {

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

    @Test
    public void testOverLoadedStartup() throws IOException {
        createSimpleRulesFile(Arrays.asList("srv[1-3] 10"));
        List<HRegionInfo> regions = randomRegions(100);

        ServerName serverA = randomServer("srv1").getServerName();
        ServerName serverB = randomServer("srv2").getServerName();
        ServerName serverC = randomServer("srv3").getServerName();

        List<ServerName> servers = Arrays.asList(serverA, serverB, serverC);

        int total = 0;
        Map<ServerName, List<HRegionInfo>> plans = loadBalancer.roundRobinAssignment(regions, servers);
        Assert.assertEquals(3, plans.size());
        List<HRegionInfo> regionsOnServerA = plans.get(serverA);
        Assert.assertTrue(  regionsOnServerA.size() <= 34 && regionsOnServerA.size() >= 33);
        total += regionsOnServerA.size();

        List<HRegionInfo> regionsOnServerB = plans.get(serverB);
        Assert.assertTrue(  regionsOnServerB.size() <= 34 && regionsOnServerB.size() >= 33);
        total += regionsOnServerB.size();

        List<HRegionInfo> regionsOnServerC = plans.get(serverC);
        Assert.assertTrue(  regionsOnServerC.size() <= 34 && regionsOnServerC.size() >= 33);
        total += regionsOnServerC.size();

        Assert.assertEquals(100, total);
    }
    @Test
    public void testNullsAndEmpty() throws IOException {
        Map<ServerName, List<HRegionInfo>> plans = loadBalancer.roundRobinAssignment(null, new ArrayList<ServerName>());
        Assert.assertEquals(0, plans.size());

        plans = loadBalancer.roundRobinAssignment(new ArrayList<HRegionInfo>(), null);
        Assert.assertEquals(0, plans.size());
    }
}
