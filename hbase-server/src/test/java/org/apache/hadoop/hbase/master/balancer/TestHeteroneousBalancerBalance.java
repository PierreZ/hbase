package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category(SmallTests.class)
public class TestHeteroneousBalancerBalance extends HeteroneousTestBase {
    @Test
    public void testTwoServersAndOneOverloaded() throws IOException {

        createSimpleRulesFile(Arrays.asList("srv1 3", "srv2 20"));
        Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
        ServerName serverA = randomServer("srv1").getServerName();
        ServerName serverB = randomServer("srv2").getServerName();
        List<HRegionInfo> regionsOnServerA = randomRegions(4);
        List<HRegionInfo> regionsOnServerB = randomRegions(9);
        clusterState.put(serverA, regionsOnServerA);
        clusterState.put(serverB, regionsOnServerB);

        testBalance(clusterState, 2);
    }


    @Test
    public void testThreeServersAndOneEmpty() throws IOException {
        createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "srv[3-5] 120"));

        // mock cluster State
        Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
        ServerName serverA = randomServer("srv1").getServerName();
        ServerName serverB = randomServer("srv2").getServerName();
        ServerName serverC = randomServer("srv3").getServerName();
        List<HRegionInfo> regionsOnServerA = randomRegions(10);
        List<HRegionInfo> regionsOnServerB = randomRegions(20);
        List<HRegionInfo> regionsOnServerC = randomRegions(50);
        clusterState.put(serverA, regionsOnServerA);
        clusterState.put(serverB, regionsOnServerB);
        clusterState.put(serverC, regionsOnServerC);

        testBalance(clusterState, 18);
    }

    @Test
    public void testTwoServersAndOneEmpty() throws IOException {
        createSimpleRulesFile(Arrays.asList("srv1 10", "srv2 10"));

        // mock cluster State
        Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
        ServerName serverA = randomServer("srv1").getServerName();
        ServerName serverB = randomServer("srv2").getServerName();
        List<HRegionInfo> regionsOnServerA = randomRegions(10);
        List<HRegionInfo> regionsOnServerB = randomRegions(0);
        clusterState.put(serverA, regionsOnServerA);
        clusterState.put(serverB, regionsOnServerB);

        testBalance(clusterState, 5);
    }

    @Test
    public void testOverloadedServers() throws IOException {
        createSimpleRulesFile(Arrays.asList("srv[1-2] 10", "srv[3-5] 10"));

        // mock cluster State
        Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
        ServerName serverA = randomServer("srv1").getServerName();
        ServerName serverB = randomServer("srv2").getServerName();
        ServerName serverC = randomServer("srv3").getServerName();
        List<HRegionInfo> regionsOnServerA = randomRegions(20);
        List<HRegionInfo> regionsOnServerB = randomRegions(20);
        List<HRegionInfo> regionsOnServerC = randomRegions(50);
        clusterState.put(serverA, regionsOnServerA);
        clusterState.put(serverB, regionsOnServerB);
        clusterState.put(serverC, regionsOnServerC);

        testBalance(clusterState, 0);
    }
    @Test
    public void testSwapLoad() throws IOException {
        createSimpleRulesFile(Arrays.asList("srv1 100", "srv2 1000"));

        // mock cluster State
        Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
        ServerName serverA = randomServer("srv1").getServerName();
        ServerName serverB = randomServer("srv2").getServerName();
        List<HRegionInfo> regionsOnServerA = randomRegions(900);
        List<HRegionInfo> regionsOnServerB = randomRegions(5);
        clusterState.put(serverA, regionsOnServerA);
        clusterState.put(serverB, regionsOnServerB);

        testBalance(clusterState, 817);
    }

    @Test
    public void testSlightlyOverloaded() throws IOException {
        createSimpleRulesFile(Arrays.asList("srv1 100", "srv2 100"));

        // mock cluster State
        Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
        ServerName serverA = randomServer("srv1").getServerName();
        ServerName serverB = randomServer("srv2").getServerName();
        List<HRegionInfo> regionsOnServerA = randomRegions(102);
        List<HRegionInfo> regionsOnServerB = randomRegions(97);
        clusterState.put(serverA, regionsOnServerA);
        clusterState.put(serverB, regionsOnServerB);

        testBalance(clusterState, 2);
    }
}
