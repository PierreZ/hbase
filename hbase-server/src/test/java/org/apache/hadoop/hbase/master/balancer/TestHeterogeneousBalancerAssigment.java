package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.*;

@Category(SmallTests.class)
public class TestHeterogeneousBalancerAssigment extends HeterogeneousTestBase {

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


    /**
     * Test the cluster startup bulk assignment which attempts to retain
     * assignment info.
     * @throws Exception
     */
    @Test (timeout=180000)
    public void testRetainAssignment() throws Exception {
        // Test simple case where all same servers are there
        List<ServerAndLoad> servers = randomServers(10, 10);
        List<HRegionInfo> regions = randomRegions(100);
        Map<HRegionInfo, ServerName> existing = new TreeMap<HRegionInfo, ServerName>();
        for (int i = 0; i < regions.size(); i++) {
            ServerName sn = servers.get(i % servers.size()).getServerName();
            // The old server would have had same host and port, but different
            // start code!
            ServerName snWithOldStartCode =
                    ServerName.valueOf(sn.getHostname(), sn.getPort(), sn.getStartcode() - 10);
            existing.put(regions.get(i), snWithOldStartCode);
        }
        List<ServerName> listOfServerNames = getListOfServerNames(servers);
        Map<ServerName, List<HRegionInfo>> assignment =
                loadBalancer.retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);

        // Include two new servers that were not there before
        List<ServerAndLoad> servers2 = new ArrayList<ServerAndLoad>(servers);
        servers2.add(randomServer("srv11"));
        servers2.add(randomServer("srv12"));
        listOfServerNames = getListOfServerNames(servers2);
        assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);

        // Remove two of the servers that were previously there
        List<ServerAndLoad> servers3 = new ArrayList<ServerAndLoad>(servers);
        servers3.remove(0);
        servers3.remove(0);
        listOfServerNames = getListOfServerNames(servers3);
        assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);
    }


    /**
     * Test the cluster startup bulk assignment which attempts to retain
     * assignment info.
     * @throws Exception
     */
    @Test (timeout=180000)
    public void testSimpleRetainAssignment() throws Exception {
        // Test simple case where all same servers are there
        List<ServerAndLoad> servers = randomServers(10, 10);
        List<HRegionInfo> regions = randomRegions(100);
        Map<HRegionInfo, ServerName> existing = new TreeMap<HRegionInfo, ServerName>();
        for (int i = 0; i < regions.size(); i++) {
            ServerName sn = servers.get(i % servers.size()).getServerName();
            System.out.println(sn);
            ServerName snWithOldStartCode =
                    ServerName.valueOf(sn.getHostname(), sn.getPort(), sn.getStartcode() - 10);
            System.out.println(snWithOldStartCode);
            existing.put(regions.get(i), snWithOldStartCode);
        }
        List<ServerName> listOfServerNames = getListOfServerNames(servers);
        Map<ServerName, List<HRegionInfo>> assignment =
                loadBalancer.retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);

        // Include two new servers that were not there before
        List<ServerAndLoad> servers2 = new ArrayList<ServerAndLoad>(servers);
        servers2.add(randomServer("srv11"));
        servers2.add(randomServer("srv12"));
        listOfServerNames = getListOfServerNames(servers2);
        assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);

        // Remove two of the servers that were previously there
        List<ServerAndLoad> servers3 = new ArrayList<ServerAndLoad>(servers);
        servers3.remove(0);
        servers3.remove(0);
        listOfServerNames = getListOfServerNames(servers3);
        assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);
    }


    @Test (timeout=180000)
    public void testRetainAssignmentAndBalance() throws Exception {
        createSimpleRulesFile(Arrays.asList("srv[0-9] 100", "srv1[0-9] 1000"));
        // Test simple case where all same servers are there
        List<ServerAndLoad> servers = randomServers(10, 100);
        List<HRegionInfo> regions = randomRegions(1000);
        Map<HRegionInfo, ServerName> existing = new TreeMap<HRegionInfo, ServerName>();
        for (int i = 0; i < regions.size(); i++) {
            ServerName sn = servers.get(i % servers.size()).getServerName();
            ServerName snWithOldStartCode =
                ServerName.valueOf(sn.getHostname(), sn.getPort(), sn.getStartcode() - 10);
            existing.put(regions.get(i), snWithOldStartCode);
        }
        List<ServerName> listOfServerNames = getListOfServerNames(servers);
        listOfServerNames = listOfServerNames.subList(0, 5);
        listOfServerNames.add(randomServer("srv11").getServerName());
        listOfServerNames.add(randomServer("srv12").getServerName());
        Map<ServerName, List<HRegionInfo>> assignment =
                loadBalancer.retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);

        for (Map.Entry<ServerName, List<HRegionInfo>> entry: assignment.entrySet()) {
            ServerName serverName = entry.getKey();
            int limit = loadBalancer.findLimitForRS(serverName);
            Assert.assertTrue(entry.getValue().size() <= limit);
            switch (limit) {
                case 100:
                    Assert.assertEquals(100, entry.getValue().size());
                    break;
                case 1000:
                    Assert.assertEquals(250, entry.getValue().size());
                    break;

            }
        }

    }


}
