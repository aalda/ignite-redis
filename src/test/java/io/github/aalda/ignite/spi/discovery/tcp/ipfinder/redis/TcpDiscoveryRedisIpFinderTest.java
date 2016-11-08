/**
 * Copyright © 2016 Alvaro Alda
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.aalda.ignite.spi.discovery.tcp.ipfinder.redis;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test for {@link TcpDiscoveryRedisIpFinder}.
 */
public class TcpDiscoveryRedisIpFinderTest extends TcpDiscoveryIpFinderBaseTest {

    private RedisServer redisServer;

    private final static int REDIS_PORT = 16379;

    private final static String REDIS_SERVICE_NAME = "ignite-test";

    /**
     * Before test.
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // remove stale system properties
        System.getProperties().remove(TcpDiscoveryRedisIpFinder.PROP_REDIS_CONNECTION_STRING);

        log.info("Starting Redis server...");
        redisServer = new RedisServer(REDIS_PORT);
        redisServer.start();
        log.info("Redis Server listening at [ localhost:{} ]", REDIS_PORT);
    }

    /**
     * After test
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();
        if (redisServer.isActive()) {
            log.info("Stopping Redis server...");
            redisServer.stop();
            log.info("Redis server stopped");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldRegisterOneNode() throws Exception {
        startGrid(0);

        assertThat(grid(0).cluster().metrics().getTotalNodes(), equalTo(1));

        // check that the node is registered in Redis
        assertThat(registeredAddresses().size(), equalTo(1));

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldRegisterTwoNodesWithTwoDifferentConfigMethods() throws Exception {
        startGrid(0);

        // set up an event listener to expect one NODE_JOINED event
        CountDownLatch latch = expectJoinEvents(grid(0), 1);

        // start the other node
        System.setProperty(TcpDiscoveryRedisIpFinder.PROP_REDIS_CONNECTION_STRING, String.format("localhost:%d", REDIS_PORT));
        startGrid(1);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 1);

        // assert the nodes see each other
        assertThat(grid(0).cluster().metrics().getTotalNodes(), equalTo(2));
        assertThat(grid(1).cluster().metrics().getTotalNodes(), equalTo(2));

        // check that the nodes are registered in Redis
        assertThat(registeredAddresses().size(), equalTo(2));

        // assert the event listener got as many events as expected
        latch.await(1, TimeUnit.SECONDS);

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFourNodesStartingAndStopping() throws Exception {
        // start one node
        startGrid(0);

        // set up an event listener to expect one NODE_JOINED event
        CountDownLatch latch = expectJoinEvents(grid(0), 3);

        // start the 2nd, 3rd & 4th nodes, first setting the system property
        System.setProperty(TcpDiscoveryRedisIpFinder.PROP_REDIS_CONNECTION_STRING, String.format("localhost:%d", REDIS_PORT));
        startGrid(1);
        startGrid(2);
        startGrid(3);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // assert the nodes see each other
        assertThat(grid(0).cluster().metrics().getTotalNodes(), equalTo(4));
        assertThat(grid(1).cluster().metrics().getTotalNodes(), equalTo(4));
        assertThat(grid(2).cluster().metrics().getTotalNodes(), equalTo(4));
        assertThat(grid(3).cluster().metrics().getTotalNodes(), equalTo(4));

        // assert the event listener got as many events as expected
        latch.await(1, TimeUnit.SECONDS);

        // stop the first grid
        stopGrid(0);

        // make sure that nodes were synchronized; they should only see 3 now
        assertThat(grid(1).cluster().metrics().getTotalNodes(), equalTo(3));
        assertThat(grid(2).cluster().metrics().getTotalNodes(), equalTo(3));
        assertThat(grid(3).cluster().metrics().getTotalNodes(), equalTo(3));

        // stop two remaining grids
        stopGrid(1);
        stopGrid(2);

        // Let the remaining node to unregister addresses
        Thread.sleep(1000);

        // stop the the last remaining grid
        stopGrid(3);

        // check that the nodes are gone in Redis
        assertThat(registeredAddresses().size(), equalTo(0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFourNodesRestartLastSeveralTimes() throws Exception {

        // start 4 nodes
        System.setProperty(TcpDiscoveryRedisIpFinder.PROP_REDIS_CONNECTION_STRING, String.format("localhost:%d", REDIS_PORT));
        startGrids(4);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // each node will only register itself
        assertThat(registeredAddresses().size(), equalTo(4));

        // repeat 5 times
        for (int i = 0; i < 5; i++) {
            // stop last grid
            stopGrid(2);

            // Let the remaining nodes to unregister addresses
            Thread.sleep(1000);

            // check that the node has unregistered itself and its party
            assertThat(registeredAddresses().size(), equalTo(3));

            // start the node again
            startGrid(2);

            // check that the node back in ZK
            assertThat(registeredAddresses().size(), equalTo(4));
        }

        // stop three grids
        stopGrid(0);
        stopGrid(1);
        stopGrid(2);

        // Let the remaining node to unregister addresses
        Thread.sleep(1000);

        // stop the the last remaining grid
        stopGrid(3);

        // check that the nodes are gone in Redis
        assertThat(registeredAddresses().size(), equalTo(0));
    }

    /**
     * Enhances the default configuration with the {#TcpDiscoveryRedisIpFinder}.
     *
     * @param gridName Grid name.
     * @return Ignite configuration.
     * @throws Exception If failed.
     */
    @Override
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = getDefaultConfiguration(gridName);

        TcpDiscoveryRedisIpFinder redisIpFinder = new TcpDiscoveryRedisIpFinder();
        redisIpFinder.setServiceName(REDIS_SERVICE_NAME);

        // first node => configure with redisConnectionString; second and subsequent
        // shall be configured through system property
        if (gridName.equals(getTestGridName(0)))
            redisIpFinder.setRedisConnectionString("localhost:16379");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        // Increase max missed heartbeats to avoid unexpected node fails.
        discoSpi.setMaxMissedHeartbeats(30);
        // Set heartbeat interval to 1 second to speed up tests.
        discoSpi.setHeartbeatFrequency(1000);
        // Set clean frequency to 500 milliseconds to speed up address unregistering
        discoSpi.setIpFinderCleanFrequency(500);
        discoSpi.setIpFinder(redisIpFinder);

        configuration.setDiscoverySpi(discoSpi);
        return configuration;
    }

    private Collection<String> registeredAddresses() throws Exception {
        Collection<String> addresses = new HashSet<>();
        try(Jedis jedis = new Jedis("localhost", REDIS_PORT)) {
            for (String key : jedis.keys(REDIS_SERVICE_NAME + "*")) {
                addresses.addAll(jedis.smembers(key));
            }
        }
        return addresses;
    }
}
