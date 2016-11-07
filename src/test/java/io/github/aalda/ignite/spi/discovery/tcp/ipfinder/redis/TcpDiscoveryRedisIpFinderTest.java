package io.github.aalda.ignite.spi.discovery.tcp.ipfinder.redis;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisServer;

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

    @Before
    public void setUp() throws Exception {
        // remove stale system properties
        System.getProperties().remove(TcpDiscoveryRedisIpFinder.PROP_REDIS_CONNECTION_STRING);

        log.info("Starting Redis server...");
        redisServer = new RedisServer(REDIS_PORT);
        redisServer.start();
        log.info("Redis Server listening at [ localhost:{} ]", REDIS_PORT);
    }

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
    public void testOneIgniteNodeIsAlone() throws Exception {
        startGrid(0);

        assertThat(grid(0).cluster().metrics().getTotalNodes(), equalTo(1));

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTwoIgniteNodesFindEachOther() throws Exception {
        startGrid(0);

        // set up an event listener to expect one NODE_JOINED event
        CountDownLatch latch = expectJoinEvents(grid(0), 1);

        // start the other node
        System.setProperty(TcpDiscoveryRedisIpFinder.PROP_REDIS_CONNECTION_STRING, String.format("localhost:%d", REDIS_PORT));
        startGrid(1);

        // assert the event listener got as many events as expected
        latch.await(1, TimeUnit.SECONDS);

        // assert the nodes see each other
        assertThat(grid(0).cluster().metrics().getTotalNodes(), equalTo(2));
        assertThat(grid(1).cluster().metrics().getTotalNodes(), equalTo(2));

        stopAllGrids();
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

        // first node => configure with redisConnectionString; second and subsequent
        // shall be configured through system property
        if (gridName.equals(getTestGridName(0)))
            redisIpFinder.setRedisConnectionString("localhost:16379");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        // Increase max missed heartbeats to avoid unexpected node fails.
        discoSpi.setMaxMissedHeartbeats(30);
        // Set heartbeat interval to 1 second to speed up tests.
        discoSpi.setHeartbeatFrequency(1000);

        discoSpi.setIpFinder(redisIpFinder);

        configuration.setDiscoverySpi(discoSpi);
        return configuration;
    }

}
