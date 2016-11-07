package io.github.aalda.ignite.spi.discovery.tcp.ipfinder.redis;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public abstract class TcpDiscoveryIpFinderBaseTest {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Starting grid name.
     */
    private static final ThreadLocal<String> startingGrid = new ThreadLocal<>();

    /**
     * Default Ignite configuration without the {#TcpDiscoveryRedisIpFinder}.
     *
     * @param gridName Grid name.
     * @return Ignite configuration
     * @throws Exception If failed.
     */
    protected IgniteConfiguration getDefaultConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setGridName(gridName);
        cfg.setGridLogger(new Slf4jLogger());
        cfg.setNetworkTimeout(10000);
        return cfg;
    }

    /**
     * This method should be overridden by subclasses to change configuration parameters.
     *
     * @param gridName Grid name.
     * @return Grid configuration used for starting of grid.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return getDefaultConfiguration(gridName);
    }

    protected IgniteEx startGrid(int idx) throws Exception {
        return (IgniteEx) startGrid(getTestGridName(idx));
    }

    private Ignite startGrid(String gridName) throws Exception {
        return startGrid(gridName, getConfiguration(gridName));
    }

    private Ignite startGrid(String gridName, IgniteConfiguration cfg) throws Exception {
        startingGrid.set(gridName);
        try {
            Ignite node = IgnitionEx.start(cfg);
            log.info("Node started with the following configuration [id=" + node.cluster().localNode().id() + "]");
            return node;
        } finally {
            startingGrid.set(null);
        }
    }

    /**
     * @param idx Index of the grid to stop.
     */
    protected void stopGrid(int idx) {
        stopGrid(getTestGridName(idx), false);
    }

    /**
     * @param gridName Grid name.
     * @param cancel   Cancel flag.
     */
    private void stopGrid(@Nullable String gridName, boolean cancel) {
        try {
            Ignite ignite = grid(gridName);
            assert ignite != null : "Ignite returned null grid for name: " + gridName;
            log.info(">>> Stopping grid [name=" + ignite.name() + ", id=" +
                    ((IgniteKernal) ignite).context().localNodeId() + ']');
            G.stop(gridName, cancel);
        } catch (IllegalStateException ignored) {
            // Ignore error if grid already stopped.
        } catch (Throwable e) {
            log.error("Failed to stop grid [gridName=" + gridName + ", cancel=" + cancel + ']', e);
        }
    }

    /**
     *
     */
    protected void stopAllGrids() {
        stopAllGrids(true);
    }

    /**
     * @param cancel Cancel flag.
     */
    protected void stopAllGrids(boolean cancel) {
        Collection<Ignite> clients = new ArrayList<>();
        Collection<Ignite> srvs = new ArrayList<>();

        for (Ignite g : G.allGrids()) {
            if (g.configuration().getDiscoverySpi().isClientMode())
                clients.add(g);
            else
                srvs.add(g);
        }

        for (Ignite g : clients)
            stopGrid(g.name(), cancel);

        for (Ignite g : srvs)
            stopGrid(g.name(), cancel);

        assert G.allGrids().isEmpty();
    }

    /**
     * @return Generated unique test grid name.
     */
    private String getTestGridName() {
        String[] parts = getClass().getName().split("\\.");
        return parts[parts.length - 2] + '.' + parts[parts.length - 1];
    }

    /**
     * @param idx Index of the grid.
     * @return Indexed grid name.
     */
    protected String getTestGridName(int idx) {
        return getTestGridName() + idx;
    }

    /**
     * Gets grid for given name.
     *
     * @param name Name.
     * @return Grid instance.
     */
    private IgniteEx grid(String name) {
        return (IgniteEx) G.ignite(name);
    }

    /**
     * Gets grid for given index.
     *
     * @param idx Index.
     * @return Grid instance.
     */
    protected IgniteEx grid(int idx) {
        return grid(getTestGridName(idx));
    }

    /**
     * @param ignite Node.
     * @param joinEvtCnt Expected events number.
     * @return Events latch.
     */
    protected CountDownLatch expectJoinEvents(Ignite ignite, int joinEvtCnt) {
        final CountDownLatch latch = new CountDownLatch(joinEvtCnt);

        ignite.events().remoteListen(new IgniteBiPredicate<UUID, Event>() {
            @Override public boolean apply(UUID uuid, Event evt) {
                latch.countDown();
                return true;
            }
        }, null, EventType.EVT_NODE_JOINED);

        return latch;
    }

}
