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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
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
     * Default Ignite configuration without the {#TcpDiscoveryRedisIpFinder}.
     *
     * @param gridName Grid name.
     * @return Ignite configuration
     * @throws Exception If failed.
     */
    protected IgniteConfiguration getDefaultConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setGridName(gridName);
        cfg.setLocalHost("127.0.0.1");
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
        return startGrid(getConfiguration(gridName));
    }

    private Ignite startGrid(IgniteConfiguration cfg) throws Exception {
        Ignite node = IgnitionEx.start(cfg);
        log.info("Node started with the following configuration [id=" + node.cluster().localNode().id() + "]");
        return node;
    }

    /**
     * @param cnt Grid count.
     * @return First started grid.
     * @throws Exception If failed.
     */
    protected final Ignite startGrids(int cnt) throws Exception {
        assert cnt > 0;

        Ignite ignite = null;

        for (int i = 0; i < cnt; i++)
            if (ignite == null)
                ignite = startGrid(i);
            else
                startGrid(i);

        checkTopology(cnt);

        assert ignite != null;

        return ignite;
    }

    /**
     * @param cnt Grid count
     * @throws Exception If an error occurs.
     */
    private void checkTopology(int cnt) throws Exception {
        for (int j = 0; j < 10; j++) {
            boolean topOk = true;

            for (int i = 0; i < cnt; i++) {
                if (cnt != grid(i).cluster().nodes().size()) {
                    log.warn("Grid size is incorrect (will re-run check in 1000 ms) " +
                            "[name=" + grid(i).name() + ", size=" + grid(i).cluster().nodes().size() + ']');

                    topOk = false;

                    break;
                }
            }

            if (topOk)
                return;
            else
                Thread.sleep(1000);
        }

        throw new Exception("Failed to wait for proper topology: " + cnt);
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
     * @param ignite     Node.
     * @param joinEvtCnt Expected events number.
     * @return Events latch.
     */
    protected CountDownLatch expectJoinEvents(Ignite ignite, int joinEvtCnt) {
        final CountDownLatch latch = new CountDownLatch(joinEvtCnt);

        ignite.events().remoteListen(new IgniteBiPredicate<UUID, Event>() {
            @Override
            public boolean apply(UUID uuid, Event evt) {
                latch.countDown();
                return true;
            }
        }, null, EventType.EVT_NODE_JOINED);

        return latch;
    }

    /**
     * @param ignite Grid
     * @param cnt    Count
     * @throws IgniteCheckedException If failed.
     */
    protected void waitForRemoteNodes(Ignite ignite, int cnt) throws IgniteCheckedException {
        while (true) {
            Collection<ClusterNode> nodes = ignite.cluster().forRemotes().nodes();

            if (nodes != null && nodes.size() == cnt)
                return;

            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                throw new IgniteCheckedException("Interrupted while waiting for remote nodes [gridName=" + ignite.name() +
                        ", count=" + cnt + ']');
            }
        }
    }

}
