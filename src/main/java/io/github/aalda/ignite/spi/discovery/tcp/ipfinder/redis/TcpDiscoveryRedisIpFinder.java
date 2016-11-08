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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class TcpDiscoveryRedisIpFinder extends TcpDiscoveryIpFinderAdapter {

    /**
     * System property name to provide the Redis Connection String.
     */
    public static final String PROP_REDIS_CONNECTION_STRING = "IGNITE_REDIS_CONNECTION_STRING";

    /**
     * Delimiter to use in Redis set names.
     */
    private static final String DELIM = "#";

    /**
     * Default service name for service registrations.
     */
    private static final String SERVICE_NAME = "ignite";

    /**
     * Init guard.
     */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /**
     * Init guard.
     */
    @GridToStringExclude
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /**
     * Logger.
     */
    @LoggerResource
    private IgniteLogger log;

    /**
     * The Redis Connection String if provided by the user.
     */
    private String redisConnectionString;

    /**
     * Service name to use, by default {#link #SERVICE_NAME}.
     */
    private String serviceName = SERVICE_NAME;

    /**
     * Constructor.
     */
    public TcpDiscoveryRedisIpFinder() {
        setShared(true);
    }

    /**
     * {@inheritDoc}
     */
    public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        init();

        if (log.isDebugEnabled())
            log.debug("Getting registered addresses from Redis IP Finder.");

        Collection<String> addresses;

        try(Jedis jedis = new Jedis("redis://" + redisConnectionString)) {
            addresses = jedis.smembers(addressesSetKey());
        } catch (JedisException e) {
            log.warning("Error while getting registered addresses from Redis IP Finder.", e);
            return Collections.emptyList();
        }

        Set<InetSocketAddress> answer = new HashSet<>();
        for (String address : addresses) {
            answer.add(deserializeAddress(address));
        }

        if (log.isInfoEnabled())
            log.info("Redis IP Finder resolved addresses: " + answer);

        return answer;
    }

    /**
     * {@inheritDoc}
     */
    public void registerAddresses(Collection<InetSocketAddress> addresses) throws IgniteSpiException {
        init();

        if (log.isInfoEnabled())
            log.info("Registering addresses with Redis IP Finder: " + addresses);

        Collection<String> serializedAddresses = new HashSet<>();
        for (InetSocketAddress address : addresses) {
            serializedAddresses.add(serializeAddress(address));
        }

        try(Jedis jedis = new Jedis("redis://" + redisConnectionString)) {
            String[] sAddresses = serializedAddresses.toArray(new String[serializedAddresses.size()]);
            jedis.sadd(addressesSetKey(), sAddresses);
        } catch (JedisException e) {
            log.warning(String.format("Error while registering an address from Redis IP Finder " +
                    "[message=%s,addresses=%s]", e.getMessage(), addresses), e);
        }

    }

    /**
     * {@inheritDoc}
     */
    public void unregisterAddresses(Collection<InetSocketAddress> addresses) throws IgniteSpiException {

        if (log.isInfoEnabled())
            log.info("Unregistering addresses with Redis IP Finder: {}" + addresses);

        try(Jedis jedis = new Jedis("redis://" + redisConnectionString)) {

            String key = addressesSetKey();

            for (InetSocketAddress address : addresses) {

                String serializedAddress = serializeAddress(address);

                if (!jedis.sismember(key, serializedAddress)) {
                    log.warning("Asked to unregister address from Redis IP Finder, but no match was found in local " +
                            "instance map for: " + address);
                } else {
                    try {
                        jedis.srem(key, serializedAddress);
                    } catch (JedisException e) {
                        log.warning("Error while unregistering an address from Redis IP Finder: " + address, e);
                    }
                }
            }

        }

    }

    @Override
    public void onSpiContextDestroyed() {
        if (!closeGuard.compareAndSet(false, true)) {
            U.warn(log, "Redis IP Finder can't be closed more than once.");
            return;
        }

        if (log.isInfoEnabled())
            log.info("Destroying Redis IP Finder.");

        super.onSpiContextDestroyed();

    }

    /**
     * @param redisConnectionString Redis connection string in case a {@link JedisPool} is not being set
     *                              explicitly.
     */
    public void setRedisConnectionString(String redisConnectionString) {
        this.redisConnectionString = redisConnectionString;
    }

    /**
     * @return Service name being used. See {@link #setServiceName(String)} for more information.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * @param serviceName Service name to use. In physical Redis terms, it represents
     *                    the key of the set under which services will be registered.
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    private String addressesSetKey() {
        return String.format("%s/addresses", this.serviceName);
    }

    /**
     * Initializes this IP Finder by creating the appropriate Jedis objects.
     *
     * @throws org.apache.ignite.spi.IgniteSpiException If any error occurs.
     */
    private void init() throws IgniteSpiException {

        if (!initGuard.compareAndSet(false, true))
            return;

        String sysPropRedisConnString = System.getProperty(PROP_REDIS_CONNECTION_STRING);

        if (sysPropRedisConnString != null && sysPropRedisConnString.trim().length() > 0)
            redisConnectionString = sysPropRedisConnString;

        if (log.isInfoEnabled())
            log.info("Initializing Redis IP Finder.");

    }

    /**
     * Gets Redis value for provided address.
     *
     * @param address Node address.
     * @return Value.
     */
    private String serializeAddress(InetSocketAddress address) {
        assert address != null;

        SB sb = new SB();

        sb.a(address.getAddress().getHostAddress())
                .a(DELIM)
                .a(address.getPort());

        return sb.toString();
    }

    /**
     * Gets address for provided Redis value.
     *
     * @param address Value.
     * @return Node address.
     */
    private InetSocketAddress deserializeAddress(String address) {
        assert address != null;

        String[] splitParts = address.split(DELIM);
        return new InetSocketAddress(splitParts[0], Integer.parseInt(splitParts[1]));
    }
}
