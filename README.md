# Apache Ignite Redis Module #

[![Build Status](https://travis-ci.org/aalda/ignite-redis.svg)](https://travis-ci.org/aalda/ignite-redis)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.aalda/ignite-redis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.aalda/ignite-redis)
[![Javadocs](http://javadoc.io/badge/io.github.aalda/ignite-redis.svg)](http://javadoc.io/doc/io.github.aalda/ignite-redis)

Apache Ignite Redis module provides a TCP Discovery IP Finder that uses a Redis
set to locate other Ignite nodes to connect to.

Currently it does not support Redis clusters.

## Importing Apache Ignite Redis Module In Maven Project ##

If you are using Maven to manage dependencies of your project, you can add the latest version of the Redis
module dependency like this:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                        http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...
    <dependencies>
        ...
        <dependency>
            <groupId>io.github.aalda</groupId>
            <artifactId>ignite-redis</artifactId>
            <version>0.1.0</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
```

## Configuration ##

In Ignite, nodes can discover each other by using DiscoverySpi. Ignite provides TcpDiscoverySpi
as a default implementation of DiscoverySpi that uses TCP/IP for node discovery. For more
information, please refer to the official Apache Ignite [Cluster Configuration](https://apacheignite.readme.io/docs/cluster-config)
documentation.

If you're using Redis to coordinate your distributed environment, you can utilize it for a
common shared storage of initial IP addresses. This is done via TcpDiscoveryRedisIpFinder.

Here is an example of how to configure this finder via Spring XML file:

```xml
<bean class="org.apache.ignite.configuration.IgniteConfiguration">
    ...
    <property name="discoverySpi">
        <bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">
            <property name="ipFinder">
                 <bean class="io.github.aalda.ignite.spi.discovery.tcp.ipfinder.redis.TcpDiscoveryRedisIpFinder">
                    <property name="redisConnectionString" value="localhost:6379"/>
                    <property name="serviceName" value="myService" />
                 </bean>
            </property>
        </bean>
    </property>
    ...
</bean>
```

Or programmatically from Java:

```java
TcpDiscoverySpi spi = new TcpDiscoverySpi();

TcpDiscoveryRedisIpFinder ipFinder = new TcpDiscoveryRedisIpFinder();

// Specify Redis connection string.
ipFinder.setRedisConnectionString("127.0.0.1:6379");
ipFinder.serServiceName("myService");

spi.setIpFinder(ipFinder);

IgniteConfiguration cfg = new IgniteConfiguration();

// Override default discovery SPI.
cfg.setDiscoverySpi(spi);

// Start Ignite node.
Ignition.start(cfg);
```

## How it works ##

Once you have configured the Redis IP Finder, every Ignite node will be able to register
its own address in a Set stored in Redis. Such address will follow the pattern _"host#port"_.
The format of the key that points to that set will be the concatenation of the service name
established through the *serviceName* property and the suffix _"/addresses"_.

When a node is stopped, it does not unregister itself and remove its address from the
Redis set. The coordinator node is responsible for that operation. It frequently runs a clean
process that detects left nodes and remove their addresses from the Redis set. You can tune
the frequency of this process changing the _ipFinderCleanFreq_ property in the
configuration of the TcpDiscoverySpi. By default, it takes the value of 60 seconds.

Note that all nodes are stopped before the clean process is executed, the last node will only
be able to unregister its own address but not the remaining ones. So such addresses will remain
in the Redis set until a new one node reuse the same ip/port combination. Fortunately, an
unreachable IP address will not affect the discovery process.

## License ##

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).