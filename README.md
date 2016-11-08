Apache Ignite Redis Module
------------------------------

[![Build Status](https://travis-ci.org/aalda/ignite-redis.svg)](https://travis-ci.org/aalda/ignite-redis)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.aalda/ignite-redis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.aalda/ignite-redis)

Apache Ignite Redis module provides a TCP Discovery IP Finder that uses a Redis
set to locate other Ignite nodes to connect to.

Importing Apache Ignite Redis Module In Maven Project
---------------------------------------------------------

If you are using Maven to manage dependencies of your project, you can add the Redis
module dependency like this (replace '${ignite-redis.version}' with actual Ignite version you
are interested in):

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
            <version>${ignite-redis.version}</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
```

Currently does not support Redis clusters