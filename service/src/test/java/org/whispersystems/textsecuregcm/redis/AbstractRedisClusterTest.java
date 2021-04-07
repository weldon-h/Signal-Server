/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;
import redis.embedded.RedisServer;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assume.assumeFalse;

/**
 * An abstract base class that assembles a real (local!) Redis cluster and provides a client to that cluster for
 * subclasses.
 */
public abstract class AbstractRedisClusterTest {

    private static final int NODE_COUNT = 2;

    private static RedisServer[] clusterNodes;

    private FaultTolerantRedisCluster redisCluster;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        assumeFalse(System.getProperty("os.name").equalsIgnoreCase("windows"));

        clusterNodes = new RedisServer[NODE_COUNT];

        for (int i = 0; i < NODE_COUNT; i++) {
            clusterNodes[i] = buildClusterNode(getNextRedisClusterPort());
            clusterNodes[i].start();
        }

        assembleCluster(clusterNodes);
    }

    @Before
    public void setUp() throws Exception {
        final List<String> urls = Arrays.stream(clusterNodes)
                                        .map(node -> String.format("redis://127.0.0.1:%d", node.ports().get(0)))
                                        .collect(Collectors.toList());

        redisCluster = new FaultTolerantRedisCluster("test-cluster",
                                                     RedisClusterClient.create(urls.stream().map(RedisURI::create).collect(Collectors.toList())),
                                                     Duration.ofSeconds(2),
                                                     new CircuitBreakerConfiguration(),
                                                     new RetryConfiguration());

        redisCluster.useCluster(connection -> {
            boolean setAll = false;

            final String[] keys = new String[NODE_COUNT];

            for (int i = 0; i < keys.length; i++) {
                keys[i] = RedisClusterUtil.getMinimalHashTag(i * SlotHash.SLOT_COUNT / keys.length);
            }

            while (!setAll) {
                try {
                    for (final String key : keys) {
                        connection.sync().set(key, "warmup");
                    }

                    setAll = true;
                } catch (final RedisException ignored) {
                    // Cluster isn't ready; wait and retry.
                    try {
                        Thread.sleep(500);
                    } catch (final InterruptedException ignored2) {
                    }
                }
            }
        });

        redisCluster.useCluster(connection -> connection.sync().flushall());
    }

    protected FaultTolerantRedisCluster getRedisCluster() {
        return redisCluster;
    }

    @After
    public void tearDown() throws Exception {
        redisCluster.shutdown();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        for (final RedisServer node : clusterNodes) {
            node.stop();
        }
    }

    private static RedisServer buildClusterNode(final int port) throws IOException {
        final File clusterConfigFile = File.createTempFile("redis", ".conf");
        clusterConfigFile.deleteOnExit();

        return RedisServer.builder()
                .setting("cluster-enabled yes")
                .setting("cluster-config-file " + clusterConfigFile.getAbsolutePath())
                .setting("cluster-node-timeout 5000")
                .setting("appendonly no")
                .setting("dir " + System.getProperty("java.io.tmpdir"))
                .port(port)
                .build();
    }

    private static void assembleCluster(final RedisServer... nodes) throws InterruptedException {
        final RedisClient meetClient = RedisClient.create(RedisURI.create("127.0.0.1", nodes[0].ports().get(0)));

        try {
            final StatefulRedisConnection<String, String> connection = meetClient.connect();
            final RedisCommands<String, String>           commands   = connection.sync();

            for (int i = 1; i < nodes.length; i++) {
                commands.clusterMeet("127.0.0.1", nodes[i].ports().get(0));
            }
        } finally {
            meetClient.shutdown();
        }

        final int slotsPerNode = SlotHash.SLOT_COUNT / nodes.length;

        for (int i = 0; i < nodes.length; i++) {
            final int startInclusive = i * slotsPerNode;
            final int endExclusive   = i == nodes.length - 1 ? SlotHash.SLOT_COUNT : (i + 1) * slotsPerNode;

            final RedisClient assignSlotClient = RedisClient.create(RedisURI.create("127.0.0.1", nodes[i].ports().get(0)));

            try (final StatefulRedisConnection<String, String> assignSlotConnection = assignSlotClient.connect()) {
                final int[] slots = new int[endExclusive - startInclusive];

                for (int s = startInclusive; s < endExclusive; s++) {
                    slots[s - startInclusive] = s;
                }

                assignSlotConnection.sync().clusterAddSlots(slots);
            } finally {
                assignSlotClient.shutdown();
            }
        }

        final RedisClient waitClient = RedisClient.create(RedisURI.create("127.0.0.1", nodes[0].ports().get(0)));

        try (final StatefulRedisConnection<String, String> connection = waitClient.connect()) {
            // CLUSTER INFO gives us a big blob of key-value pairs, but the one we're interested in is `cluster_state`.
            // According to https://redis.io/commands/cluster-info, `cluster_state:ok` means that the node is ready to
            // receive queries, all slots are assigned, and a majority of master nodes are reachable.
            while (!connection.sync().clusterInfo().contains("cluster_state:ok")) {
                Thread.sleep(500);
            }
        } finally {
            waitClient.shutdown();
        }
    }

    public static int getNextRedisClusterPort() throws IOException {
        final int MAX_ITERATIONS = 11_000;
        int port;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(false);
                port = socket.getLocalPort();
            }
            if (port < 55535) {
                return port;
            }
        }
        throw new IOException("Couldn't find an open port below 55,535 in " + MAX_ITERATIONS + " tries");
    }
}
