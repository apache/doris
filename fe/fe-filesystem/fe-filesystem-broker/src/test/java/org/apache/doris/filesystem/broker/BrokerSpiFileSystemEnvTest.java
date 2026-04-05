// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.filesystem.broker;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Environment-dependent integration tests for {@link BrokerSpiFileSystem}.
 * Requires a running Apache Doris Broker process.
 */
@Tag("environment")
@Tag("broker")
@EnabledIfEnvironmentVariable(named = "DORIS_FS_TEST_BROKER_HOST", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BrokerSpiFileSystemEnvTest {

    private static final String PREFIX = "/doris-broker-ut-" + UUID.randomUUID();
    private static BrokerSpiFileSystem fs;

    @BeforeAll
    static void setUp() throws IOException {
        String host = requireEnv("DORIS_FS_TEST_BROKER_HOST");
        int port = Integer.parseInt(requireEnv("DORIS_FS_TEST_BROKER_PORT"));

        Map<String, String> params = new HashMap<>();
        // Broker may need HDFS or other backend storage params; pass through env
        String hdfsHost = System.getenv("DORIS_FS_TEST_HDFS_HOST");
        String hdfsPort = System.getenv("DORIS_FS_TEST_HDFS_PORT");
        if (hdfsHost != null && hdfsPort != null) {
            params.put("fs.defaultFS", "hdfs://" + hdfsHost + ":" + hdfsPort);
        }

        Map<String, String> brokerProps = new HashMap<>();
        brokerProps.put("BROKER_HOST", host);
        brokerProps.put("BROKER_PORT", String.valueOf(port));
        brokerProps.put("BROKER_CLIENT_ID", "env-test-client");

        BrokerFileSystemProvider provider = new BrokerFileSystemProvider();
        brokerProps.put("_STORAGE_TYPE_", "BROKER");
        brokerProps.putAll(params);
        fs = (BrokerSpiFileSystem) provider.create(brokerProps);
    }

    @AfterAll
    static void tearDown() throws IOException {
        try {
            fs.delete(Location.of(PREFIX), true);
        } catch (IOException ignored) {
            // best-effort
        }
        fs.close();
    }

    private static String requireEnv(String name) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) {
            throw new IllegalStateException("Missing required env var: " + name);
        }
        return val;
    }

    private static Location loc(String suffix) {
        return Location.of(PREFIX + "/" + suffix);
    }

    // ------------------------------------------------------------------
    // exists
    // ------------------------------------------------------------------

    @Test
    @Order(1)
    void existsReturnsFalseForMissing() throws IOException {
        Assertions.assertFalse(fs.exists(loc("no-such-file-" + UUID.randomUUID())));
    }

    // ------------------------------------------------------------------
    // write + read
    // ------------------------------------------------------------------

    @Test
    @Order(2)
    void writeAndRead() throws IOException {
        byte[] expected = "broker-env-test".getBytes();
        DorisOutputFile out = fs.newOutputFile(loc("rw-test.txt"));
        try (OutputStream os = out.createOrOverwrite()) {
            os.write(expected);
        }

        DorisInputFile in = fs.newInputFile(loc("rw-test.txt"));
        try (DorisInputStream is = in.newStream()) {
            byte[] actual = is.readAllBytes();
            Assertions.assertArrayEquals(expected, actual);
        }
    }

    // ------------------------------------------------------------------
    // delete
    // ------------------------------------------------------------------

    @Test
    @Order(3)
    void deleteRemovesFile() throws IOException {
        DorisOutputFile out = fs.newOutputFile(loc("to-delete.txt"));
        try (OutputStream os = out.createOrOverwrite()) {
            os.write("del".getBytes());
        }
        Assertions.assertTrue(fs.exists(loc("to-delete.txt")));

        fs.delete(loc("to-delete.txt"), false);
        Assertions.assertFalse(fs.exists(loc("to-delete.txt")));
    }

    // ------------------------------------------------------------------
    // list
    // ------------------------------------------------------------------

    @Test
    @Order(4)
    void listReturnsFiles() throws IOException {
        DorisOutputFile out1 = fs.newOutputFile(loc("list/a.txt"));
        try (OutputStream os = out1.createOrOverwrite()) {
            os.write("a".getBytes());
        }
        DorisOutputFile out2 = fs.newOutputFile(loc("list/b.txt"));
        try (OutputStream os = out2.createOrOverwrite()) {
            os.write("b".getBytes());
        }

        List<FileEntry> entries = new ArrayList<>();
        try (FileIterator iter = fs.list(loc("list"))) {
            while (iter.hasNext()) {
                entries.add(iter.next());
            }
        }
        Assertions.assertTrue(entries.size() >= 2,
                "Expected at least 2 entries, got " + entries.size());
    }
}
