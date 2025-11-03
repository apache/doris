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

package org.apache.doris.plugin;

import org.apache.doris.plugin.dialect.HttpDialectUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

public class HttpDialectUtilsTest {

    private List<Integer> ports = new ArrayList<>();
    private List<SimpleHttpServer> servers = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        // Create three test servers
        for (int i = 0; i < 3; i++) {
            int port = findValidPort();
            ports.add(port);
            SimpleHttpServer server = new SimpleHttpServer(port);
            server.start("/api/v1/convert");
            servers.add(server);
        }
    }

    @After
    public void tearDown() {
        for (SimpleHttpServer server : servers) {
            if (server != null) {
                server.stop();
            }
        }
        servers.clear();
        ports.clear();
    }

    @Test
    public void testSingleUrlConvert() {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};
        String targetURL = "http://127.0.0.1:" + ports.get(0) + "/api/v1/convert";

        // Test with no response (should return original SQL)
        String res = HttpDialectUtils.convertSql(targetURL, originSql, "presto", features, "{}");
        Assert.assertEquals(originSql, res);

        // Test successful conversion
        servers.get(0).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        res = HttpDialectUtils.convertSql(targetURL, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);

        // Test version error
        servers.get(0).setResponse(
                "{\"version\": \"v2\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        res = HttpDialectUtils.convertSql(targetURL, originSql, "presto", features, "{}");
        Assert.assertEquals(originSql, res);

        // Test code error
        servers.get(0).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 400, \"message\": \"\"}");
        res = HttpDialectUtils.convertSql(targetURL, originSql, "presto", features, "{}");
        Assert.assertEquals(originSql, res);
    }

    @Test
    public void testMultipleUrlsConvert() {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};

        String targetURLs = "http://127.0.0.1:" + ports.get(0) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(1) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(2) + "/api/v1/convert";

        servers.get(0).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");

        String res = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);
    }

    @Test
    public void testFailoverMechanism() throws InterruptedException {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};

        String targetURLs = "http://127.0.0.1:" + ports.get(0) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(1) + "/api/v1/convert";

        // First server returns error, second server succeeds
        servers.get(0).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 500, \"message\": \"error\"}");
        servers.get(1).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");

        String res = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);
    }

    @Test
    public void testBlacklistMechanism() throws InterruptedException {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};

        String targetURLs = "http://127.0.0.1:" + ports.get(0) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(1) + "/api/v1/convert";

        // Stop first server, set second server to work
        servers.get(0).stop();
        servers.get(1).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");

        // First call should succeed via second server
        String res = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);

        // Restart first server
        servers.set(0, new SimpleHttpServer(ports.get(0)));
        try {
            servers.get(0).start("/api/v1/convert");
            servers.get(0).setResponse(
                    "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        } catch (IOException e) {
            return; // Skip test if port is occupied
        }

        // Should still work with blacklist recovery
        res = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);
    }

    @Test
    public void testAllUrlsFailure() {
        String originSql = "select * from t1 where \"k1\" = 1";
        String[] features = new String[] {"ctas"};

        String targetURLs = "http://127.0.0.1:" + ports.get(0) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(1) + "/api/v1/convert";

        // All servers return error
        servers.get(0).setResponse("{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");
        servers.get(1).setResponse("{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");

        String res = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
        Assert.assertEquals(originSql, res);
    }

    @Test
    public void testUrlParsing() {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};

        // Test URL parsing with spaces and empty items
        String targetURLs = " http://127.0.0.1:" + ports.get(0) + "/api/v1/convert , ,"
                + " http://127.0.0.1:" + ports.get(1) + "/api/v1/convert ";

        servers.get(0).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");

        String res = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);
    }

    @Test
    public void testSeamlessFailover() throws IOException {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};

        String targetURLs = "http://127.0.0.1:" + ports.get(0) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(1) + "/api/v1/convert";

        // Both servers start healthy
        servers.get(0).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        servers.get(1).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");

        String res = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);

        // Stop first server
        servers.get(0).stop();
        res = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);

        // Restart first server, stop second
        servers.set(0, new SimpleHttpServer(ports.get(0)));
        servers.get(0).start("/api/v1/convert");
        servers.get(0).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        servers.get(1).stop();

        // Should seamlessly switch to first server
        res = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
        Assert.assertEquals(expectedSql, res);
    }

    @Test
    public void testConcurrentRequests() throws InterruptedException {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};

        String targetURLs = "http://127.0.0.1:" + ports.get(0) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(1) + "/api/v1/convert";

        servers.get(0).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        servers.get(1).setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");

        // Test with multiple concurrent threads
        Thread[] threads = new Thread[10];
        String[] results = new String[10];

        for (int i = 0; i < 10; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                results[index] = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Verify all results
        for (String result : results) {
            Assert.assertEquals(expectedSql, result);
        }
    }

    @Test
    public void testZeroFailureGuarantee() throws InterruptedException {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};

        String targetURLs = "http://127.0.0.1:" + ports.get(0) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(1) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(2) + "/api/v1/convert";

        int totalRequests = 30; // Reduced for faster testing with production timeouts
        int successCount = 0;

        // Test various failure scenarios while ensuring at least one service is always available
        for (int i = 0; i < totalRequests; i++) {
            if (i < 6) {
                // All servers healthy
                setAllServersHealthy(expectedSql);
            } else if (i < 12) {
                // Server 0 fails, others healthy
                servers.get(0)
                        .setResponse("{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");
                servers.get(1).setResponse(
                        "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
                servers.get(2).setResponse(
                        "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
            } else if (i < 18) {
                // Servers 0,1 fail, server 2 healthy
                servers.get(0)
                        .setResponse("{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");
                servers.get(1)
                        .setResponse("{\"version\": \"v1\", \"data\": \"\", \"code\": 503, \"message\": \"error\"}");
                servers.get(2).setResponse(
                        "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
            } else if (i < 24) {
                // Only server 1 healthy
                servers.get(0)
                        .setResponse("{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");
                servers.get(1).setResponse(
                        "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
                servers.get(2)
                        .setResponse("{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");
            } else {
                // Alternating recovery
                if (i % 2 == 0) {
                    servers.get(0).setResponse(
                            "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
                    servers.get(1).setResponse(
                            "{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");
                    servers.get(2).setResponse(
                            "{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");
                } else {
                    servers.get(0).setResponse(
                            "{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");
                    servers.get(1).setResponse(
                            "{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"error\"}");
                    servers.get(2).setResponse(
                            "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
                }
            }

            String result = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
            if (expectedSql.equals(result)) {
                successCount++;
            }

            Thread.sleep(50); // Small delay between requests
        }

        System.out.println("Zero Failure Guarantee Test Results:");
        System.out.println("Total requests: " + totalRequests);
        System.out.println("Successful: " + successCount);
        System.out.println("Success rate: " + (successCount * 100.0 / totalRequests) + "%");

        // Must achieve 100% success rate when at least one service is available
        Assert.assertEquals("Must achieve 100% success rate when service is available",
                totalRequests, successCount);
    }

    @Test
    public void testNetworkJitterStress() throws InterruptedException {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";
        String[] features = new String[] {"ctas"};

        String targetURLs = "http://127.0.0.1:" + ports.get(0) + "/api/v1/convert,"
                + "http://127.0.0.1:" + ports.get(1) + "/api/v1/convert";

        int totalRequests = 15; // Reduced for faster testing with production timeouts
        int successCount = 0;

        // Simulate network jitter while ensuring at least one server is always available
        for (int i = 0; i < totalRequests; i++) {
            double random = Math.random();
            if (random < 0.3) {
                // Server 0 fails, Server 1 works
                servers.get(0)
                        .setResponse("{\"version\": \"v1\", \"data\": \"\", \"code\": 500, \"message\": \"timeout\"}");
                servers.get(1).setResponse(
                        "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
            } else if (random < 0.5) {
                // Server 1 fails, Server 0 works
                servers.get(0).setResponse(
                        "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
                servers.get(1).setResponse(
                        "{\"version\": \"v1\", \"data\": \"\", \"code\": 503, \"message\": \"service unavailable\"}");
            } else {
                // Both servers work
                servers.get(0).setResponse(
                        "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
                servers.get(1).setResponse(
                        "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
            }

            String result = HttpDialectUtils.convertSql(targetURLs, originSql, "presto", features, "{}");
            if (expectedSql.equals(result)) {
                successCount++;
            }

            Thread.sleep(100); // Delay between requests for production timeouts
        }

        System.out.println("Network Jitter Test Results:");
        System.out.println("Total requests: " + totalRequests);
        System.out.println("Successful: " + successCount);
        System.out.println("Success rate: " + (successCount * 100.0 / totalRequests) + "%");

        // Must achieve 100% success rate since we ensure at least one server is always available
        Assert.assertEquals("Must handle network jitter with 100% success when service is available",
                totalRequests, successCount);
    }

    private void setAllServersHealthy(String expectedSql) {
        for (int i = 0; i < 3; i++) {
            servers.get(i).setResponse(
                    "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        }
    }

    private static int findValidPort() {
        int port;
        while (true) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                port = socket.getLocalPort();
                try (DatagramSocket datagramSocket = new DatagramSocket(port)) {
                    datagramSocket.setReuseAddress(true);
                    break;
                } catch (SocketException e) {
                    System.out.println("The port " + port + " is invalid and try another port.");
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not find a free TCP/IP port to start HTTP Server on");
            }
        }
        return port;
    }
}
