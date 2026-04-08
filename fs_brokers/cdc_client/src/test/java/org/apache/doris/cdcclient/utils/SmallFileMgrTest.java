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

package org.apache.doris.cdcclient.utils;

import com.sun.net.httpserver.HttpServer;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SmallFileMgrTest {

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        SmallFileMgr.clearCache();
    }

    @AfterEach
    void tearDown() {
        SmallFileMgr.clearCache();
    }

    // -------------------------------------------------------------------------
    // Input validation
    // -------------------------------------------------------------------------

    @Test
    void testInvalidPrefix() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SmallFileMgr.getFilePath(
                        "host:8030", "NOFILE:123:abc", "token", tempDir.toString()));
    }

    @Test
    void testInvalidFormatMissingMd5() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SmallFileMgr.getFilePath(
                        "host:8030", "FILE:123", "token", tempDir.toString()));
    }

    @Test
    void testInvalidFormatTooManyParts() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SmallFileMgr.getFilePath(
                        "host:8030", "FILE:123:abc:extra", "token", tempDir.toString()));
    }

    // -------------------------------------------------------------------------
    // Disk cache hit
    // -------------------------------------------------------------------------

    @Test
    void testDiskCacheHit() throws Exception {
        byte[] content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----".getBytes();
        String md5 = DigestUtils.md5Hex(content);
        String fileId = "10001";

        // Pre-populate disk cache
        File cachedFile = tempDir.resolve(fileId + "." + md5).toFile();
        Files.write(cachedFile.toPath(), content);

        String result = SmallFileMgr.getFilePath(
                "host:8030", "FILE:" + fileId + ":" + md5, "token", tempDir.toString());

        assertEquals(cachedFile.getAbsolutePath(), result);
    }

    @Test
    void testDiskCacheMd5MismatchTriggersRedownload() throws Exception {
        byte[] correctContent = "correct cert content".getBytes();
        String correctMd5 = DigestUtils.md5Hex(correctContent);
        String fileId = "10002";

        // Write a file with the correct name but corrupted content
        File corruptFile = tempDir.resolve(fileId + "." + correctMd5).toFile();
        Files.write(corruptFile.toPath(), "corrupted content".getBytes());

        // Start a mock FE HTTP server that serves the correct content
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        int port = server.getAddress().getPort();
        server.createContext(
                "/api/get_small_file",
                exchange -> {
                    exchange.sendResponseHeaders(200, correctContent.length);
                    exchange.getResponseBody().write(correctContent);
                    exchange.getResponseBody().close();
                });
        server.start();

        try {
            String result = SmallFileMgr.getFilePath(
                    "127.0.0.1:" + port,
                    "FILE:" + fileId + ":" + correctMd5,
                    "test-token",
                    tempDir.toString());

            assertEquals(corruptFile.getAbsolutePath(), result);
            // Verify the file now contains the correct content
            byte[] written = Files.readAllBytes(corruptFile.toPath());
            assertEquals(correctMd5, DigestUtils.md5Hex(written));
        } finally {
            server.stop(0);
        }
    }

    // -------------------------------------------------------------------------
    // In-memory cache hit
    // -------------------------------------------------------------------------

    @Test
    void testMemoryCacheHitSkipsDiskIo() throws Exception {
        byte[] content = "ssl cert data".getBytes();
        String md5 = DigestUtils.md5Hex(content);
        String fileId = "10003";

        // Populate memory cache via a disk-cache hit on the first call
        File cachedFile = tempDir.resolve(fileId + "." + md5).toFile();
        Files.write(cachedFile.toPath(), content);
        SmallFileMgr.getFilePath(
                "host:8030", "FILE:" + fileId + ":" + md5, "token", tempDir.toString());

        // Remove disk file so any disk/HTTP access would fail
        assertTrue(cachedFile.delete());

        // Second call must still succeed via memory cache
        String result = SmallFileMgr.getFilePath(
                "host:8030", "FILE:" + fileId + ":" + md5, "token", tempDir.toString());

        assertEquals(cachedFile.getAbsolutePath(), result);
    }

    // -------------------------------------------------------------------------
    // HTTP download from FE
    // -------------------------------------------------------------------------

    @Test
    void testDownloadSuccess() throws Exception {
        byte[] content = "-----BEGIN CERTIFICATE-----\ndata\n-----END CERTIFICATE-----".getBytes();
        String md5 = DigestUtils.md5Hex(content);
        String fileId = "20001";

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        int port = server.getAddress().getPort();
        server.createContext(
                "/api/get_small_file",
                exchange -> {
                    // Verify query params are forwarded correctly
                    String query = exchange.getRequestURI().getQuery();
                    assertTrue(query.contains("file_id=" + fileId));
                    assertTrue(query.contains("token=test-token"));

                    exchange.sendResponseHeaders(200, content.length);
                    exchange.getResponseBody().write(content);
                    exchange.getResponseBody().close();
                });
        server.start();

        try {
            String result = SmallFileMgr.getFilePath(
                    "127.0.0.1:" + port,
                    "FILE:" + fileId + ":" + md5,
                    "test-token",
                    tempDir.toString());

            File expectedFile = tempDir.resolve(fileId + "." + md5).toFile();
            assertEquals(expectedFile.getAbsolutePath(), result);
            assertTrue(expectedFile.exists());

            // Verify file content
            byte[] written = Files.readAllBytes(expectedFile.toPath());
            assertEquals(md5, DigestUtils.md5Hex(written));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testDownloadSecondCallUsesMemoryCache() throws Exception {
        byte[] content = "cert content".getBytes();
        String md5 = DigestUtils.md5Hex(content);
        String fileId = "20002";

        int[] callCount = {0};
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        int port = server.getAddress().getPort();
        server.createContext(
                "/api/get_small_file",
                exchange -> {
                    callCount[0]++;
                    exchange.sendResponseHeaders(200, content.length);
                    exchange.getResponseBody().write(content);
                    exchange.getResponseBody().close();
                });
        server.start();

        try {
            String address = "127.0.0.1:" + port;
            String filePath = "FILE:" + fileId + ":" + md5;

            SmallFileMgr.getFilePath(address, filePath, "token", tempDir.toString());
            SmallFileMgr.getFilePath(address, filePath, "token", tempDir.toString());

            assertEquals(1, callCount[0], "FE should only be contacted once");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testDownloadMd5Mismatch() throws Exception {
        byte[] expectedContent = "expected content".getBytes();
        String expectedMd5 = DigestUtils.md5Hex(expectedContent);
        String fileId = "20003";

        byte[] wrongContent = "totally different content".getBytes();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        int port = server.getAddress().getPort();
        server.createContext(
                "/api/get_small_file",
                exchange -> {
                    exchange.sendResponseHeaders(200, wrongContent.length);
                    exchange.getResponseBody().write(wrongContent);
                    exchange.getResponseBody().close();
                });
        server.start();

        try {
            RuntimeException ex = assertThrows(
                    RuntimeException.class,
                    () -> SmallFileMgr.getFilePath(
                            "127.0.0.1:" + port,
                            "FILE:" + fileId + ":" + expectedMd5,
                            "test-token",
                            tempDir.toString()));
            assertTrue(ex.getMessage().contains("MD5 mismatch"));

            // Tmp file must be cleaned up after failure
            File tmpFile = tempDir.resolve(fileId + ".tmp").toFile();
            assertTrue(!tmpFile.exists(), "tmp file should be deleted after MD5 mismatch");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testDownloadHttpError() throws Exception {
        String fileId = "20004";
        String md5 = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4";

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        int port = server.getAddress().getPort();
        server.createContext(
                "/api/get_small_file",
                exchange -> {
                    exchange.sendResponseHeaders(500, -1);
                    exchange.getResponseBody().close();
                });
        server.start();

        try {
            RuntimeException ex = assertThrows(
                    RuntimeException.class,
                    () -> SmallFileMgr.getFilePath(
                            "127.0.0.1:" + port,
                            "FILE:" + fileId + ":" + md5,
                            "test-token",
                            tempDir.toString()));
            assertTrue(ex.getMessage().contains("status=500"));
        } finally {
            server.stop(0);
        }
    }

    // -------------------------------------------------------------------------
    // Concurrency
    // -------------------------------------------------------------------------

    /**
     * Verifies that N threads concurrently requesting the same file result in exactly one FE
     * download and all threads receive the correct file path.
     */
    @Test
    void testConcurrentDownloadSameFileSingleFetch() throws Exception {
        byte[] content = "concurrent ssl cert content".getBytes();
        String md5 = DigestUtils.md5Hex(content);
        String fileId = "30001";
        int threadCount = 8;

        AtomicInteger callCount = new AtomicInteger(0);
        // Simulate a slow FE download so threads have a chance to race
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        int port = server.getAddress().getPort();
        server.createContext(
                "/api/get_small_file",
                exchange -> {
                    callCount.incrementAndGet();
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ignored) {
                    }
                    exchange.sendResponseHeaders(200, content.length);
                    exchange.getResponseBody().write(content);
                    exchange.getResponseBody().close();
                });
        server.start();

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startGate = new CountDownLatch(1);
        String address = "127.0.0.1:" + port;
        String filePath = "FILE:" + fileId + ":" + md5;
        String expectedPath = tempDir.resolve(fileId + "." + md5).toFile().getAbsolutePath();

        try {
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                futures.add(pool.submit(() -> {
                    startGate.await();
                    return SmallFileMgr.getFilePath(
                            address, filePath, "token", tempDir.toString());
                }));
            }

            startGate.countDown(); // release all threads simultaneously

            for (Future<String> future : futures) {
                assertEquals(expectedPath, future.get());
            }

            assertEquals(1, callCount.get(), "FE should be contacted exactly once");

            // Verify the final file is intact
            byte[] written = Files.readAllBytes(tempDir.resolve(fileId + "." + md5));
            assertEquals(md5, DigestUtils.md5Hex(written));
        } finally {
            pool.shutdown();
            server.stop(0);
        }
    }
}
