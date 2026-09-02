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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.InternalHttpsUtils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class HttpUtilsTest {

    // Port 1 refuses connections on loopback, so no real HTTP(S) server is needed.
    private static final String REFUSED_PORT_URL_HTTP = "http://127.0.0.1:1/ping";
    private static final String REFUSED_PORT_URL_HTTPS = "https://127.0.0.1:1/ping";

    private boolean originalEnableHttps;
    private String originalKeyStorePath;
    private HttpServer httpServer;

    @Before
    public void setUp() throws Exception {
        originalEnableHttps = Config.enable_https;
        originalKeyStorePath = Config.key_store_path;
        resetCachedSslContext();
    }

    @After
    public void tearDown() throws Exception {
        Config.enable_https = originalEnableHttps;
        Config.key_store_path = originalKeyStorePath;
        resetCachedSslContext();
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    private String startServer(HttpHandler handler) throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        httpServer.createContext("/file.txt", handler);
        httpServer.start();
        return "http://127.0.0.1:" + httpServer.getAddress().getPort() + "/file.txt";
    }

    private static void sendEmpty(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
        exchange.close();
    }

    private void resetCachedSslContext() throws Exception {
        Field field = InternalHttpsUtils.class.getDeclaredField("cachedSslContext");
        field.setAccessible(true);
        field.set(null, null);
    }

    @Test
    public void testExecuteRequestUsesPlainClientForHttpUrlEvenWhenHttpsEnabled() throws Exception {
        Config.enable_https = true;
        Config.key_store_path = "/non/existent/path/doris_ssl_certificate.keystore";
        resetCachedSslContext();

        try {
            HttpUtils.doGet(REFUSED_PORT_URL_HTTP, null);
            Assert.fail("Expected a connection failure against the refused port");
        } catch (RuntimeException e) {
            Assert.fail("Should not have attempted to build the HTTPS client for an http:// URL: "
                    + e.getMessage());
        } catch (IOException expected) {
            // Plain client hit the network and failed there, never touching the broken keystore.
        }
    }

    @Test
    public void testExecuteRequestUsesHttpsClientForHttpsUrl() throws Exception {
        Config.enable_https = true;
        Config.key_store_path = "/non/existent/path/doris_ssl_certificate.keystore";
        resetCachedSslContext();

        try {
            HttpUtils.doGet(REFUSED_PORT_URL_HTTPS, null);
            Assert.fail("Expected SSLContext build failure before any connection attempt");
        } catch (RuntimeException e) {
            Assert.assertTrue("Failure should come from the missing keystore, not an unrelated error",
                    e.getMessage() != null && e.getMessage().contains("doris_ssl_certificate.keystore"));
        } catch (IOException e) {
            Assert.fail("Expected the HTTPS client's keystore failure, not a network-level error: "
                    + e.getMessage());
        }
    }

    @Test
    public void testHeadSuccess() throws IOException {
        final long totalSize = 12345L;
        String url = startServer(exchange -> {
            Assert.assertEquals("HEAD", exchange.getRequestMethod());
            exchange.getResponseHeaders().add("Content-Length", String.valueOf(totalSize));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });

        long size = HttpUtils.getHttpFileSize(url, Collections.emptyMap());
        Assert.assertEquals(totalSize, size);
    }

    @Test
    public void testHeadForbiddenGetRangeReturns206() throws IOException {
        byte[] data = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        String url = startServer(exchange -> {
            if ("HEAD".equals(exchange.getRequestMethod())) {
                sendEmpty(exchange, 403);
                return;
            }
            Assert.assertEquals("bytes=0-0", exchange.getRequestHeaders().getFirst("Range"));
            exchange.getResponseHeaders().add("Content-Range", "bytes 0-0/" + data.length);
            exchange.sendResponseHeaders(206, 1);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data, 0, 1);
            }
        });

        long size = HttpUtils.getHttpFileSize(url, Collections.emptyMap());
        Assert.assertEquals(data.length, size);
    }

    @Test
    public void testHeadForbiddenGetRangeIgnoredReturns200() throws IOException {
        byte[] data = "hello world, this is a test file".getBytes(StandardCharsets.UTF_8);
        String url = startServer(exchange -> {
            if ("HEAD".equals(exchange.getRequestMethod())) {
                sendEmpty(exchange, 403);
                return;
            }
            exchange.getResponseHeaders().add("Content-Length", String.valueOf(data.length));
            exchange.sendResponseHeaders(200, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        });

        long size = HttpUtils.getHttpFileSize(url, Collections.emptyMap());
        Assert.assertEquals(data.length, size);
    }

    @Test
    public void testHeadAndGetBothForbidden() throws IOException {
        String url = startServer(exchange -> sendEmpty(exchange, 403));

        try {
            HttpUtils.getHttpFileSize(url, Collections.emptyMap());
            Assert.fail("Expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get file size"));
        }
    }

    @Test
    public void testHeadOkNoContentLengthFallsBackToGet() throws IOException {
        final long totalSize = 37L;
        String url = startServer(exchange -> {
            if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
                exchange.close();
                return;
            }
            exchange.getResponseHeaders().add("Content-Range", "bytes 0-0/" + totalSize);
            exchange.sendResponseHeaders(206, 1);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(new byte[] { 0 });
            }
        });

        Assert.assertEquals(totalSize, HttpUtils.getHttpFileSize(url, Collections.emptyMap()));
    }

    @Test
    public void testGetRangeMissingContentRangeRejected() throws IOException {
        assertInvalidContentRange(null);
    }

    @Test
    public void testGetRangeMalformedContentRangeRejected() throws IOException {
        assertInvalidContentRange("not-a-content-range");
    }

    @Test
    public void testGetRangeUnknownContentRangeTotalRejected() throws IOException {
        assertInvalidContentRange("bytes 0-0/*");
    }

    @Test
    public void testEmptyResourceReturnsZero() throws IOException {
        String url = startServer(exchange -> {
            if ("HEAD".equals(exchange.getRequestMethod())) {
                sendEmpty(exchange, 403);
                return;
            }
            exchange.getResponseHeaders().add("Content-Range", "bytes */0");
            sendEmpty(exchange, 416);
        });

        Assert.assertEquals(0, HttpUtils.getHttpFileSize(url, Collections.emptyMap()));
    }

    private void assertInvalidContentRange(String contentRange) throws IOException {
        String url = startServer(exchange -> {
            if ("HEAD".equals(exchange.getRequestMethod())) {
                sendEmpty(exchange, 403);
                return;
            }
            if (contentRange != null) {
                exchange.getResponseHeaders().add("Content-Range", contentRange);
            }
            exchange.sendResponseHeaders(206, 1);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(new byte[] { 0 });
            }
        });

        try {
            HttpUtils.getHttpFileSize(url, Collections.emptyMap());
            Assert.fail("Expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Content-Range"));
        }
    }

    @Test
    public void testNullOrEmptyUriRejected() {
        try {
            HttpUtils.getHttpFileSize(null, Collections.emptyMap());
            Assert.fail("Expected IllegalArgumentException for null uri");
        } catch (IllegalArgumentException | IOException e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        try {
            HttpUtils.getHttpFileSize("   ", Collections.emptyMap());
            Assert.fail("Expected IllegalArgumentException for blank uri");
        } catch (IllegalArgumentException | IOException e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }
}
