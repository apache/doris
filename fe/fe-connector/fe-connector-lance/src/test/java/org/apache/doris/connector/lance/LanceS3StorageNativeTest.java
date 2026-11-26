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

package org.apache.doris.connector.lance;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Dataset;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Exercises native HTTP/signing with unrelated AWS credentials and endpoints in the environment. */
class LanceS3StorageNativeTest {

    @Test
    void explicitS3ConfigurationOverridesAmbientIdentityAndEndpoints() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment("catalog");
    }

    @Test
    void vendedS3ConfigurationOverridesAmbientIdentityAndEndpoints() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment("vended");
    }

    @Test
    void inheritedEndpointFollowsAVendedVirtualHostedStyle() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment("to-virtual");
    }

    @Test
    void inheritedEndpointFollowsAVendedPathStyle() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment("to-path");
    }

    @Test
    void inheritedEndpointFollowsAnAlternateTrueValue() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment("to-virtual", "on");
    }

    @Test
    void inheritedEndpointFollowsAnAlternateFalseValue() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment("to-path", "off");
    }

    private void assertExplicitIdentityOverridesTheEnvironment(String mode) throws Exception {
        assertExplicitIdentityOverridesTheEnvironment(mode, String.valueOf("to-virtual".equals(mode)));
    }

    private void assertExplicitIdentityOverridesTheEnvironment(String mode, String vendedVirtualHostedStyle)
            throws Exception {
        LanceNativeTestSupport.requireNativeLibraries();
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> token = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> host = new AtomicReference<>();
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            token.set(exchange.getRequestHeaders().getFirst("X-Amz-Security-Token"));
            path.set(exchange.getRequestURI().getPath());
            host.set(exchange.getRequestHeaders().getFirst("Host"));
            requests.incrementAndGet();
            byte[] body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                    + "<Name>warehouse</Name><Prefix>all_types.lance/_versions/</Prefix>"
                    + "<KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>"
                    + "</ListBucketResult>").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/xml");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        Process process = null;
        try {
            ProcessBuilder builder = new ProcessBuilder(
                    Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    LanceS3StorageNativeTest.class.getName(), "http://127.0.0.1:" + server.getAddress().getPort(),
                    mode, vendedVirtualHostedStyle)
                    .redirectErrorStream(true);
            builder.environment().keySet().removeIf(key -> key.startsWith("AWS_"));
            builder.environment().put("AWS_ACCESS_KEY_ID", "ambient-ak");
            builder.environment().put("AWS_SECRET_ACCESS_KEY", "ambient-sk");
            builder.environment().put("AWS_SESSION_TOKEN", "ambient-token");
            builder.environment().put("AWS_ENDPOINT_URL", "http://127.0.0.1:1");
            builder.environment().put("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:1");
            process = builder.start();
            Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "native listing timed out");
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            Assertions.assertEquals(0, process.exitValue(), output);
            Assertions.assertTrue(requests.get() > 0);
            Assertions.assertEquals("to-virtual".equals(mode) ? "/" : "/warehouse", path.get());
            if (mode.startsWith("to-")) {
                Assertions.assertEquals("to-virtual".equals(mode)
                        ? "warehouse.storage.example:9000" : "storage.example:9000", host.get());
            }
            Assertions.assertTrue(authorization.get().contains("Credential=catalog-ak/"));
            Assertions.assertTrue(token.get() == null || token.get().isEmpty(),
                    "a token from the process identity must not be sent with the catalog keys");
        } finally {
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
            }
            server.stop(0);
        }
    }

    /** Child process entry point: load the real native provider after setting its environment. */
    public static void main(String[] args) {
        if (args[1].startsWith("to-")) {
            boolean virtual = "to-virtual".equals(args[1]);
            Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(
                    "s3://warehouse/all_types.lance",
                    Collections.singletonList(new FakeS3CompatibleStorageProperties("S3")
                            .endpoint("http://storage.example:9000").region("us-east-1")
                            .accessKey("catalog-ak").secretKey("catalog-sk")
                            .usePathStyle(String.valueOf(virtual))),
                    Map.of("virtual_hosted_style_request", args[2], "proxy_url", args[0]));
            Assertions.assertTrue(Dataset.listManifestLocations("s3://warehouse/all_types.lance", options).isEmpty());
            return;
        }
        Map<String, String> options = "vended".equals(args[1])
                ? LanceStorageOptions.fromDorisAndVendedStorageOptions("s3://warehouse/all_types.lance",
                        Collections.emptyList(), Map.of("endpoint", args[0], "region", "us-east-1",
                                "access_key_id", "catalog-ak", "secret_access_key", "catalog-sk",
                                "virtual_hosted_style_request", "false"))
                : LanceStorageOptions.fromDorisStorageProperties("s3://warehouse/all_types.lance",
                        Collections.singletonList(new FakeS3CompatibleStorageProperties("S3").endpoint(args[0])
                                .region("us-east-1").accessKey("catalog-ak").secretKey("catalog-sk")
                                .usePathStyle("true")));
        Assertions.assertTrue(Dataset.listManifestLocations("s3://warehouse/all_types.lance", options).isEmpty());
    }
}
