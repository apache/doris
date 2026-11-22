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

/** Observes the native OSS signer after Lance imports unrelated process credentials and selectors. */
class LanceOssStorageNativeTest {

    @Test
    void permanentKeysOverrideAmbientTokens() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment(false, "token");
        assertExplicitIdentityOverridesTheEnvironment(true, "token");
    }

    @Test
    void explicitKeysOverrideAmbientAnonymousSelectors() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment(false, "anonymous");
        assertExplicitIdentityOverridesTheEnvironment(true, "anonymous");
    }

    @Test
    void explicitKeysOverrideAmbientRoles() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment(false, "role");
        assertExplicitIdentityOverridesTheEnvironment(true, "role");
    }

    @Test
    void mixedCaseVendedAddressingOverridesTheCatalog() throws Exception {
        assertExplicitIdentityOverridesTheEnvironment(true, "addressing");
    }

    private void assertExplicitIdentityOverridesTheEnvironment(boolean vended, String ambient) throws Exception {
        LanceNativeTestSupport.requireNativeLibraries();
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> token = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            token.set(exchange.getRequestHeaders().getFirst("X-Oss-Security-Token"));
            path.set(exchange.getRequestURI().getPath());
            requests.incrementAndGet();
            byte[] body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    + "<ListBucketResult><Name>warehouse</Name><Prefix>all_types.lance/_versions/</Prefix>"
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
                    LanceOssStorageNativeTest.class.getName(), "http://127.0.0.1:" + server.getAddress().getPort(),
                    vended ? "vended" : "catalog", ambient).redirectErrorStream(true);
            builder.environment().keySet().removeIf(key -> key.startsWith("OSS_")
                    || key.startsWith("AWS_") || key.startsWith("ALIBABA_CLOUD_"));
            builder.environment().put("OSS_ACCESS_KEY_ID", "ambient-ak");
            builder.environment().put("OSS_ACCESS_KEY_SECRET", "ambient-sk");
            builder.environment().put("OSS_ENDPOINT", "http://127.0.0.1:1");
            if ("token".equals(ambient)) {
                builder.environment().put("OSS_SECURITY_TOKEN", "ambient-token");
                builder.environment().put("ALIBABA_CLOUD_SECURITY_TOKEN", "ambient-token");
            } else if ("anonymous".equals(ambient)) {
                builder.environment().put("OSS_SKIP_SIGNATURE", "true");
                builder.environment().put("OSS_ALLOW_ANONYMOUS", "true");
            } else if ("role".equals(ambient)) {
                builder.environment().put("OSS_ROLE_ARN", "acs:ram::123456789012:role/ambient");
                builder.environment().put("ALIBABA_CLOUD_ROLE_ARN", "acs:ram::123456789012:role/ambient");
                builder.environment().put("OSS_STS_ENDPOINT", "http://127.0.0.1:1");
            }
            process = builder.start();
            Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "native OSS listing timed out");
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            Assertions.assertEquals(0, process.exitValue(), output);
            Assertions.assertTrue(requests.get() > 0);
            Assertions.assertEquals("/warehouse", path.get());
            Assertions.assertNotNull(authorization.get(), "explicit keys must sign the request");
            Assertions.assertTrue(authorization.get().contains("catalog-ak"), authorization.get());
            Assertions.assertTrue(token.get() == null || token.get().isEmpty(),
                    "a token from another identity must not accompany the permanent keys");
        } finally {
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
            }
            server.stop(0);
        }
    }

    /** Load the actual JNI provider only after the child process has received its environment. */
    public static void main(String[] args) {
        FakeS3CompatibleStorageProperties binding = new FakeS3CompatibleStorageProperties("OSS")
                .endpoint(args[0]).region("cn-hangzhou").usePathStyle("false");
        Map<String, String> options = "vended".equals(args[1])
                ? LanceStorageOptions.fromDorisAndVendedStorageOptions("oss://warehouse/all_types.lance",
                        Collections.singletonList(binding.sessionToken("catalog-token")),
                        Map.of("access_key_id", "catalog-ak", "access_key_secret", "catalog-sk",
                                "addressing".equals(args[2]) ? "ADDRESSING_STYLE" : "addressing_style", "path"))
                : LanceStorageOptions.fromDorisStorageProperties("oss://warehouse/all_types.lance",
                        Collections.singletonList(binding.accessKey("catalog-ak").secretKey("catalog-sk")
                                .usePathStyle("true")));
        Assertions.assertTrue(Dataset.listManifestLocations("oss://warehouse/all_types.lance", options).isEmpty());
    }
}
