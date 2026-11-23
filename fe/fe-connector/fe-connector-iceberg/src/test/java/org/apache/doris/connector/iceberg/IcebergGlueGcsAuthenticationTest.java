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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x;
import org.apache.doris.filesystem.gcs.GcsFileSystemProperties;

import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.S3FileIOAwsClientFactories;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class IcebergGlueGcsAuthenticationTest {
    @Test
    void nativeGcsPreservesGlueRegionAndSigningCredentials() throws Exception {
        try (RequestRecorder recorder = new RequestRecorder()) {
            for (String mode : List.of("DEFAULT", "COMPUTE_ENGINE")) {
                for (String account : List.of("", "reader@test.iam.gserviceaccount.com")) {
                    Map<String, String> raw = properties(mode, recorder.endpoint());
                    raw.put("gs.impersonation_service_account", account);
                    Map<String, String> options = catalogOptions(raw);
                    Assertions.assertEquals("us-west-2", options.get(AwsClientProperties.CLIENT_REGION));
                    Assertions.assertEquals(GcpS3FileIOAwsClientFactory.class.getName(),
                            options.get(S3FileIOProperties.CLIENT_FACTORY));
                    assertGlueRequestIsSigned(options, recorder);
                }
            }
            Assertions.assertEquals(4, recorder.glueRequests.get());
        }
    }

    @Test
    void anonymousGcsPreservesGlueCredentialsAndSendsUnsignedObjectRequests() throws Exception {
        try (RequestRecorder recorder = new RequestRecorder()) {
            Map<String, String> options = catalogOptions(properties("ANONYMOUS", recorder.endpoint()));
            Assertions.assertEquals(ConfigurationAWSCredentialsProvider2x.class.getName(),
                    options.get(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER));
            assertGlueRequestIsSigned(options, recorder);
            assertObjectRequestsAreUnsigned(options, recorder);
            // Initializing FileIO must leave the catalog's credential provider intact.
            assertGlueRequestIsSigned(options, recorder);
        }
    }

    @Test
    void anonymousGcsFileIoDoesNotAssumeTheGlueRole() throws Exception {
        try (RequestRecorder recorder = new RequestRecorder()) {
            Map<String, String> raw = properties("ANONYMOUS", recorder.endpoint());
            raw.remove("glue.access_key");
            raw.remove("glue.secret_key");
            raw.put("glue.role_arn", "arn:aws:iam::123456789012:role/glue-reader");
            Map<String, String> options = catalogOptions(raw);
            Assertions.assertEquals("org.apache.iceberg.aws.AssumeRoleAwsClientFactory",
                    options.get("client.factory"));
            assertObjectRequestsAreUnsigned(options, recorder);
            Assertions.assertEquals(0, recorder.glueRequests.get());
        }
    }

    private static Map<String, String> properties(String mode, String endpoint) {
        Map<String, String> raw = new HashMap<>(Map.of(
                "iceberg.catalog.type", "glue", "provider", "GCP",
                "warehouse", "gs://bucket/warehouse", "gs.credential_provider_type", mode,
                "glue.access_key", "glue-test-ak", "glue.secret_key", "glue-test-sk",
                "glue.region", "us-west-2", "glue.endpoint", endpoint));
        // Keep the local transport test independent of the optional Apache HTTP client.
        raw.put(HttpClientProperties.CLIENT_TYPE, HttpClientProperties.CLIENT_TYPE_URLCONNECTION);
        if ("ANONYMOUS".equals(mode)) {
            raw.put("gs.endpoint", endpoint);
            raw.put("use_path_style", "true");
            // Exercise the Java async client without the optional native CRT dependency.
            raw.put(S3FileIOProperties.S3_CRT_ENABLED, "false");
        }
        return raw;
    }

    private static Map<String, String> catalogOptions(Map<String, String> raw) {
        return IcebergCatalogFactory.buildCatalogProperties(IcebergCatalogProperties.of(raw),
                Optional.of(GcsFileSystemProperties.of(raw)), false);
    }

    private static void assertGlueRequestIsSigned(Map<String, String> options, RequestRecorder recorder) {
        try (GlueClient glue = AwsClientFactories.from(options).glue()) {
            Assertions.assertTrue(glue.getDatabases(b -> { }).databaseList().isEmpty());
            Assertions.assertEquals("us-west-2", glue.serviceClientConfiguration().region().id());
        }
        String authorization = recorder.glueAuthorization.get();
        Assertions.assertNotNull(authorization);
        Assertions.assertTrue(authorization.contains("Credential=glue-test-ak/"), authorization);
        Assertions.assertTrue(authorization.contains("/us-west-2/glue/aws4_request"), authorization);
    }

    private static void assertObjectRequestsAreUnsigned(Map<String, String> options, RequestRecorder recorder) {
        try (S3FileIO io = new S3FileIO()) {
            io.initialize(options);
            Assertions.assertEquals(8L, io.newInputFile("s3://bucket/object").getLength());
        }
        Assertions.assertEquals(1, recorder.objectRequests.get());
        Assertions.assertNull(recorder.objectAuthorization.get());
        S3FileIOAwsClientFactory factory = S3FileIOAwsClientFactories.initialize(options);
        factory = SerializationUtil.deserializeFromBytes(SerializationUtil.serializeToBytes(factory));
        try (S3AsyncClient client = factory.s3Async()) {
            Assertions.assertEquals(8L, client.headObject(b -> b.bucket("bucket").key("object"))
                    .join().contentLength());
        }
        Assertions.assertEquals(2, recorder.objectRequests.get());
        Assertions.assertNull(recorder.objectAuthorization.get());
    }

    private static final class RequestRecorder implements AutoCloseable {
        private final HttpServer server;
        private final AtomicReference<String> glueAuthorization = new AtomicReference<>();
        private final AtomicReference<String> objectAuthorization = new AtomicReference<>();
        private final AtomicInteger glueRequests = new AtomicInteger();
        private final AtomicInteger objectRequests = new AtomicInteger();

        private RequestRecorder() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                try (exchange) {
                    exchange.getRequestBody().readAllBytes();
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        objectRequests.incrementAndGet();
                        objectAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                        exchange.getResponseHeaders().set("Content-Length", "8");
                        exchange.sendResponseHeaders(200, -1);
                    } else {
                        glueRequests.incrementAndGet();
                        glueAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                        byte[] response = "{\"DatabaseList\":[]}".getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.1");
                        exchange.sendResponseHeaders(200, response.length);
                        exchange.getResponseBody().write(response);
                    }
                }
            });
            server.start();
        }

        private String endpoint() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

}
