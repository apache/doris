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

import org.apache.doris.filesystem.gcs.GcsFileSystemProperties;

import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class IcebergRestGcsAuthenticationTest {
    @Test
    void anonymousGcsPreservesRestDefaultAndSelectedCredentialProviders() {
        Map<String, String> raw = properties("http://127.0.0.1");
        Map<String, String> options = catalogOptions(raw);
        Assertions.assertFalse(options.containsKey(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER));
        Assertions.assertInstanceOf(DefaultCredentialsProvider.class,
                new AwsProperties(options).restCredentialsProvider());
        raw.put("iceberg.rest.credentials_provider_type", "SYSTEM_PROPERTIES");
        options = catalogOptions(raw);
        Assertions.assertEquals(SystemPropertyCredentialsProvider.class.getName(),
                options.get(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER));
        Assertions.assertInstanceOf(SystemPropertyCredentialsProvider.class,
                new AwsProperties(options).restCredentialsProvider());
        Assertions.assertEquals(AnonymousS3FileIOAwsClientFactory.class.getName(),
                options.get(S3FileIOProperties.CLIENT_FACTORY));
    }

    @Test
    void anonymousGcsObjectsDoNotDisableRestRequestSigning() throws Exception {
        AtomicReference<String> restAuthorization = new AtomicReference<>();
        AtomicReference<String> objectAuthorization = new AtomicReference<>();
        AtomicInteger restRequests = new AtomicInteger();
        AtomicInteger objectRequests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                String authorization = exchange.getRequestHeaders().getFirst("Authorization");
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    objectRequests.incrementAndGet();
                    objectAuthorization.set(authorization);
                    exchange.getResponseHeaders().set("Content-Length", "8");
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    restRequests.incrementAndGet();
                    restAuthorization.set(authorization);
                    byte[] response = "{\"defaults\":{},\"overrides\":{}}".getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, response.length);
                    exchange.getResponseBody().write(response);
                }
            }
        });
        server.start();
        try {
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
            Map<String, String> raw = properties(endpoint);
            raw.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, MetadataCredentialsProvider.class.getName());
            Map<String, String> options = catalogOptions(raw);
            assertRestRequestIsSigned(options, restAuthorization);
            try (S3FileIO io = new S3FileIO()) {
                io.initialize(options);
                Assertions.assertEquals(8L, io.newInputFile("s3://bucket/object").getLength());
            }
            Assertions.assertEquals(1, objectRequests.get());
            Assertions.assertNull(objectAuthorization.get());
            // Creating an object client must not mutate the REST catalog's credential configuration.
            assertRestRequestIsSigned(options, restAuthorization);
            Assertions.assertTrue(restRequests.get() >= 2);
        } finally {
            server.stop(0);
        }
    }

    private static void assertRestRequestIsSigned(Map<String, String> options,
            AtomicReference<String> authorization) throws Exception {
        authorization.set(null);
        try (RESTCatalog catalog = new RESTCatalog()) {
            catalog.initialize("rest-gcs", options);
        }
        String header = authorization.get();
        Assertions.assertNotNull(header);
        Assertions.assertTrue(header.contains("Credential=rest-test-ak/"), header);
        Assertions.assertTrue(header.contains("/us-west-2/execute-api/aws4_request"), header);
    }

    private static Map<String, String> properties(String endpoint) {
        Map<String, String> raw = new HashMap<>(Map.of(
                "iceberg.catalog.type", "rest", "iceberg.rest.uri", endpoint,
                "iceberg.rest.signing-name", "execute-api", "iceberg.rest.sigv4-enabled", "true",
                "iceberg.rest.signing-region", "us-west-2", "provider", "GCP",
                "gs.credential_provider_type", "ANONYMOUS", "gs.endpoint", endpoint,
                "use_path_style", "true", S3FileIOProperties.S3_CRT_ENABLED, "false"));
        // Keep the local transport test independent of the optional Apache HTTP client.
        raw.put(HttpClientProperties.CLIENT_TYPE, HttpClientProperties.CLIENT_TYPE_URLCONNECTION);
        return raw;
    }

    private static Map<String, String> catalogOptions(Map<String, String> raw) {
        return IcebergCatalogFactory.buildCatalogProperties(IcebergCatalogProperties.of(raw),
                Optional.of(GcsFileSystemProperties.of(raw)), false);
    }

    public static final class MetadataCredentialsProvider implements AwsCredentialsProvider {
        public static MetadataCredentialsProvider create() {
            return new MetadataCredentialsProvider();
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create("rest-test-ak", "rest-test-sk");
        }
    }
}
