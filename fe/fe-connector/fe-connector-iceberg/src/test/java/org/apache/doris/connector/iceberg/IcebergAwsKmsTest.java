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

import com.sun.net.httpserver.HttpServer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

class IcebergAwsKmsTest {
    @Test
    void sdkKmsClientWrapsAndUnwrapsBinaryKeysThroughPackagedDependencies() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        List<String> requests = new CopyOnWriteArrayList<>();
        server.createContext("/", exchange -> {
            String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
            requests.add(target);
            exchange.getRequestBody().close();
            byte[] body = (target.endsWith(".Encrypt")
                    ? "{\"CiphertextBlob\":\"AAEC\"}"
                    : "{\"Plaintext\":\"AAH/Ag==\"}").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.1");
            exchange.sendResponseHeaders(200, body.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        try (KeyManagementClient client = EncryptionUtil.createKmsClient(Map.of(
                CatalogProperties.ENCRYPTION_KMS_TYPE, "aws",
                "kms.endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                "client.region", "us-east-1", "http-client.type", "urlconnection",
                "client.credentials-provider", "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider"))) {
            ByteBuffer key = ByteBuffer.wrap(new byte[] {0, 1, (byte) 255, 2});
            ByteBuffer wrapped = client.wrapKey(key.duplicate(), "fixture-key");
            Assertions.assertEquals(ByteBuffer.wrap(new byte[] {0, 1, 2}), wrapped);
            Assertions.assertEquals(key, client.unwrapKey(wrapped, "fixture-key"));
            Assertions.assertEquals(List.of("TrentService.Encrypt", "TrentService.Decrypt"), requests);
        } finally {
            server.stop(0);
        }
    }
}
