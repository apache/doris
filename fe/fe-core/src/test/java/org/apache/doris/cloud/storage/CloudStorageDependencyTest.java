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

package org.apache.doris.cloud.storage;

import com.aliyun.core.http.HttpClient;
import com.aliyun.core.http.HttpMethod;
import com.aliyun.core.http.HttpRequest;
import com.aliyun.core.http.HttpResponse;
import com.aliyun.core.utils.Configuration;
import com.aliyun.core.utils.HttpClientOptions;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.BasicCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.aliyuncs.profile.DefaultProfile;
import com.obs.services.ObsClient;
import com.obs.services.model.HttpMethodEnum;
import com.obs.services.model.TemporarySignatureRequest;
import com.sun.net.httpserver.HttpServer;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.sts.v20180813.StsClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class CloudStorageDependencyTest {
    @Test
    public void testAliyunUnshadedHttpTransport() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/credentials-test", exchange -> {
            byte[] body = "transport-ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        try (HttpClient client = HttpClient.createDefault(
                new HttpClientOptions().setConfiguration(Configuration.NONE))) {
            Assertions.assertTrue(client.getClass().getName().contains("ApacheAsyncHttpClient"));
            HttpRequest request = new HttpRequest(HttpMethod.GET,
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/credentials-test");
            try (HttpResponse response = client.send(request).get(10, TimeUnit.SECONDS)) {
                Assertions.assertEquals(200, response.getStatusCode());
                Assertions.assertEquals("transport-ok", response.getBodyAsString());
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testAliyunExplicitCredentials() throws Exception {
        BasicCredentials credentials = new BasicCredentials("test-access-key", "test-secret-key");
        DefaultAcsClient client = new DefaultAcsClient(DefaultProfile.getProfile("cn-hangzhou"),
                new StaticCredentialsProvider(credentials));
        try {
            Assertions.assertNotNull(client);
        } finally {
            client.shutdown();
        }

        OSS oss = new OSSClientBuilder().build("https://oss-cn-hangzhou.aliyuncs.com",
                "test-access-key", "test-secret-key", "test-session-token");
        try {
            Assertions.assertNotNull(oss);
        } finally {
            oss.shutdown();
        }
        OSS withoutToken = new OSSClientBuilder().build("https://oss-cn-hangzhou.aliyuncs.com",
                "test-access-key", "test-secret-key");
        try {
            Assertions.assertNotNull(withoutToken);
        } finally {
            withoutToken.shutdown();
        }
    }

    @Test
    public void testTencentExplicitCredentials() {
        Credential credentials = new Credential("test-access-key", "test-secret-key");
        Assertions.assertNotNull(new StsClient(credentials, "ap-beijing"));
    }

    @Test
    public void testObsSigningWithoutDuplicateBundle() throws Exception {
        // hadoop-huaweicloud already supplies the SDK used by ObsRemote.
        Assertions.assertFalse(ObsClient.class.getProtectionDomain().getCodeSource().getLocation()
                .toString().contains("esdk-obs-java-bundle"));
        try (ObsClient client = new ObsClient("test-access-key", "test-secret-key",
                "https://obs.cn-north-4.myhuaweicloud.com")) {
            TemporarySignatureRequest request = new TemporarySignatureRequest(HttpMethodEnum.PUT, 60);
            request.setBucketName("dependency-check");
            request.setObjectKey("sdk-dependency-check");
            String url = client.createTemporarySignature(request).getSignedUrl();
            Assertions.assertTrue(url.contains("sdk-dependency-check"));
            Assertions.assertTrue(url.contains("Signature="));
        }
    }
}
