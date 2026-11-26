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

package org.apache.doris.httpv2.rest;

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ConfigException;
import org.apache.doris.httpv2.HttpTokenManager;
import org.apache.doris.httpv2.entity.ResponseBody;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class CredentialRotateActionTest {
    @TempDir
    Path tempDir;

    private CredentialRotateAction action;
    private Path baseConf;
    private Path customConf;

    private String originalHttpToken;
    private String originalCloudUniqueId;
    private boolean originalEnableAllHttpAuth;
    private int originalHttpPort;

    @BeforeEach
    public void setUp() throws Exception {
        originalHttpToken = Config.http_token;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalEnableAllHttpAuth = Config.enable_all_http_auth;
        originalHttpPort = Config.http_port;

        baseConf = tempDir.resolve("fe.conf");
        customConf = tempDir.resolve("fe_custom.conf");
        Files.writeString(baseConf,
                "http_token = token-A\n"
                        + "cloud_unique_id = 1:inst1:fe-A\n"
                        + "http_port = " + originalHttpPort + "\n");

        Config config = new Config();
        config.init(baseConf.toAbsolutePath().toString());
        config.initCustom(customConf.toAbsolutePath().toString());
        HttpTokenManager.getInstance().resetForTest(Config.http_token);

        action = new CredentialRotateAction();
    }

    @AfterEach
    public void tearDown() {
        Config.http_token = originalHttpToken;
        Config.cloud_unique_id = originalCloudUniqueId;
        Config.enable_all_http_auth = originalEnableAllHttpAuth;
        Config.http_port = originalHttpPort;
        HttpTokenManager.getInstance().resetForTest(originalHttpToken);
    }

    private static HttpServletRequest mockTokenRequest(String token, String authHeader) {
        HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
        Mockito.when(req.getParameter("token")).thenReturn(token);
        if (authHeader != null) {
            Mockito.when(req.getHeader("Authorization")).thenReturn(authHeader);
        }
        return req;
    }

    private static ResponseBody<?> assertOkHttpAndExtractBody(ResponseEntity entity) {
        Assertions.assertEquals(HttpStatus.OK, entity.getStatusCode());
        Assertions.assertInstanceOf(ResponseBody.class, entity.getBody());
        return (ResponseBody<?>) entity.getBody();
    }

    @Test
    public void testEndpointsOnlyAllowPostMethod() throws Exception {
        Method rotateTokenMethod = CredentialRotateAction.class.getMethod(
                "rotateHttpToken", HttpServletRequest.class);
        RequestMapping tokenMapping = rotateTokenMethod.getAnnotation(RequestMapping.class);
        Assertions.assertNotNull(tokenMapping);
        Assertions.assertArrayEquals(new String[] {"/api/rotate_http_token"}, tokenMapping.path());
        Assertions.assertArrayEquals(new RequestMethod[] {RequestMethod.POST}, tokenMapping.method());

        Method rotateIdMethod = CredentialRotateAction.class.getMethod(
                "rotateCloudUniqueId", HttpServletRequest.class);
        RequestMapping idMapping = rotateIdMethod.getAnnotation(RequestMapping.class);
        Assertions.assertNotNull(idMapping);
        Assertions.assertArrayEquals(new String[] {"/api/rotate_cloud_unique_id"}, idMapping.path());
        Assertions.assertArrayEquals(new RequestMethod[] {RequestMethod.POST}, idMapping.method());
    }

    @Test
    public void testTokenAuthCannotBeBypassedByBasicAuthOrGlobalSwitch() throws Exception {
        Config.enable_all_http_auth = false;
        String basicHeader = "Basic " + Base64.getEncoder().encodeToString("root:".getBytes());

        // Missing token with Basic Auth header still rejected.
        ResponseBody<?> missingBody = assertOkHttpAndExtractBody(
                action.rotateHttpToken(mockTokenRequest(null, basicHeader)));
        Assertions.assertEquals(401, missingBody.getCode());
        Assertions.assertEquals("Unauthorized", missingBody.getMsg());
        Assertions.assertEquals("incorrect token", missingBody.getData());
        Assertions.assertEquals(0, missingBody.getCount());

        // Empty token rejected.
        ResponseBody<?> emptyBody = assertOkHttpAndExtractBody(
                action.rotateCloudUniqueId(mockTokenRequest("", null)));
        Assertions.assertEquals(401, emptyBody.getCode());
        Assertions.assertEquals("Unauthorized", emptyBody.getMsg());
        Assertions.assertEquals("incorrect token", emptyBody.getData());
        Assertions.assertEquals(0, emptyBody.getCount());

        // New token written on disk cannot authenticate before rotate.
        Files.writeString(baseConf, "http_token = token-B\n");
        ResponseBody<?> preAuthBody = assertOkHttpAndExtractBody(
                action.rotateHttpToken(mockTokenRequest("token-B", null)));
        Assertions.assertEquals(401, preAuthBody.getCode());
        Assertions.assertEquals("incorrect token", preAuthBody.getData());
    }

    @Test
    public void testHttpTokenRotationLifecycleGracePeriodAndCustomOverride() throws Exception {
        AtomicLong nowMs = new AtomicLong(1_000_000L);
        HttpTokenManager.getInstance().setClockForTest(nowMs::get);

        // fe_custom.conf overrides fe.conf; unrelated config changes are not reloaded.
        Files.writeString(baseConf,
                "http_token = token-base\n"
                        + "http_port = 19999\n");
        Files.writeString(customConf, "http_token = token-B\n");

        ResponseBody<?> rotateBody = assertOkHttpAndExtractBody(
                action.rotateHttpToken(mockTokenRequest("token-A", null)));
        Assertions.assertEquals(0, rotateBody.getCode());
        Assertions.assertEquals("success", rotateBody.getMsg());
        Assertions.assertInstanceOf(Map.class, rotateBody.getData());
        Assertions.assertTrue(((Map<?, ?>) rotateBody.getData()).isEmpty());
        Assertions.assertEquals(0, rotateBody.getCount());

        // Unrelated config was not reloaded.
        Assertions.assertEquals(originalHttpPort, Config.http_port);

        // Within grace period: both A and B are valid, base-only value is not.
        Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-B"));
        Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-A"));
        Assertions.assertFalse(HttpTokenManager.getInstance().verify("token-base"));

        // Idempotent retry at T + 1000s must not refresh A's expiry or clear A.
        nowMs.set(1_000_000L + 1_000_000L);
        ResponseBody<?> retryBody = assertOkHttpAndExtractBody(
                action.rotateHttpToken(mockTokenRequest("token-B", null)));
        Assertions.assertEquals(0, retryBody.getCode());
        Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-A"));

        // Strictly before 1800s boundary: A still valid.
        nowMs.set(1_000_000L + HttpTokenManager.HTTP_TOKEN_GRACE_PERIOD_MS - 1L);
        Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-A"));

        // At exact 1800s boundary: A expires, B remains valid.
        nowMs.set(1_000_000L + HttpTokenManager.HTTP_TOKEN_GRACE_PERIOD_MS);
        Assertions.assertFalse(HttpTokenManager.getInstance().verify("token-A"));
        Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-B"));

        ResponseBody<?> expiredBody = assertOkHttpAndExtractBody(
                action.rotateHttpToken(mockTokenRequest("token-A", null)));
        Assertions.assertEquals(401, expiredBody.getCode());

        // Simulated restart only restores current token B, dropping any previous state.
        HttpTokenManager.getInstance().init("token-B");
        Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-B"));
        Assertions.assertFalse(HttpTokenManager.getInstance().verify("token-A"));
    }

    @Test
    public void testHttpTokenRotateFailuresLeaveStateUnchanged() throws Exception {
        // Empty value in custom config overriding base.
        Files.writeString(customConf, "http_token = \n");
        ResponseBody<?> emptyBody = assertOkHttpAndExtractBody(
                action.rotateHttpToken(mockTokenRequest("token-A", null)));
        Assertions.assertEquals(1, emptyBody.getCode());
        Assertions.assertEquals("Error", emptyBody.getMsg());
        Assertions.assertEquals("Configured http_token must not be empty", emptyBody.getData());
        Assertions.assertEquals(0, emptyBody.getCount());
        Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-A"));

        // Corrupted properties escape sequence in custom config.
        Files.writeString(customConf, "http_token = \\uZZZZ\n");
        ResponseBody<?> corruptBody = assertOkHttpAndExtractBody(
                action.rotateHttpToken(mockTokenRequest("token-A", null)));
        Assertions.assertEquals(1, corruptBody.getCode());
        Assertions.assertEquals("Error", corruptBody.getMsg());
        Assertions.assertEquals("Failed to read configured http_token", corruptBody.getData());
        Assertions.assertEquals(0, corruptBody.getCount());
        Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-A"));

        // Missing base config file.
        Files.delete(customConf);
        Files.delete(baseConf);
        ResponseBody<?> missingFileBody = assertOkHttpAndExtractBody(
                action.rotateHttpToken(mockTokenRequest("token-A", null)));
        Assertions.assertEquals(1, missingFileBody.getCode());
        Assertions.assertEquals("Failed to read configured http_token", missingFileBody.getData());
        Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-A"));
    }

    @Test
    public void testCloudUniqueIdRotateAndFailuresAndMutableBypassRejected() throws Exception {
        // Custom config overrides base config.
        Files.writeString(baseConf, "http_token = token-A\ncloud_unique_id = 1:inst1:fe-base\n");
        Files.writeString(customConf, "cloud_unique_id = 1:inst1:fe-B\n");

        ResponseBody<?> okBody = assertOkHttpAndExtractBody(
                action.rotateCloudUniqueId(mockTokenRequest("token-A", null)));
        Assertions.assertEquals(0, okBody.getCode());
        Assertions.assertEquals("success", okBody.getMsg());
        Assertions.assertInstanceOf(Map.class, okBody.getData());
        Assertions.assertTrue(((Map<?, ?>) okBody.getData()).isEmpty());
        Assertions.assertEquals(0, okBody.getCount());
        Assertions.assertEquals("1:inst1:fe-B", Config.cloud_unique_id);

        // Idempotent retry succeeds.
        ResponseBody<?> retryBody = assertOkHttpAndExtractBody(
                action.rotateCloudUniqueId(mockTokenRequest("token-A", null)));
        Assertions.assertEquals(0, retryBody.getCode());
        Assertions.assertEquals("1:inst1:fe-B", Config.cloud_unique_id);

        // Empty value leaves existing runtime cloud_unique_id unchanged.
        Files.writeString(customConf, "cloud_unique_id = \n");
        ResponseBody<?> emptyBody = assertOkHttpAndExtractBody(
                action.rotateCloudUniqueId(mockTokenRequest("token-A", null)));
        Assertions.assertEquals(1, emptyBody.getCode());
        Assertions.assertEquals("Error", emptyBody.getMsg());
        Assertions.assertEquals("Configured cloud_unique_id must not be empty", emptyBody.getData());
        Assertions.assertEquals("1:inst1:fe-B", Config.cloud_unique_id);

        // Missing base config leaves runtime state unchanged.
        Files.delete(customConf);
        Files.delete(baseConf);
        ResponseBody<?> missingBody = assertOkHttpAndExtractBody(
                action.rotateCloudUniqueId(mockTokenRequest("token-A", null)));
        Assertions.assertEquals(1, missingBody.getCode());
        Assertions.assertEquals("Failed to read configured cloud_unique_id", missingBody.getData());
        Assertions.assertEquals("1:inst1:fe-B", Config.cloud_unique_id);

        // Existing mutable config update path must not bypass rotation.
        Assertions.assertThrows(ConfigException.class,
                () -> ConfigBase.setMutableConfig("http_token", "bypassed"));
        Assertions.assertThrows(ConfigException.class,
                () -> ConfigBase.setMutableConfig("cloud_unique_id", "bypassed"));

        // Both credentials are masked in config dumps.
        Assertions.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, ConfigBase.dump().get("http_token"));
        Assertions.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, ConfigBase.dump().get("cloud_unique_id"));
    }

    @Test
    public void testConcurrentRotateIsSerialized() throws Exception {
        Files.writeString(baseConf, "http_token = token-B\ncloud_unique_id = 1:inst1:fe-B\n");
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Boolean>> tasks = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                tasks.add(() -> HttpTokenManager.getInstance().rotate().isOk());
            }
            for (Future<Boolean> f : pool.invokeAll(tasks)) {
                Assertions.assertTrue(f.get());
            }
            Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-B"));
            Assertions.assertTrue(HttpTokenManager.getInstance().verify("token-A"));
        } finally {
            pool.shutdownNow();
        }
    }
}
