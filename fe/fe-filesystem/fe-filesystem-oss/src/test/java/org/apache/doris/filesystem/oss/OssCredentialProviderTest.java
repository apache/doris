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

package org.apache.doris.filesystem.oss;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyuncs.auth.AlibabaCloudCredentials;
import com.aliyuncs.auth.BasicSessionCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for credential bridging and provider selection in OssObjStorage.
 * No real OSS credentials required — all external calls are overridden.
 */
class OssCredentialProviderTest {

    // ── AliyunCredentialsBridge ───────────────────────────────────────────────

    @Test
    void bridge_wrapsSessionCredentialsWithToken() {
        BasicSessionCredentials alibabaCreds =
                new BasicSessionCredentials("session-ak", "session-sk", "session-token");

        AliyunCredentialsBridge bridge = new AliyunCredentialsBridge(() -> alibabaCreds);

        Credentials ossCreds = bridge.getCredentials();
        Assertions.assertEquals("session-ak", ossCreds.getAccessKeyId());
        Assertions.assertEquals("session-sk", ossCreds.getSecretAccessKey());
        Assertions.assertEquals("session-token", ossCreds.getSecurityToken());
    }

    @Test
    void bridge_wrapsBasicCredentialsWithoutToken() {
        AlibabaCloudCredentials alibabaCreds = new com.aliyuncs.auth.BasicCredentials("ak", "sk");

        AliyunCredentialsBridge bridge = new AliyunCredentialsBridge(() -> alibabaCreds);

        Credentials ossCreds = bridge.getCredentials();
        Assertions.assertEquals("ak", ossCreds.getAccessKeyId());
        Assertions.assertEquals("sk", ossCreds.getSecretAccessKey());
        Assertions.assertTrue(ossCreds.getSecurityToken() == null
                || ossCreds.getSecurityToken().isEmpty());
    }

    @Test
    void bridge_wrapsProviderException() {
        AliyunCredentialsBridge bridge = new AliyunCredentialsBridge(() -> {
            throw new com.aliyuncs.exceptions.ClientException("network error");
        });

        Assertions.assertThrows(IllegalStateException.class, bridge::getCredentials);
    }

    // ── OssObjStorage — INSTANCE_PROFILE mode auto-discovery ─────────────────

    @Test
    void objStorage_instanceProfileMode_callsDiscoverEcsRoleName() {
        OssFileSystemProperties props = OssFileSystemProperties.of(java.util.Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.credentials_provider", "instance_profile"));

        String[] captured = {null};
        OssObjStorage storage = new OssObjStorage(props) {
            @Override
            protected String discoverEcsRoleName() {
                captured[0] = "discovered";
                return "AutoRole";
            }
        };

        try {
            storage.getClient();
        } catch (Exception ignored) {
            // expected — no real endpoint in tests
        }
        Assertions.assertEquals("discovered", captured[0],
                "discoverEcsRoleName() should be called when no role name is set");
    }

    @Test
    void objStorage_instanceProfileMode_usesConfiguredRoleNameWithoutDiscovery() {
        OssFileSystemProperties props = OssFileSystemProperties.of(java.util.Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.ecs_ram_role_name", "ExplicitRole",
                "oss.credentials_provider", "instance_profile"));

        String[] captured = {null};
        OssObjStorage storage = new OssObjStorage(props) {
            @Override
            protected String discoverEcsRoleName() {
                captured[0] = "should-not-be-called";
                return "AutoRole";
            }
        };

        try {
            storage.getClient();
        } catch (Exception ignored) {
            // expected
        }
        Assertions.assertNull(captured[0], "discoverEcsRoleName() should NOT be called when role name is set");
    }
}
