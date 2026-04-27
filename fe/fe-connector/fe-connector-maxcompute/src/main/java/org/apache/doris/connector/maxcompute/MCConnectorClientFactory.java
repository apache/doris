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

package org.apache.doris.connector.maxcompute;

import com.aliyun.auth.credentials.Credential;
import com.aliyun.auth.credentials.provider.EcsRamRoleCredentialProvider;
import com.aliyun.auth.credentials.provider.RamRoleArnCredentialProvider;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AklessAccount;
import com.aliyun.odps.account.AliyunAccount;

import java.util.Map;

/**
 * Factory for creating MaxCompute (ODPS) client instances.
 * Adapted from fe-common MCUtils — copied into plugin to avoid
 * depending on fe-common.
 */
public final class MCConnectorClientFactory {
    private MCConnectorClientFactory() {
    }

    /**
     * Validates that required authentication properties are present.
     */
    public static void checkAuthProperties(Map<String, String> properties) {
        String authType = properties.getOrDefault(
                MCConnectorProperties.AUTH_TYPE,
                MCConnectorProperties.DEFAULT_AUTH_TYPE);

        if (authType.equalsIgnoreCase(
                MCConnectorProperties.AUTH_TYPE_AK_SK)) {
            if (!properties.containsKey(MCConnectorProperties.ACCESS_KEY)
                    || !properties.containsKey(
                            MCConnectorProperties.SECRET_KEY)) {
                throw new RuntimeException(
                        "Missing access key or secret key for "
                                + "AK/SK auth type");
            }
        } else if (authType.equalsIgnoreCase(
                MCConnectorProperties.AUTH_TYPE_RAM_ROLE_ARN)) {
            if (!properties.containsKey(MCConnectorProperties.ACCESS_KEY)
                    || !properties.containsKey(
                            MCConnectorProperties.SECRET_KEY)
                    || !properties.containsKey(
                            MCConnectorProperties.RAM_ROLE_ARN)) {
                throw new RuntimeException(
                        "Missing access key, secret key or role arn "
                                + "for RAM Role ARN auth type");
            }
        } else if (authType.equalsIgnoreCase(
                MCConnectorProperties.AUTH_TYPE_ECS_RAM_ROLE)) {
            if (!properties.containsKey(
                    MCConnectorProperties.ECS_RAM_ROLE)) {
                throw new RuntimeException(
                        "Missing role name for ECS RAM Role auth type");
            }
        } else {
            throw new RuntimeException(
                    "Unsupported auth type: " + authType);
        }
    }

    /**
     * Creates an Odps client based on the authentication configuration.
     */
    public static Odps createClient(Map<String, String> properties) {
        String authType = properties.getOrDefault(
                MCConnectorProperties.AUTH_TYPE,
                MCConnectorProperties.DEFAULT_AUTH_TYPE);

        if (authType.equalsIgnoreCase(
                MCConnectorProperties.AUTH_TYPE_AK_SK)) {
            String accessKey = properties.get(
                    MCConnectorProperties.ACCESS_KEY);
            String secretKey = properties.get(
                    MCConnectorProperties.SECRET_KEY);
            Account account = new AliyunAccount(accessKey, secretKey);
            return new Odps(account);
        } else if (authType.equalsIgnoreCase(
                MCConnectorProperties.AUTH_TYPE_RAM_ROLE_ARN)) {
            String accessKey = properties.get(
                    MCConnectorProperties.ACCESS_KEY);
            String secretKey = properties.get(
                    MCConnectorProperties.SECRET_KEY);
            String roleArn = properties.get(
                    MCConnectorProperties.RAM_ROLE_ARN);
            RamRoleArnCredentialProvider provider =
                    RamRoleArnCredentialProvider.builder()
                            .credential(Credential.builder()
                                    .accessKeyId(accessKey)
                                    .accessKeySecret(secretKey).build())
                            .roleArn(roleArn).build();
            AklessAccount aklessAccount = new AklessAccount(provider);
            return new Odps(aklessAccount);
        } else if (authType.equalsIgnoreCase(
                MCConnectorProperties.AUTH_TYPE_ECS_RAM_ROLE)) {
            String roleName = properties.get(
                    MCConnectorProperties.ECS_RAM_ROLE);
            EcsRamRoleCredentialProvider provider =
                    EcsRamRoleCredentialProvider.create(roleName);
            AklessAccount aklessAccount = new AklessAccount(provider);
            return new Odps(aklessAccount);
        } else {
            throw new RuntimeException(
                    "Unsupported auth type: " + authType);
        }
    }
}
