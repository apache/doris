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

package org.apache.doris.common.maxcompute;

import com.aliyun.auth.credentials.Credential;
import com.aliyun.auth.credentials.provider.EcsRamRoleCredentialProvider;
import com.aliyun.auth.credentials.provider.RamRoleArnCredentialProvider;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AklessAccount;
import com.aliyun.odps.account.AliyunAccount;

import java.util.Map;

public class MCUtils {
    public static void checkAuthProperties(Map<String, String> properties) {
        String authType = properties.getOrDefault(MCProperties.AUTH_TYPE, MCProperties.DEFAULT_AUTH_TYPE);
        if (authType.equalsIgnoreCase(MCProperties.AUTH_TYPE_AK_SK)) {
            if (!properties.containsKey(MCProperties.ACCESS_KEY) || !properties.containsKey(MCProperties.SECRET_KEY)) {
                throw new RuntimeException("Missing access key or secret key for AK/SK auth type");
            }
        } else if (authType.equalsIgnoreCase(MCProperties.AUTH_TYPE_RAM_ROLE_ARN)) {
            if (!properties.containsKey(MCProperties.ACCESS_KEY) || !properties.containsKey(MCProperties.SECRET_KEY)
                    || !properties.containsKey(MCProperties.RAM_ROLE_ARN)) {
                throw new RuntimeException("Missing access key, secret key or role arn for RAM Role ARN auth type");
            }
        } else if (authType.equalsIgnoreCase(MCProperties.AUTH_TYPE_ECS_RAM_ROLE)) {
            if (!properties.containsKey(MCProperties.ECS_RAM_ROLE)) {
                throw new RuntimeException("Missing role name for ECS RAM Role auth type");
            }
        } else {
            throw new RuntimeException("Unsupported auth type: " + authType);
        }
    }

    public static Odps createMcClient(Map<String, String> properties) {
        String authType = properties.getOrDefault(MCProperties.AUTH_TYPE, MCProperties.DEFAULT_AUTH_TYPE);
        if (authType.equalsIgnoreCase(MCProperties.AUTH_TYPE_AK_SK)) {
            String accessKey = properties.get(MCProperties.ACCESS_KEY);
            String secretKey = properties.get(MCProperties.SECRET_KEY);
            Account account = new AliyunAccount(accessKey, secretKey);
            return new Odps(account);
        } else if (authType.equalsIgnoreCase(MCProperties.AUTH_TYPE_RAM_ROLE_ARN)) {
            String accessKey = properties.get(MCProperties.ACCESS_KEY);
            String secretKey = properties.get(MCProperties.SECRET_KEY);
            String roleArn = properties.get(MCProperties.RAM_ROLE_ARN);
            RamRoleArnCredentialProvider ramRoleArnCredentialProvider =
                    RamRoleArnCredentialProvider.builder().credential(
                                    Credential.builder().accessKeyId(accessKey)
                                            .accessKeySecret(secretKey).build())
                            .roleArn(roleArn).build();
            AklessAccount aklessAccount = new AklessAccount(ramRoleArnCredentialProvider);
            return new Odps(aklessAccount);
        } else if (authType.equalsIgnoreCase(MCProperties.AUTH_TYPE_ECS_RAM_ROLE)) {
            String roleName = properties.get(MCProperties.ECS_RAM_ROLE);
            EcsRamRoleCredentialProvider credentialProvider = EcsRamRoleCredentialProvider.create(roleName);
            AklessAccount aklessAccount = new AklessAccount(credentialProvider);
            return new Odps(aklessAccount);
        } else {
            throw new RuntimeException("Unsupported auth type: " + authType);
        }
    }
}
