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

/**
 * Factory for creating MaxCompute (ODPS) client instances.
 * Adapted from fe-common MCUtils — copied into plugin to avoid
 * depending on fe-common.
 */
public final class MCConnectorClientFactory {
    private MCConnectorClientFactory() {
    }

    /**
     * Creates an Odps client based on the authentication configuration.
     *
     * <p>Every case below is reachable with its credentials already present: the auth type is one of
     * three values and each one's credentials are checked when {@link MCCatalogProperties} is built, so
     * there is no unsupported-type arm left to write here.
     */
    public static Odps createClient(MCCatalogProperties props) {
        switch (props.getAuthType()) {
            case AK_SK: {
                Account account = new AliyunAccount(props.getAccessKey(), props.getSecretKey());
                return new Odps(account);
            }
            case RAM_ROLE_ARN: {
                RamRoleArnCredentialProvider provider =
                        RamRoleArnCredentialProvider.builder()
                                .credential(Credential.builder()
                                        .accessKeyId(props.getAccessKey())
                                        .accessKeySecret(props.getSecretKey()).build())
                                .roleArn(props.getRamRoleArn()).build();
                AklessAccount aklessAccount = new AklessAccount(provider);
                return new Odps(aklessAccount);
            }
            case ECS_RAM_ROLE: {
                EcsRamRoleCredentialProvider provider =
                        EcsRamRoleCredentialProvider.create(props.getEcsRamRole());
                AklessAccount aklessAccount = new AklessAccount(provider);
                return new Odps(aklessAccount);
            }
            default:
                throw new IllegalStateException("Unhandled auth type: " + props.getAuthType());
        }
    }
}
