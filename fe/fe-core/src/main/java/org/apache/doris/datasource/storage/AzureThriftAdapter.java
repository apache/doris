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

package org.apache.doris.datasource.storage;

import org.apache.doris.filesystem.properties.AzureBackendPropertyKeys;
import org.apache.doris.thrift.TAzureClientConfig;
import org.apache.doris.thrift.TAzureClientSecret;
import org.apache.doris.thrift.TAzureCredential;
import org.apache.doris.thrift.TAzureSas;
import org.apache.doris.thrift.TAzureSharedKey;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;

/**
 * fe-core-only Thrift glue: builds the typed BE request {@link TAzureClientConfig} from the
 * neutral {@link AzureBackendPropertyKeys} that the Azure filesystem provider writes into its
 * backend property map.
 *
 * <p>The provider stays RPC-neutral (see {@code BackendStorageProperties}); this adapter is the
 * single place that knows the Thrift shape. A map without {@link AzureBackendPropertyKeys#AUTH_TYPE}
 * describes no native Azure access (S3, an old Azure definition, ...) and yields nothing, so the
 * request keeps using the S3-style keys only.
 */
public final class AzureThriftAdapter {

    private AzureThriftAdapter() {
    }

    public static Optional<TAzureClientConfig> fromBackendProperties(Map<String, String> props) {
        String authType = props.get(AzureBackendPropertyKeys.AUTH_TYPE);
        if (authType == null) {
            return Optional.empty();
        }
        TAzureClientConfig config = new TAzureClientConfig();
        config.setEndpoint(props.getOrDefault(AzureBackendPropertyKeys.ENDPOINT, ""));
        String accountName = props.get(AzureBackendPropertyKeys.ACCOUNT_NAME);
        if (StringUtils.isNotBlank(accountName)) {
            config.setAccountName(accountName);
        }
        String container = props.get(AzureBackendPropertyKeys.CONTAINER);
        if (StringUtils.isNotBlank(container)) {
            config.setContainer(container);
        }

        TAzureCredential credential = new TAzureCredential();
        switch (authType) {
            case AzureBackendPropertyKeys.AUTH_SHARED_KEY: {
                TAzureSharedKey sharedKey = new TAzureSharedKey();
                sharedKey.setAccountKey(props.getOrDefault(AzureBackendPropertyKeys.ACCOUNT_KEY, ""));
                credential.setSharedKey(sharedKey);
                break;
            }
            case AzureBackendPropertyKeys.AUTH_SAS: {
                TAzureSas sas = new TAzureSas();
                sas.setToken(props.getOrDefault(AzureBackendPropertyKeys.SAS_TOKEN, ""));
                credential.setSas(sas);
                break;
            }
            case AzureBackendPropertyKeys.AUTH_OAUTH2: {
                TAzureClientSecret clientSecret = new TAzureClientSecret();
                clientSecret.setTenantId(props.getOrDefault(AzureBackendPropertyKeys.TENANT_ID, ""));
                clientSecret.setClientId(props.getOrDefault(AzureBackendPropertyKeys.CLIENT_ID, ""));
                clientSecret.setClientSecret(props.getOrDefault(AzureBackendPropertyKeys.CLIENT_SECRET, ""));
                String authorityHost = props.get(AzureBackendPropertyKeys.AUTHORITY_HOST);
                if (StringUtils.isNotBlank(authorityHost)) {
                    clientSecret.setAuthorityHost(authorityHost);
                }
                credential.setClientSecret(clientSecret);
                break;
            }
            default:
                // The keys come from the Azure provider in this build, so an unknown value is a
                // provider/adapter mismatch rather than user input.
                throw new IllegalStateException("Unknown Azure auth type in backend properties: " + authType);
        }
        config.setCredential(credential);
        return Optional.of(config);
    }
}
