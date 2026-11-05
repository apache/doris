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
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.THiveTableSink;
import org.apache.doris.thrift.TIcebergDeleteSink;
import org.apache.doris.thrift.TIcebergMergeSink;
import org.apache.doris.thrift.TIcebergTableSink;

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

    /**
     * Binds typed Azure access onto a connector-built write sink, from the backend properties the
     * connector already put in its hadoop config. Doing it here keeps the connectors Thrift-shape
     * agnostic for Azure and gives every write sink the same rule.
     *
     * <p>The sinks' output paths already keep their native Azure form
     * ({@code abfss://c1@acct.dfs.core.windows.net/db/t/data}): connectors normalize them through
     * {@code DefaultConnectorContext}, which leaves Azure locations unchanged because this config
     * is bound here.
     */
    public static void bindToWriteSink(TDataSink sink) {
        // Keyed on the member that is set rather than the sink type: a connector fills exactly
        // one of them, and only these carry object-storage writes.
        if (sink.isSetIcebergTableSink()) {
            TIcebergTableSink tableSink = sink.getIcebergTableSink();
            writeSinkConfig(tableSink.getHadoopConfig()).ifPresent(tableSink::setAzureClientConfig);
        }
        if (sink.isSetIcebergDeleteSink()) {
            TIcebergDeleteSink deleteSink = sink.getIcebergDeleteSink();
            writeSinkConfig(deleteSink.getHadoopConfig()).ifPresent(deleteSink::setAzureClientConfig);
        }
        if (sink.isSetIcebergMergeSink()) {
            TIcebergMergeSink mergeSink = sink.getIcebergMergeSink();
            writeSinkConfig(mergeSink.getHadoopConfig()).ifPresent(mergeSink::setAzureClientConfig);
        }
        if (sink.isSetHiveTableSink()) {
            THiveTableSink hiveSink = sink.getHiveTableSink();
            writeSinkConfig(hiveSink.getHadoopConfig()).ifPresent(hiveSink::setAzureClientConfig);
        }
    }

    // hadoop_config is optional in Thrift; a sink without it has no object-storage access to type.
    private static Optional<TAzureClientConfig> writeSinkConfig(Map<String, String> hadoopConfig) {
        return hadoopConfig == null ? Optional.empty() : fromBackendProperties(hadoopConfig);
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
                String expiresAtMs = props.get(AzureBackendPropertyKeys.SAS_EXPIRES_AT_MS);
                if (StringUtils.isNotBlank(expiresAtMs)) {
                    // The provider validated it as Unix milliseconds.
                    sas.setExpiresAtMs(Long.parseLong(expiresAtMs));
                }
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
