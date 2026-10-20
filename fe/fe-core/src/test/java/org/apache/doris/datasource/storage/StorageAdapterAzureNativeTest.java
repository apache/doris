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

import org.apache.doris.datasource.credentials.IcebergAdlsVendedCredentials;
import org.apache.doris.thrift.TAzureClientConfig;
import org.apache.doris.thrift.TAzureClientSecret;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * End-to-end FE half of native Azure access: user properties bound through {@link StorageAdapter}
 * must yield the typed BE request built by {@link AzureThriftAdapter}.
 */
public class StorageAdapterAzureNativeTest {

    @Test
    public void testOauth2BindsOutsideIcebergRestAndYieldsClientSecret() {
        // A TVF/catalog definition without iceberg.catalog.type=rest. OAuth2 used to be rejected
        // here because BE could only use it through the Iceberg REST/OneLake Hadoop path.
        Map<String, String> props = new HashMap<>();
        props.put("provider", "azure");
        props.put("azure.auth_type", "OAuth2");
        props.put("azure.oauth2_account_host", "acct.blob.core.windows.net");
        props.put("azure.oauth2_client_id", "client");
        props.put("azure.oauth2_client_secret", "secret");
        props.put("azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant-1/oauth2/token");

        StorageAdapter adapter = StorageAdapter.of(props);
        TAzureClientConfig config = AzureThriftAdapter
                .fromBackendProperties(adapter.getBackendConfigProperties()).orElseThrow();

        Assertions.assertEquals("https://acct.blob.core.windows.net", config.getEndpoint());
        Assertions.assertTrue(config.getCredential().isSetClientSecret());
        TAzureClientSecret secret = config.getCredential().getClientSecret();
        Assertions.assertEquals("tenant-1", secret.getTenantId());
        Assertions.assertEquals("client", secret.getClientId());
        Assertions.assertEquals("secret", secret.getClientSecret());
        Assertions.assertEquals("https://login.microsoftonline.com/", secret.getAuthorityHost());
    }

    @Test
    public void testSasYieldsSasCredential() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "azure");
        props.put("azure.auth_type", "SAS");
        props.put("azure.account_name", "acct");
        props.put("azure.sas_token", "sv=2024&se=2030-01-01&sig=abc");

        StorageAdapter adapter = StorageAdapter.of(props);
        TAzureClientConfig config = AzureThriftAdapter
                .fromBackendProperties(adapter.getBackendConfigProperties()).orElseThrow();

        Assertions.assertEquals("https://acct.blob.core.windows.net", config.getEndpoint());
        Assertions.assertEquals("acct", config.getAccountName());
        Assertions.assertTrue(config.getCredential().isSetSas());
        Assertions.assertEquals("sv=2024&se=2030-01-01&sig=abc", config.getCredential().getSas().getToken());
    }

    @Test
    void testIcebergRestVendedSasYieldsSasCredential() {
        // What Databricks Unity Catalog vends with an Iceberg table, plus a key Doris ignores.
        Map<String, String> vended = new HashMap<>();
        vended.put("adls.sas-token.acct.dfs.core.windows.net", "sv=2024&se=2030-01-01&sig=abc");
        vended.put("adls.sas-token-expires-at-ms.acct.dfs.core.windows.net", "1893456000000");
        vended.put("adls.refresh-credentials-endpoint", "https://example.cloud.databricks.com/credentials");

        StorageAdapter adapter = StorageAdapter.of(IcebergAdlsVendedCredentials.toAzureProperties(vended));
        TAzureClientConfig config = AzureThriftAdapter
                .fromBackendProperties(adapter.getBackendConfigProperties()).orElseThrow();

        Assertions.assertEquals("https://acct.blob.core.windows.net", config.getEndpoint());
        Assertions.assertEquals("acct", config.getAccountName());
        Assertions.assertTrue(config.getCredential().isSetSas());
        Assertions.assertEquals("sv=2024&se=2030-01-01&sig=abc", config.getCredential().getSas().getToken());
        Assertions.assertEquals(1893456000000L, config.getCredential().getSas().getExpiresAtMs());
    }
}
