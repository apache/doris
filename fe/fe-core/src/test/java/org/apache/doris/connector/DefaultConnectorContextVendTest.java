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

package org.apache.doris.connector;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.filesystem.properties.FsCacheKeys;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.kerberos.ExecutionAuthenticator;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Pins provider-owned Azure vended binding and the legacy normalization retained for other connectors.
 * No test in this class creates a remote client or makes a storage request.
 */
public class DefaultConnectorContextVendTest {

    private static DefaultConnectorContext context() {
        return new DefaultConnectorContext("c", 1L);
    }

    @Test
    public void normalizesOssTokenToBackendAwsProps() {
        // Mirrors the raw OSS vended token shape from PaimonVendedCredentialsProviderTest.
        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.accessKeyId", "STS.testAccessKey123");
        token.put("fs.oss.accessKeySecret", "testSecretKey456");
        token.put("fs.oss.securityToken", "testSessionToken789");
        token.put("fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com");

        Map<String, String> be = context().vendStorageCredentials(token);

        // WHY: the BE native S3/object-store client consumes ONLY normalized AWS_* keys; the raw
        // fs.oss.* token is unintelligible to it. The bridge must run StorageProperties.createAll +
        // getBackendPropertiesFromStorageMap to produce them. MUTATION: leaving the default no-op
        // (empty) or skipping the normalization -> AWS_ACCESS_KEY absent -> red.
        Assertions.assertFalse(be.isEmpty(), "a valid OSS token must normalize to non-empty BE props");
        Assertions.assertEquals("STS.testAccessKey123", be.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("testSecretKey456", be.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("testSessionToken789", be.get("AWS_TOKEN"));
    }

    @Test
    public void normalizesAdlsTokenToNativeAzureProps() {
        String accountHost = "account.dfs.core.windows.net";
        Map<String, String> token = Map.of(
                "adls.sas-token." + accountHost, "?sv=2024-01-01&sig=temporary",
                "adls.sas-token-expires-at-ms." + accountHost, "4102444800000");

        Map<String, String> be = context().vendStorageCredentials(token);

        Assertions.assertEquals("azure", be.get("provider"));
        Assertions.assertEquals("SAS", be.get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals("account", be.get("AZURE_ACCOUNT_NAME"));
        Assertions.assertEquals("https://account.blob.core.windows.net", be.get("AZURE_ENDPOINT"));
        Assertions.assertEquals("sv=2024-01-01&sig=temporary", be.get("AZURE_SAS_TOKEN"));
        Assertions.assertEquals("4102444800000", be.get("AZURE_SAS_EXPIRY_MS"));
        Assertions.assertFalse(be.keySet().stream().anyMatch(key -> key.startsWith("adls.")));
        Assertions.assertFalse(be.keySet().stream().anyMatch(key -> key.startsWith("fs.azure.")));
        Assertions.assertFalse(be.keySet().stream().anyMatch(key -> key.startsWith("AWS_")));
        Assertions.assertFalse(be.containsKey("fs.defaultFS"));
        Assertions.assertFalse(be.containsKey("hdfs.security.authentication"));
        Assertions.assertFalse(be.containsKey(FsCacheKeys.fsCacheKeyProperty("hdfs")));
    }

    @Test
    public void expiredAdlsTokenIsNotSilentlyDowngraded() {
        String accountHost = "account.dfs.core.windows.net";
        Map<String, String> token = Map.of(
                "adls.sas-token." + accountHost, "sv=2024-01-01&sig=expired",
                "adls.sas-token-expires-at-ms." + accountHost, "1");

        StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                () -> context().vendStorageCredentials(token));
        Assertions.assertEquals("Azure SAS credential is expired", failure.getMessage());
        Assertions.assertNull(failure.getCause());
    }

    @Test
    public void sasQueryExpiryIsCheckedAtBackendAccessInsteadOfBinding() {
        Map<String, String> token = Map.of(
                "adls.sas-token.account.dfs.core.windows.net",
                "sv=2024-01-01&se=2000-01-01T00%3A00%3A00Z&sig=expired",
                "adls.sas-token-expires-at-ms.account.dfs.core.windows.net", "4102444800000");
        String path = "abfss://container@account.dfs.core.windows.net/table/data.parquet";
        DefaultConnectorContext context = context();

        Assertions.assertEquals(path, context.normalizeStorageUri(path, token));
        Assertions.assertEquals(TFileType.FILE_S3.name(), context.getBackendFileType(path, token));
        StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                () -> context.vendStorageCredentials(token));
        Assertions.assertEquals("Azure SAS credential is expired", failure.getMessage());
    }

    @Test
    public void vendedSasReplacesOldAuthenticationWithoutRebindingIt() {
        Map<String, String> catalog = Map.of(
                "azure.account_name", "account",
                "azure.endpoint", "https://account.blob.core.windows.net",
                "azure.account_key", "static-key",
                "azure.auth_type", "SharedKey");
        Map<String, String> token = new HashMap<>();
        token.put("adls.sas-token.account.dfs.core.windows.net", "sig=temporary&se=2100-01-01T00:00:00Z");
        token.put("azure.auth_type", "OAuth2");
        token.put("AZURE_AUTH_TYPE", "SharedKey");
        token.put("azure.account_key", "old-key");
        token.put("azure.oauth2_client_secret", "old-secret");
        token.put("azure.oauth2_client_id", "old-client");
        Map<String, String> originalToken = new HashMap<>(token);
        DefaultConnectorContext context = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {}, Collections::emptyMap, () -> catalog);

        Map<String, String> backend = context.vendStorageCredentials(token);

        Assertions.assertEquals("SAS", backend.get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals("sig=temporary&se=2100-01-01T00:00:00Z", backend.get("AZURE_SAS_TOKEN"));
        Assertions.assertFalse(backend.containsKey("AZURE_ACCOUNT_KEY"));
        Assertions.assertFalse(backend.containsKey("AZURE_CLIENT_ID"));
        Assertions.assertFalse(backend.containsKey("AZURE_CLIENT_SECRET"));
        Assertions.assertEquals(originalToken, token);
        Assertions.assertEquals("static-key", catalog.get("azure.account_key"));
    }

    @Test
    public void invalidRecognizedSasCannotFallBackToStaticSharedKey() {
        Map<String, String> catalog = Map.of("azure.account_name", "account", "azure.account_key", "static-key");
        StorageAdapter staticBinding = StorageAdapter.ofProvider("AZURE", catalog);
        DefaultConnectorContext context = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {},
                () -> Map.of(StorageTypeId.AZURE, staticBinding), () -> catalog);
        String path = "abfss://container@account.dfs.core.windows.net/table/data.parquet";
        Map<String, String> token = new HashMap<>();
        token.put("adls.sas-token.account.dfs.core.windows.net", null);

        Assertions.assertThrows(StoragePropertiesException.class, () -> context.vendStorageCredentials(token));
        Assertions.assertThrows(StoragePropertiesException.class, () -> context.normalizeStorageUri(path, token));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> context.newStorageUriNormalizer(token).apply(path));
        Assertions.assertThrows(StoragePropertiesException.class, () -> context.getBackendFileType(path, token));
    }

    @Test
    public void azureVendedCredentialsRetainARealHdfsBinding() {
        Map<String, String> token = new HashMap<>();
        token.put("adls.sas-token.account.dfs.core.windows.net", "sig=temporary");
        token.put("fs.defaultFS", "hdfs://namenode:8020");
        token.put("hadoop.username", "mixed-user");
        DefaultConnectorContext context = context();
        String hdfsPath = "hdfs://namenode:8020/table/data.parquet";
        String azurePath = "abfss://container@account.dfs.core.windows.net/table/data.parquet";

        Map<String, String> backend = context.vendStorageCredentials(token);

        Assertions.assertEquals("SAS", backend.get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals("hdfs://namenode:8020", backend.get("fs.defaultFS"));
        Assertions.assertEquals("mixed-user", backend.get("hadoop.username"));
        Assertions.assertEquals(TFileType.FILE_HDFS.name(), context.getBackendFileType(hdfsPath, token));
        Assertions.assertEquals(TFileType.FILE_S3.name(), context.getBackendFileType(azurePath, token));
        Assertions.assertEquals(hdfsPath, context.normalizeStorageUri(hdfsPath, token));
    }

    @Test
    public void unrelatedVendedPropertiesRetainEmptyLegacyResult() {
        Assertions.assertTrue(context().vendStorageCredentials(Map.of("unknown.option", "value")).isEmpty());
    }

    @Test
    public void refreshedSasIsReboundForEachCredentialRequest() {
        DefaultConnectorContext context = context();
        String key = "adls.sas-token.account.dfs.core.windows.net";

        Map<String, String> first = context.vendStorageCredentials(Map.of(key, "sig=first"));
        Map<String, String> second = context.vendStorageCredentials(Map.of(key, "sig=second"));

        Assertions.assertEquals("sig=first", first.get("AZURE_SAS_TOKEN"));
        Assertions.assertEquals("sig=second", second.get("AZURE_SAS_TOKEN"));
        Assertions.assertEquals(first.keySet(), second.keySet());
    }

    @Test
    public void normalizesS3VendedCredentialsThroughExistingPath() {
        Map<String, String> backend = context().vendStorageCredentials(Map.of(
                "s3.endpoint", "https://s3.us-west-2.amazonaws.com",
                "s3.access-key-id", "temporary-access",
                "s3.secret-access-key", "temporary-secret",
                "s3.session-token", "temporary-session"));

        Assertions.assertEquals("temporary-access", backend.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("temporary-secret", backend.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("temporary-session", backend.get("AWS_TOKEN"));
        Assertions.assertFalse(backend.keySet().stream().anyMatch(key -> key.startsWith("AZURE_")));
    }

    @Test
    public void emptyOrNullInputYieldsEmpty() {
        // WHY: a non-REST / no-token table passes an empty map; the bridge must short-circuit to
        // empty (no overlay), never NPE. MUTATION: NPE on null, or fabricating props from nothing -> red.
        Assertions.assertTrue(context().vendStorageCredentials(Collections.emptyMap()).isEmpty());
        Assertions.assertTrue(context().vendStorageCredentials(null).isEmpty());
    }
}
