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

import org.apache.doris.connector.spi.ConnectorStorageAccess;
import org.apache.doris.connector.spi.ConnectorStorageAccessResolver;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.kerberos.ExecutionAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Exercises the real engine/provider binding, without an Azure network dependency. */
class DefaultConnectorContextStorageAccessTest {
    private static final String PATH = "abfss://container@account.dfs.core.windows.net/table/data.parquet";
    private static final String TOKEN_KEY = "adls.sas-token.account.dfs.core.windows.net";
    private static final String TOKEN = "sig=fresh&se=2100-01-01T00:00:00Z";

    @Test
    void freshVendedBindingReplacesExpiredStaticSasBeforeAccess() {
        Map<String, String> catalog = Map.of("azure.account_name", "account",
                "azure.sas_token", "sig=expired", "azure.sas_expiry_ms", "1");
        StorageAdapter expired = StorageAdapter.ofProvider("AZURE", catalog);
        Assertions.assertThrows(StoragePropertiesException.class, expired::getBackendConfigProperties);
        DefaultConnectorContext context = context(catalog, Map.of(StorageTypeId.AZURE, expired));

        ConnectorStorageAccess access = context.newStorageAccessResolver(Map.of(TOKEN_KEY, TOKEN)).apply(PATH);

        assertNativeAzure(access);
        Assertions.assertEquals(TOKEN, access.getBackendProperties().get("AZURE_SAS_TOKEN"));
        Assertions.assertEquals(PATH, access.getNormalizedUri());
        Assertions.assertFalse(access.getBackendProperties().containsKey("AZURE_ACCOUNT_KEY"));
        Assertions.assertFalse(access.getBackendProperties().containsKey("AZURE_CLIENT_SECRET"));
    }

    @Test
    void failedNewCredentialsDoNotFallBackToStaticSharedKey() {
        Map<String, String> catalog = Map.of("azure.account_name", "account", "azure.account_key", "static-key");
        DefaultConnectorContext context = context(catalog,
                Map.of(StorageTypeId.AZURE, StorageAdapter.ofProvider("AZURE", catalog)));
        Function<String, ConnectorStorageAccess> resolver = context.newStorageAccessResolver(
                Map.of(TOKEN_KEY, "sig=expired&se=2000-01-01T00:00:00Z"));

        StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                () -> resolver.apply(PATH));

        Assertions.assertEquals("Azure SAS credential is expired", failure.getMessage());
        Assertions.assertNull(failure.getCause());
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> context.newStorageAccessResolver(Map.of(TOKEN_KEY, "sig=invalid&se=not-a-timestamp")));
    }

    @Test
    void resolverSnapshotsInputsOnceAndIsSharedAcrossThreads() {
        AtomicInteger rawCalls = new AtomicInteger();
        AtomicInteger staticCalls = new AtomicInteger();
        Map<String, String> rawCatalog = new HashMap<>(Map.of("azure.account_name", "account"));
        Map<String, String> vended = new HashMap<>(Map.of(TOKEN_KEY, TOKEN));
        Map<StorageTypeId, StorageAdapter> bindings = new HashMap<>();
        DefaultConnectorContext context = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {}, () -> {
                    staticCalls.incrementAndGet();
                    return bindings;
                }, () -> {
                    rawCalls.incrementAndGet();
                    return rawCatalog;
                });
        Function<String, ConnectorStorageAccess> resolver = context.newStorageAccessResolver(vended);
        rawCatalog.put("azure.account_name", "changed");
        vended.put(TOKEN_KEY, "sig=changed");
        bindings.clear();
        List<CompletableFuture<ConnectorStorageAccess>> tasks = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            tasks.add(CompletableFuture.supplyAsync(() -> resolver.apply(PATH)));
        }

        for (CompletableFuture<ConnectorStorageAccess> task : tasks) {
            Assertions.assertEquals(TOKEN, task.join().getBackendProperties().get("AZURE_SAS_TOKEN"));
        }
        Assertions.assertEquals(0, staticCalls.get());
        Assertions.assertEquals(1, rawCalls.get());
    }

    @Test
    void vendedReplacementDoesNotBindMalformedStaticAuthenticationOrReadAnotherGeneration() {
        Map<String, String> catalog = Map.of("azure.account_name", "account",
                "azure.auth_type", "OAuth2", "azure.oauth2_client_secret", "incomplete-old-identity");
        DefaultConnectorContext context = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {}, () -> {
                    throw new AssertionError("Must not read a second catalog generation or bind old authentication");
                }, () -> catalog);

        ConnectorStorageAccess access = context.newStorageAccessResolver(Map.of(TOKEN_KEY, TOKEN)).apply(PATH);

        assertNativeAzure(access);
        Assertions.assertEquals(TOKEN, access.getBackendProperties().get("AZURE_SAS_TOKEN"));
    }

    @Test
    void hdfsAndAzureViewsDoNotMergeTheirCredentialChannels() {
        Map<String, String> hdfsProps = Map.of("fs.defaultFS", "hdfs://namenode:8020", "hadoop.username", "reader");
        StorageAdapter hdfs = StorageAdapter.ofProvider("HDFS", hdfsProps);
        Function<String, ConnectorStorageAccess> resolver = context(hdfsProps,
                Map.of(StorageTypeId.HDFS, hdfs)).newStorageAccessResolver(Map.of(TOKEN_KEY, TOKEN));

        ConnectorStorageAccess azure = resolver.apply(PATH);
        ConnectorStorageAccess hadoop = resolver.apply("hdfs://namenode:8020/table/data.parquet");

        assertNativeAzure(azure);
        Assertions.assertEquals("FILE_HDFS", hadoop.getBackendFileType());
        Assertions.assertEquals(BackendStorageKind.HDFS, hadoop.getBackendKind());
        Assertions.assertEquals(hdfs.getBackendConfigProperties(), hadoop.getBackendProperties());
        Assertions.assertFalse(hadoop.getBackendProperties().containsKey("AZURE_SAS_TOKEN"));
    }

    @Test
    void providerQueryUsesBoundProvidersWithoutResolvingAnUnrelatedMetadataLocation() {
        Map<String, String> hdfsProps = Map.of("fs.defaultFS", "hdfs://namenode:8020", "hadoop.username", "reader");
        DefaultConnectorContext context = context(hdfsProps, Collections.emptyMap());
        ConnectorStorageAccessResolver hdfsOnly = context.newStorageAccessResolver(Collections.emptyMap());

        Assertions.assertTrue(hdfsOnly.hasProvider("hDfS"));
        Assertions.assertFalse(hdfsOnly.hasProvider("azure"));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> hdfsOnly.apply("s3://metadata-bucket/table"));
        Assertions.assertEquals("FILE_HDFS", hdfsOnly.apply("hdfs://namenode:8020/data.parquet").getBackendFileType());

        ConnectorStorageAccessResolver withVendedAzure = context.newStorageAccessResolver(Map.of(TOKEN_KEY, TOKEN));
        Assertions.assertTrue(withVendedAzure.hasProvider("azure"));
        Assertions.assertTrue(withVendedAzure.hasProvider("HDFS"));
        Assertions.assertFalse(hdfsOnly.hasProvider("azure"), "another request must not mutate the old provider set");
    }

    @Test
    void legacyS3VendingDoesNotReplaceRealHdfsWithASyntheticDefault() {
        Map<String, String> hdfsProps = Map.of("fs.defaultFS", "hdfs://namenode:8020", "hadoop.username", "reader");
        StorageAdapter hdfs = StorageAdapter.ofProvider("HDFS", hdfsProps);
        Function<String, ConnectorStorageAccess> resolver = context(hdfsProps,
                Map.of(StorageTypeId.HDFS, hdfs)).newStorageAccessResolver(Map.of(
                        "s3.endpoint", "https://s3.us-west-2.amazonaws.com",
                        "s3.access-key-id", "temporary-access", "s3.secret-access-key", "temporary-secret",
                        "s3.session-token", "temporary-session"));

        ConnectorStorageAccess hadoop = resolver.apply("hdfs://namenode:8020/table/data.parquet");
        ConnectorStorageAccess s3 = resolver.apply("s3://bucket/data.parquet");

        Assertions.assertEquals(hdfs.getBackendConfigProperties(), hadoop.getBackendProperties());
        Assertions.assertEquals("temporary-session", s3.getBackendProperties().get("AWS_TOKEN"));
    }

    @Test
    void oneLakeSelectsOnlyTheHadoopViewFromTheAzureProvider() {
        Map<String, String> catalog = Map.of(
                "fs.azure.support", "true", "iceberg.catalog.type", "rest", "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "onelake.dfs.fabric.microsoft.com",
                "azure.oauth2_client_id", "client-id", "azure.oauth2_client_secret", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token");
        Function<String, ConnectorStorageAccess> resolver = context(catalog,
                Map.of(StorageTypeId.AZURE, StorageAdapter.ofProvider("AZURE", catalog)))
                .newStorageAccessResolver(Collections.emptyMap());
        ConnectorStorageAccess access = resolver.apply(
                "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/data.parquet");

        Assertions.assertEquals("AZURE", access.getProviderName());
        Assertions.assertEquals("FILE_HDFS", access.getBackendFileType());
        Assertions.assertEquals(BackendStorageKind.HDFS, access.getBackendKind());
        Assertions.assertEquals("OAuth", access.getBackendProperties().get(
                "fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com"));
        Assertions.assertFalse(access.getBackendProperties().containsKey("AZURE_CLIENT_SECRET"));
        Assertions.assertFalse(access.getBackendProperties().containsKey("provider"));
    }

    @Test
    void azureHttpsBindingWinsOverGenericHttpButDoesNotClaimOtherHosts() {
        Map<String, String> catalog = Map.of("azure.account_name", "account", "azure.account_key", "static-key");
        StorageAdapter azure = StorageAdapter.ofProvider("AZURE", catalog);
        StorageAdapter http = StorageAdapter.ofProvider("HTTP", Map.of("uri", "https://example.org/data"));
        Function<String, ConnectorStorageAccess> resolver = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {},
                () -> Map.of(StorageTypeId.AZURE, azure, StorageTypeId.HTTP, http))
                .newStorageAccessResolver(Collections.emptyMap());

        ConnectorStorageAccess access = resolver.apply("https://account.blob.core.windows.net/container/data.parquet");
        assertNativeAzure(access);
        Assertions.assertEquals("static-key", access.getBackendProperties().get("AZURE_ACCOUNT_KEY"));
        ConnectorStorageAccess genericHttp = resolver.apply("https://example.org/data");
        Assertions.assertEquals("FILE_HTTP", genericHttp.getBackendFileType());
        Assertions.assertEquals(http.getBackendConfigProperties(), genericHttp.getBackendProperties());
        Assertions.assertEquals("FILE_HTTP", resolver.apply(
                "https://account.blob.core.windows.net.example.org/container/data").getBackendFileType());
        StoragePropertiesException mismatch = Assertions.assertThrows(StoragePropertiesException.class,
                () -> resolver.apply("https://other.blob.core.windows.net/container/data?sig=secret"));
        Assertions.assertFalse(mismatch.toString().contains("secret"));
        Assertions.assertFalse(mismatch.getCause().toString().contains("secret"));
    }

    @ParameterizedTest
    @CsvSource({"S3,s3.us-west-2.amazonaws.com,s3", "OSS,oss-cn-beijing.aliyuncs.com,oss",
            "GCS,storage.googleapis.com,gs"})
    void nonAzureNativeMapsKeepExistingBackendParameters(String provider, String endpoint, String scheme) {
        Map<String, String> catalog = Map.of("s3.endpoint", endpoint, "s3.access_key", "access-key",
                "s3.secret_key", "secret-key");
        StorageAdapter adapter = StorageAdapter.ofProvider(provider, catalog);
        ConnectorStorageAccess access = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {}, () -> Map.of(adapter.getType(), adapter))
                .newStorageAccessResolver(Collections.emptyMap()).apply(scheme + "://bucket/data.parquet");

        Assertions.assertEquals("FILE_S3", access.getBackendFileType());
        Assertions.assertEquals("s3://bucket/data.parquet", access.getNormalizedUri());
        Assertions.assertEquals(adapter.getBackendConfigProperties(), access.getBackendProperties());
    }

    @Test
    void localAccessNeedsNoRemoteBindingAndMissingRemoteBindingFails() {
        Function<String, ConnectorStorageAccess> resolver = context(Collections.emptyMap(), Collections.emptyMap())
                .newStorageAccessResolver(Collections.emptyMap());

        Assertions.assertEquals("FILE_LOCAL", resolver.apply("file:///tmp/data.parquet").getBackendFileType());
        Assertions.assertThrows(StoragePropertiesException.class, () -> resolver.apply(PATH));
    }

    private static DefaultConnectorContext context(Map<String, String> raw,
            Map<StorageTypeId, StorageAdapter> bindings) {
        return new DefaultConnectorContext("c", 1L, () -> new ExecutionAuthenticator() {}, () -> bindings, () -> raw);
    }

    private static void assertNativeAzure(ConnectorStorageAccess access) {
        Assertions.assertEquals("AZURE", access.getProviderName());
        Assertions.assertEquals("FILE_S3", access.getBackendFileType());
        Assertions.assertEquals(BackendStorageKind.NATIVE, access.getBackendKind());
        Set<String> keys = access.getBackendProperties().keySet();
        Assertions.assertTrue(keys.contains("AZURE_AUTH_TYPE"));
        Assertions.assertTrue(keys.stream().allMatch(key -> key.equals("provider") || key.startsWith("AZURE_")));
        Assertions.assertFalse(keys.contains("AZURE_CONTAINER"));
    }
}
