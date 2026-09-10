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
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.kerberos.ExecutionAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** The engine exposes provider-bound FileIO credentials without first emitting a backend map. */
class DefaultConnectorContextResolvedStoragePropertiesTest {
    private static final String TOKEN_KEY = "adls.sas-token.account.dfs.core.windows.net";
    private static final String TOKEN = "sig=fresh&se=2100-01-01T00:00:00Z";
    private static final String SHARED_KEY_ACCOUNT_NAME = "adls.auth.shared-key.account.name";
    private static final String SHARED_KEY_ACCOUNT_KEY = "adls.auth.shared-key.account.key";
    private static final String FRESH_SHARED_KEY = "ZnJlc2gtdGVzdC1rZXk=";

    @Test
    void freshVendedSasReplacesExpiredStaticAuthenticationBeforeFileIOAccess() {
        Map<String, String> catalog = Map.of("azure.account_name", "account",
                "azure.sas_token", "sig=expired", "azure.sas_expiry_ms", "1");
        DefaultConnectorContext context = context(catalog);

        StorageProperties azure = azure(context.resolveStorageProperties(Map.of(TOKEN_KEY, TOKEN)));
        Map<String, String> fileIO = azure.toIcebergFileIOProperties();

        Assertions.assertEquals(TOKEN, fileIO.get(TOKEN_KEY));
        Assertions.assertEquals(TOKEN, fileIO.get("adls.sas-token.account.blob.core.windows.net"));
        Assertions.assertEquals("4102444800000",
                fileIO.get("adls.sas-token-expires-at-ms.account.dfs.core.windows.net"));
        Assertions.assertTrue(fileIO.keySet().stream().allMatch(key -> key.startsWith("adls.")));
        Assertions.assertFalse(fileIO.containsKey("adls.auth.shared-key.account.key"));
        Assertions.assertEquals(fileIO.get(TOKEN_KEY),
                azure.toBackendProperties().orElseThrow().toMap().get("AZURE_SAS_TOKEN"));
    }

    @Test
    void staticSharedKeyRemainsAvailableForNullOrEmptyRequestCredentials() {
        Map<String, String> catalog = Map.of("azure.account_name", "account", "azure.account_key", "static-key");
        DefaultConnectorContext context = context(catalog);

        for (Map<String, String> credentials : Arrays.<Map<String, String>>asList(null, Map.of())) {
            StorageProperties storage = azure(context.resolveStorageProperties(credentials));
            Map<String, String> fileIO = storage.toIcebergFileIOProperties();

            Assertions.assertEquals("account", fileIO.get("adls.auth.shared-key.account.name"));
            Assertions.assertEquals("static-key", fileIO.get("adls.auth.shared-key.account.key"));
            Assertions.assertFalse(fileIO.containsKey(TOKEN_KEY));
            Assertions.assertEquals("SHARED_KEY", storage.toBackendProperties().orElseThrow()
                    .toMap().get("AZURE_AUTH_TYPE"));
        }
        Assertions.assertEquals("static-key", azure(context.getStorageProperties())
                .toIcebergFileIOProperties().get("adls.auth.shared-key.account.key"));
    }

    @Test
    void vendedAzureSasDoesNotChangeTheIndependentS3Binding() {
        Map<String, String> catalog = Map.of(
                "azure.endpoint", "https://account.dfs.core.windows.net",
                "azure.account_name", "account", "azure.account_key", "azure-static-key",
                "s3.endpoint", "https://s3.us-west-2.amazonaws.com", "s3.region", "us-west-2",
                "s3.access_key", "s3-access-key", "s3.secret_key", "s3-secret-key");
        DefaultConnectorContext context = context(catalog);
        List<StorageProperties> before = context.resolveStorageProperties(Map.of());
        StorageProperties staticAzure = azure(before);
        StorageProperties staticS3 = provider(before, "S3");
        Map<String, String> originalAzure = staticAzure.toBackendProperties().orElseThrow().toMap();
        Map<String, String> originalS3 = staticS3.toBackendProperties().orElseThrow().toMap();
        Assertions.assertEquals("azure-static-key", originalAzure.get("AZURE_ACCOUNT_KEY"));
        Assertions.assertEquals("s3-access-key", originalS3.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("s3-secret-key", originalS3.get("AWS_SECRET_KEY"));
        Assertions.assertTrue(staticAzure.matchedProperties().keySet().stream()
                .allMatch(key -> key.startsWith("azure.")));
        Assertions.assertTrue(staticS3.matchedProperties().keySet().stream().allMatch(key -> key.startsWith("s3.")));
        Assertions.assertFalse(originalAzure.keySet().stream().anyMatch(key -> key.startsWith("AWS_")));
        Assertions.assertFalse(originalS3.keySet().stream().anyMatch(key -> key.startsWith("AZURE_")));

        List<StorageProperties> after = context.resolveStorageProperties(Map.of(TOKEN_KEY, TOKEN));
        Map<String, String> vendedAzure = azure(after).toBackendProperties().orElseThrow().toMap();

        Assertions.assertEquals(originalS3, provider(after, "S3").toBackendProperties().orElseThrow().toMap());
        Assertions.assertEquals(originalAzure, staticAzure.toBackendProperties().orElseThrow().toMap());
        Assertions.assertEquals(TOKEN, vendedAzure.get("AZURE_SAS_TOKEN"));
        Assertions.assertEquals("https://account.blob.core.windows.net", vendedAzure.get("AZURE_ENDPOINT"));
        Assertions.assertFalse(vendedAzure.containsKey("AZURE_ACCOUNT_KEY"));
        Assertions.assertFalse(vendedAzure.keySet().stream().anyMatch(key -> key.startsWith("AWS_")));
        Assertions.assertEquals("azure-static-key", catalog.get("azure.account_key"));
    }

    @Test
    void staticOAuthKeepsItsHadoopRequirementAndAuthentication() {
        Map<String, String> catalog = Map.of(
                "fs.azure.support", "true", "iceberg.catalog.type", "rest", "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "account.dfs.core.windows.net",
                "azure.oauth2_client_id", "client-id", "azure.oauth2_client_secret", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token");

        StorageProperties storage = azure(context(catalog).resolveStorageProperties(Map.of()));

        Assertions.assertEquals(Map.of("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO"),
                storage.toIcebergFileIOProperties());
        Assertions.assertEquals("client-secret", storage.toHadoopProperties().orElseThrow()
                .toHadoopConfigurationMap().get("fs.azure.account.oauth2.client.secret.account.dfs.core.windows.net"));
        Assertions.assertEquals("OAUTH2", storage.toBackendProperties().orElseThrow().toMap().get("AZURE_AUTH_TYPE"));
    }

    @Test
    void freshVendedSasSkipsMalformedStaticAuthenticationAndCapturesOneImmutableSnapshot() {
        Map<String, String> catalog = new HashMap<>(Map.of("azure.account_name", "account",
                "azure.auth_type", "OAuth2", "azure.oauth2_client_secret", "incomplete-old-identity"));
        Map<String, String> vended = new HashMap<>(Map.of(TOKEN_KEY, TOKEN));
        Map<String, String> originalCatalog = Map.copyOf(catalog);
        Map<String, String> originalVended = Map.copyOf(vended);
        AtomicInteger rawCalls = new AtomicInteger();
        DefaultConnectorContext context = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {}, () -> {
                    throw new AssertionError("Must not bind another generation of static authentication");
                }, () -> {
                    rawCalls.incrementAndGet();
                    return catalog;
                });

        List<StorageProperties> bindings = context.resolveStorageProperties(vended);

        Assertions.assertEquals(originalCatalog, catalog);
        Assertions.assertEquals(originalVended, vended);
        catalog.clear();
        vended.clear();
        Assertions.assertEquals(TOKEN, azure(bindings).toIcebergFileIOProperties().get(TOKEN_KEY));
        Assertions.assertFalse(azure(bindings).toBackendProperties().orElseThrow()
                .toMap().containsKey("AZURE_CLIENT_SECRET"));
        Assertions.assertEquals(1, rawCalls.get());
        Assertions.assertThrows(UnsupportedOperationException.class, bindings::clear);
    }

    @ParameterizedTest
    @MethodSource("replacedSharedKeyAuthentications")
    void sharedKeyReplacesObsoleteAuthenticationWithoutChangingTheIndependentHdfsIdentity(
            Map<String, String> oldAuthentication) {
        Map<String, String> catalog = new HashMap<>(oldAuthentication);
        catalog.put("azure.endpoint", "https://account.dfs.core.windows.net");
        catalog.put("fs.defaultFS", "hdfs://namenode:8020");
        catalog.put("hadoop.username", "reader");
        Map<String, String> vended = Map.of(
                SHARED_KEY_ACCOUNT_NAME, "account", SHARED_KEY_ACCOUNT_KEY, FRESH_SHARED_KEY);
        AtomicInteger rawCalls = new AtomicInteger();
        DefaultConnectorContext context = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {}, () -> {
                    throw new AssertionError("Must not access the replaced static authentication supplier");
                }, () -> {
                    rawCalls.incrementAndGet();
                    return catalog;
                });

        List<StorageProperties> bindings = context.resolveStorageProperties(vended);
        StorageProperties azure = azure(bindings);
        Map<String, String> nativeProperties = azure.toBackendProperties().orElseThrow().toMap();
        Map<String, String> fileIOProperties = azure.toIcebergFileIOProperties();

        Assertions.assertEquals(Map.of(
                "provider", "azure", "AZURE_AUTH_TYPE", "SHARED_KEY", "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net", "AZURE_ACCOUNT_KEY", FRESH_SHARED_KEY),
                nativeProperties);
        Assertions.assertEquals(Map.of(
                SHARED_KEY_ACCOUNT_NAME, "account", SHARED_KEY_ACCOUNT_KEY, FRESH_SHARED_KEY,
                "adls.connection-string.account.dfs.core.windows.net", "https://account.blob.core.windows.net",
                "adls.connection-string.account.blob.core.windows.net", "https://account.blob.core.windows.net"),
                fileIOProperties);
        Assertions.assertEquals(fileIOProperties.get(SHARED_KEY_ACCOUNT_KEY),
                nativeProperties.get("AZURE_ACCOUNT_KEY"));
        StorageProperties hdfs = provider(bindings, "HDFS");
        Assertions.assertFalse(hdfs.isSyntheticDefault());
        Map<String, String> hadoopProperties = hdfs.toHadoopProperties().orElseThrow().toHadoopConfigurationMap();
        Assertions.assertEquals("hdfs://namenode:8020", hadoopProperties.get("fs.defaultFS"));
        Assertions.assertEquals("reader", hadoopProperties.get("hadoop.username"));
        Assertions.assertEquals(1, rawCalls.get());

        ConnectorStorageAccessResolver resolver = context.newStorageAccessResolver(vended);
        ConnectorStorageAccess nativeAccess = resolver.apply(
                "abfss://container@account.dfs.core.windows.net/table/data.parquet");
        ConnectorStorageAccess hdfsAccess = resolver.apply("hdfs://namenode:8020/table/data.parquet");

        Assertions.assertEquals("AZURE", nativeAccess.getProviderName());
        Assertions.assertEquals("FILE_S3", nativeAccess.getBackendFileType());
        Assertions.assertEquals(nativeProperties, nativeAccess.getBackendProperties());
        Assertions.assertEquals("HDFS", hdfsAccess.getProviderName());
        Assertions.assertEquals("FILE_HDFS", hdfsAccess.getBackendFileType());
        Assertions.assertEquals("hdfs://namenode:8020", hdfsAccess.getBackendProperties().get("fs.defaultFS"));
        Assertions.assertEquals("reader", hdfsAccess.getBackendProperties().get("hadoop.username"));
        Assertions.assertEquals(2, rawCalls.get(), "each request captures one raw catalog snapshot");
    }

    private static List<Map<String, String>> replacedSharedKeyAuthentications() {
        return List.of(
                Map.of("azure.account_name", "account", "azure.auth_type", "SAS",
                        "azure.sas_token", "sig=expired-old-identity", "azure.sas_expiry_ms", "1"),
                Map.of("azure.account_name", "account", "azure.auth_type", "OAuth2",
                        "azure.oauth2_client_secret", "incomplete-old-identity"));
    }

    @Test
    void unknownVendedExpiryDoesNotInheritThePreviousSasExpiry() {
        Map<String, String> catalog = Map.of("azure.account_name", "account",
                "azure.sas_token", "sig=old", "azure.sas_expiry_ms", "1");
        String token = "si=stored-policy&sig=fresh+encoded%2Fsignature";

        StorageProperties storage = azure(context(catalog).resolveStorageProperties(Map.of(TOKEN_KEY, token)));
        Map<String, String> fileIO = storage.toIcebergFileIOProperties();
        Map<String, String> backend = storage.toBackendProperties().orElseThrow().toMap();

        Assertions.assertEquals(token, fileIO.get(TOKEN_KEY));
        Assertions.assertEquals(token, backend.get("AZURE_SAS_TOKEN"));
        Assertions.assertFalse(fileIO.keySet().stream()
                .anyMatch(key -> key.startsWith("adls.sas-token-expires-at-ms.")));
        Assertions.assertFalse(backend.containsKey("AZURE_SAS_EXPIRY_MS"));
    }

    @Test
    void capturingExpiredCredentialsDoesNotEmitThemOrFallBackToStaticSharedKey() {
        DefaultConnectorContext context = context(Map.of("azure.account_name", "account",
                "azure.account_key", "static-key"));

        List<StorageProperties> bindings = Assertions.assertDoesNotThrow(() -> context.resolveStorageProperties(
                Map.of(TOKEN_KEY, "sig=private-material&se=2000-01-01T00:00:00Z")));
        StorageProperties storage = azure(bindings);

        StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                storage::toIcebergFileIOProperties);
        Assertions.assertEquals("Azure SAS credential is expired", failure.getMessage());
        Assertions.assertNull(failure.getCause());
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> storage.toBackendProperties().orElseThrow().toMap());
    }

    @Test
    void invalidRecognizedCredentialsFailWithoutAStaticFallbackOrSecretDiagnostic() {
        DefaultConnectorContext context = context(Map.of("azure.account_name", "account",
                "azure.account_key", "static-key"));
        for (Map<String, String> vended : List.of(
                Map.of(TOKEN_KEY, "sig=private-material&se=not-a-timestamp"),
                Map.of("adls.token", "private-material"))) {
            StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                    () -> context.resolveStorageProperties(vended));

            Assertions.assertFalse(failure.getMessage().contains("private-material"));
            Assertions.assertNull(failure.getCause());
        }
    }

    @ParameterizedTest
    @MethodSource("invalidSharedKeyCredentials")
    void invalidSharedKeyIsRejectedByBothBindingEntrypointsWithoutReusingStaticAuthentication(
            Map<String, String> vended) {
        DefaultConnectorContext context = context(Map.of(
                "azure.account_name", "account", "azure.account_key", "private-static-key"));
        Map<String, String> originalVended = new HashMap<>(vended);

        for (Executable request : List.<Executable>of(
                () -> context.resolveStorageProperties(vended),
                () -> context.newStorageAccessResolver(vended))) {
            StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class, request);

            Assertions.assertTrue(failure.getMessage().contains("SharedKey"));
            Assertions.assertFalse(failure.toString().contains("private-static-key"));
            Assertions.assertFalse(failure.toString().contains(FRESH_SHARED_KEY));
            Assertions.assertFalse(failure.toString().contains("private-conflicting-key"));
            Assertions.assertNull(failure.getCause());
        }
        Assertions.assertEquals(originalVended, vended);
    }

    private static List<Map<String, String>> invalidSharedKeyCredentials() {
        Map<String, String> valid = Map.of(
                SHARED_KEY_ACCOUNT_NAME, "account", SHARED_KEY_ACCOUNT_KEY, FRESH_SHARED_KEY);
        Map<String, String> nullAccount = new HashMap<>(valid);
        nullAccount.put(SHARED_KEY_ACCOUNT_NAME, null);
        Map<String, String> nullKey = new HashMap<>(valid);
        nullKey.put(SHARED_KEY_ACCOUNT_KEY, null);
        Map<String, String> conflictingAccount = new HashMap<>(valid);
        conflictingAccount.put("ADLS.AUTH.SHARED-KEY.ACCOUNT.NAME", "other-account");
        Map<String, String> conflictingKey = new HashMap<>(valid);
        conflictingKey.put("ADLS.AUTH.SHARED-KEY.ACCOUNT.KEY", "private-conflicting-key");
        return List.of(Map.of(SHARED_KEY_ACCOUNT_NAME, "account"),
                Map.of(SHARED_KEY_ACCOUNT_KEY, FRESH_SHARED_KEY), nullAccount, nullKey,
                conflictingAccount, conflictingKey);
    }

    @Test
    void azureVendingRetainsTheIndependentHdfsBinding() {
        Map<String, String> catalog = Map.of("fs.defaultFS", "hdfs://namenode:8020", "hadoop.username", "reader");
        List<StorageProperties> bindings = context(catalog).resolveStorageProperties(Map.of(TOKEN_KEY, TOKEN));

        StorageProperties hdfs = provider(bindings, "HDFS");
        Assertions.assertFalse(hdfs.isSyntheticDefault());
        Assertions.assertEquals("reader", hdfs.toHadoopProperties().orElseThrow()
                .toHadoopConfigurationMap().get("hadoop.username"));
        Assertions.assertTrue(hdfs.toIcebergFileIOProperties().isEmpty());
        Assertions.assertEquals(TOKEN, azure(bindings).toIcebergFileIOProperties().get(TOKEN_KEY));
    }

    @Test
    void legacyS3VendingDoesNotReplaceRealHdfsWithItsSyntheticDefault() {
        Map<String, String> catalog = Map.of("fs.defaultFS", "hdfs://namenode:8020", "hadoop.username", "reader");
        List<StorageProperties> bindings = context(catalog).resolveStorageProperties(Map.of(
                "s3.endpoint", "https://s3.us-west-2.amazonaws.com",
                "s3.access-key-id", "temporary-access", "s3.secret-access-key", "temporary-secret",
                "s3.session-token", "temporary-session"));

        StorageProperties hdfs = provider(bindings, "HDFS");
        Assertions.assertFalse(hdfs.isSyntheticDefault());
        Assertions.assertEquals("reader", hdfs.toHadoopProperties().orElseThrow()
                .toHadoopConfigurationMap().get("hadoop.username"));
        Assertions.assertEquals("temporary-session", provider(bindings, "S3")
                .toBackendProperties().orElseThrow().toMap().get("AWS_TOKEN"));
        Assertions.assertTrue(bindings.stream().allMatch(binding -> binding.toIcebergFileIOProperties().isEmpty()));
    }

    @Test
    void lightweightContextKeepsItsPreboundStaticStorageWithoutRequiringARawCatalog() {
        StorageAdapter staticAzure = StorageAdapter.ofProvider("AZURE",
                Map.of("azure.account_name", "account", "azure.account_key", "static-key"));
        DefaultConnectorContext context = new DefaultConnectorContext("c", 1L,
                () -> new ExecutionAuthenticator() {}, () -> Map.of(StorageTypeId.AZURE, staticAzure));

        List<StorageProperties> bindings = context.resolveStorageProperties(Map.of());

        Assertions.assertSame(staticAzure.getSpiProperties(), azure(bindings));
        Assertions.assertEquals("static-key", azure(bindings).toIcebergFileIOProperties()
                .get("adls.auth.shared-key.account.key"));
        Assertions.assertTrue(new DefaultConnectorContext("empty", 2L).resolveStorageProperties(null).isEmpty());
    }

    private static StorageProperties azure(List<StorageProperties> bindings) {
        return provider(bindings, "AZURE");
    }

    private static StorageProperties provider(List<StorageProperties> bindings, String name) {
        return bindings.stream().filter(binding -> binding.providerName().equalsIgnoreCase(name))
                .findFirst().orElseThrow();
    }

    private static DefaultConnectorContext context(Map<String, String> raw) {
        return new DefaultConnectorContext("c", 1L, () -> new ExecutionAuthenticator() {},
                Collections::emptyMap, () -> raw);
    }
}
