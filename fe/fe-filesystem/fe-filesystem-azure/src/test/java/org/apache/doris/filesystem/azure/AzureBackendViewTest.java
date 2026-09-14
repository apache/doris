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

package org.apache.doris.filesystem.azure;

import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.FsCacheKeys;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class AzureBackendViewTest {

    private static final String ACCOUNT_HOST = "account.dfs.core.windows.net";
    private static final String ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com";
    private static final String TOKEN = "sig=test-signature&se=2100-01-01T00%3A00%3A00Z";
    private static final String TOKEN_ENDPOINT = "https://login.microsoftonline.com/tenant/oauth2/token";

    @ParameterizedTest
    @ValueSource(strings = {"SHARED_KEY", "SAS", "OAUTH2"})
    void nativeViewContainsOnlyTheSelectedAuthenticationVocabulary(String authType) {
        AzureFileSystemProperties properties = new AzureFileSystemProvider().bind(input(authType, ACCOUNT_HOST));
        Map<String, String> expected = new HashMap<>(Map.of(
                "provider", "azure", "AZURE_AUTH_TYPE", authType,
                "AZURE_ACCOUNT_NAME", "account", "AZURE_ENDPOINT", "https://account.blob.core.windows.net"));
        switch (authType) {
            case "SHARED_KEY":
                expected.put("AZURE_ACCOUNT_KEY", "shared-key");
                break;
            case "SAS":
                expected.put("AZURE_SAS_TOKEN", TOKEN);
                expected.put("AZURE_SAS_EXPIRY_MS", "4102444800000");
                break;
            case "OAUTH2":
                expected.put("AZURE_CLIENT_ID", "client");
                expected.put("AZURE_CLIENT_SECRET", "client-secret");
                expected.put("AZURE_TENANT_ID", "tenant");
                expected.put("AZURE_OAUTH_SERVER_URI", TOKEN_ENDPOINT);
                break;
            default:
                throw new AssertionError("Unexpected test authentication type");
        }

        Assertions.assertEquals(BackendStorageKind.NATIVE, properties.backendKind());
        Assertions.assertEquals(expected, properties.toMap());
        for (String uri : List.of(
                "abfss://container@" + ACCOUNT_HOST + "/dir/http://example/file",
                "wasbs://container@account.blob.core.windows.net/dir/file",
                "https://account.blob.core.windows.net/container/dir/file")) {
            FileSystemProperties binding = properties;
            BackendStorageProperties backend = binding.resolveBackendProperties(
                    binding.validateAndNormalizeUri(uri)).orElseThrow();
            Assertions.assertSame(properties, backend);
            Assertions.assertEquals(BackendStorageKind.NATIVE, backend.backendKind());
            Assertions.assertEquals(expected, backend.toMap());
            Assertions.assertThrows(UnsupportedOperationException.class, () -> backend.toMap().put("extra", "value"));
        }
        // Input aliases remain available to legacy FE clients, but never leak into the native map.
        Assertions.assertEquals("container", properties.getContainer());
        Assertions.assertEquals("true", properties.getUsePathStyle());
        Assertions.assertTrue(properties.toHadoopConfigurationMap().containsKey("fs.azure.readaheadqueue.depth"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SAS", "OAUTH2"})
    void nativeSasAndOauthRequireAnAzureUri(String authType) {
        AzureFileSystemProperties properties = new AzureFileSystemProvider().bind(input(authType, ACCOUNT_HOST));
        for (String scheme : List.of("s3", "S3")) {
            String uri = scheme + "://container/dir/file";
            StoragePropertiesException normalizeError = Assertions.assertThrows(StoragePropertiesException.class,
                    () -> properties.validateAndNormalizeUri(uri));
            StoragePropertiesException viewError = Assertions.assertThrows(StoragePropertiesException.class,
                    () -> properties.resolveBackendProperties(uri));
            Assertions.assertEquals("Azure SAS/OAuth2 data access requires an Azure URI", normalizeError.getMessage());
            Assertions.assertEquals(normalizeError.getMessage(), viewError.getMessage());
            Assertions.assertNull(normalizeError.getCause());
            Assertions.assertNull(viewError.getCause());
        }
    }

    @Test
    void legacySharedKeyRetainsS3UriAndCredentialAliases() {
        AzureFileSystemProperties properties = new AzureFileSystemProvider().bind(Map.of(
                "fs.azure.support", "true", "AWS_ACCESS_KEY", "account", "AWS_SECRET_KEY", "shared-key",
                "AWS_ENDPOINT", "https://account.blob.core.windows.net"));
        String normalized = properties.validateAndNormalizeUri("S3://container/dir/http://example/file");
        BackendStorageProperties view = properties.resolveBackendProperties(normalized).orElseThrow();

        Assertions.assertEquals("s3://container/dir/http://example/file", normalized);
        Assertions.assertSame(properties, view);
        Assertions.assertEquals(BackendStorageKind.NATIVE, view.backendKind());
        Assertions.assertEquals("SHARED_KEY", view.toMap().get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals("shared-key", view.toMap().get("AZURE_ACCOUNT_KEY"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SHARED_KEY", "SAS", "OAUTH2"})
    void oneLakeSelectsHadoopWithoutMutatingTheNativeView(String authType) {
        Map<String, String> raw = input(authType, ONELAKE_HOST);
        raw.remove("AZURE_CONTAINER");
        AzureFileSystemProperties properties = new AzureFileSystemProvider().bind(raw);
        Map<String, String> nativeBefore = properties.toMap();
        Map<String, String> cachedHadoop = properties.resolveBackendProperties(
                "abfss://workspace@" + ONELAKE_HOST + "/lakehouse/Tables/first-file").orElseThrow().toMap();
        for (String scheme : List.of("abfs", "abfss", "ABFSS")) {
            String uri = scheme + "://workspace@" + ONELAKE_HOST + "/lakehouse/Tables/dir//file%2Fname";
            String normalized = properties.validateAndNormalizeUri(uri);
            Assertions.assertEquals(uri.replace("ABFSS://", "abfss://"), normalized);
            BackendStorageProperties backend = properties.resolveBackendProperties(normalized).orElseThrow();
            Map<String, String> hadoop = backend.toMap();
            Assertions.assertSame(cachedHadoop, hadoop);
            Assertions.assertSame(hadoop, backend.toMap());
            Assertions.assertEquals(BackendStorageKind.HDFS, backend.backendKind());
            Assertions.assertEquals("8", hadoop.get("fs.azure.readaheadqueue.depth"));
            Assertions.assertEquals("false", hadoop.get("fs.abfss.impl.disable.cache"));
            Assertions.assertEquals("file:///", hadoop.get("fs.defaultFS"));
            Assertions.assertEquals(properties.fsCacheFingerprint(),
                    hadoop.get(FsCacheKeys.fsCacheKeyProperty("abfss")));
            Assertions.assertFalse(hadoop.containsKey("provider"));
            Assertions.assertFalse(hadoop.keySet().stream().anyMatch(key -> key.startsWith("AZURE_")));
            switch (authType) {
                case "SHARED_KEY":
                    Assertions.assertEquals("shared-key", hadoop.get("fs.azure.account.key"));
                    break;
                case "SAS":
                    Assertions.assertEquals("SAS", hadoop.get("fs.azure.account.auth.type." + ONELAKE_HOST));
                    Assertions.assertEquals(TOKEN, hadoop.get("fs.azure.sas.fixed.token." + ONELAKE_HOST));
                    break;
                case "OAUTH2":
                    Assertions.assertEquals("OAuth", hadoop.get("fs.azure.account.auth.type." + ONELAKE_HOST));
                    Assertions.assertEquals("client", hadoop.get("fs.azure.account.oauth2.client.id." + ONELAKE_HOST));
                    Assertions.assertEquals("client-secret",
                            hadoop.get("fs.azure.account.oauth2.client.secret." + ONELAKE_HOST));
                    Assertions.assertEquals(TOKEN_ENDPOINT,
                            hadoop.get("fs.azure.account.oauth2.client.endpoint." + ONELAKE_HOST));
                    break;
                default:
                    throw new AssertionError("Unexpected test authentication type");
            }
            Assertions.assertThrows(UnsupportedOperationException.class, () -> hadoop.put("extra", "value"));
        }
        Assertions.assertEquals(BackendStorageKind.NATIVE,
                properties.toBackendProperties().orElseThrow().backendKind());
        Assertions.assertEquals(nativeBefore, properties.toMap());
    }

    @ParameterizedTest
    @ValueSource(strings = {"core.windows.net", "core.chinacloudapi.cn", "core.usgovcloudapi.net", "core.cloudapi.de"})
    void claimsKnownAzureHttpHostsWithoutRegisteringGenericHttpSchemes(String suffix) {
        FileSystemProperties binding = new AzureFileSystemProvider().bind(input("SAS", ACCOUNT_HOST));

        Assertions.assertEquals(Set.of("abfs", "abfss", "wasb", "wasbs"), binding.getSupportedSchemes());
        for (String service : List.of("blob", "dfs")) {
            for (String scheme : List.of("http", "HTTPS")) {
                Assertions.assertTrue(binding.claimsUri(scheme + "://account." + service + "." + suffix
                        + "/container/file?sig=test-signature"));
            }
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
            "https://example.test/container/file",
            "https://account.blob.core.windows.net.example.test/container/file",
            "https://accountblob.core.windows.net/container/file",
            "https://example.test/account.blob.core.windows.net/container/file",
            "https://account.blob.core.windows.net@example.test/container/file",
            "https://user@account.blob.core.windows.net/container/file",
            "https://account.blob.core.windows.net:99999/container/file",
            "https://onelake.dfs.fabric.microsoft.com/container/file",
            "https://bucket.s3.amazonaws.com/container/file",
            "ftp://account.blob.core.windows.net/container/file",
            "abfss://container@account.dfs.core.windows.net/file"
    })
    void doesNotClaimOtherProvidersOrLookalikeHosts(String uri) {
        FileSystemProperties binding = new AzureFileSystemProvider().bind(input("SAS", ACCOUNT_HOST));

        Assertions.assertFalse(binding.claimsUri(uri));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://account.blob.core.windows.net/container/%GG?sig=test-signature",
            "https://account.blob.core.windows.net/InvalidContainer/file?sig=test-signature"
    })
    void claimedHostWithAnInvalidPathFailsStrictlyWithoutLeakingTheQuery(String uri) {
        FileSystemProperties binding = new AzureFileSystemProvider().bind(input("SAS", ACCOUNT_HOST));

        Assertions.assertTrue(binding.claimsUri(uri));
        StoragePropertiesException exception = Assertions.assertThrows(StoragePropertiesException.class,
                () -> binding.validateAndNormalizeUri(uri));
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            Assertions.assertFalse(cause.getMessage().contains("test-signature"));
            Assertions.assertFalse(cause.getMessage().contains(uri));
        }
    }

    @Test
    void claimDoesNotBypassAccountContainerOrEndpointValidation() {
        FileSystemProperties binding = new AzureFileSystemProvider().bind(input("SAS", ACCOUNT_HOST));
        for (String uri : List.of(
                "https://other.blob.core.windows.net/container/file",
                "https://account.blob.core.chinacloudapi.cn/container/file",
                "https://account.blob.core.windows.net/other/file",
                "https://account.blob.core.windows.net:8443/container/file",
                "http://account.blob.core.windows.net/container/file")) {
            Assertions.assertTrue(binding.claimsUri(uri));
            Assertions.assertThrows(StoragePropertiesException.class, () -> binding.validateAndNormalizeUri(uri));
            Assertions.assertThrows(StoragePropertiesException.class, () -> binding.resolveBackendProperties(uri));
        }
    }

    @ParameterizedTest
    @CsvSource({
            "https://storage.example.test:8443, https://STORAGE.EXAMPLE.TEST:8443/container/file, true",
            "https://storage.example.test:443, https://storage.example.test/container/file, true",
            "http://127.0.0.1:10000, http://127.0.0.1:10000/container/file, true",
            "https://storage.example.test:8443, https://storage.example.test/container/file, false",
            "https://storage.example.test:8443, http://storage.example.test:8443/container/file, false",
            "https://storage.example.test:8443, https://storage.example.test.evil:8443/container/file, false",
            "https://storage.example.test:8443, https://other.example.test:8443/container/file, false"
    })
    void customHttpClaimRequiresTheExplicitEndpointOrigin(String endpoint, String uri, boolean expectedClaim) {
        Map<String, String> raw = input("SHARED_KEY", ACCOUNT_HOST);
        raw.put("azure.endpoint", endpoint);
        AzureFileSystemProperties binding = new AzureFileSystemProvider().bind(raw);

        Assertions.assertEquals(expectedClaim, binding.claimsUri(uri));
        if (expectedClaim) {
            Assertions.assertEquals(uri, binding.validateAndNormalizeUri(uri));
            Assertions.assertEquals(BackendStorageKind.NATIVE,
                    binding.resolveBackendProperties(uri).orElseThrow().backendKind());
            // The custom hostname's first label is not a replacement credential account.
            Assertions.assertEquals("account", binding.toMap().get("AZURE_ACCOUNT_NAME"));
        } else {
            Assertions.assertThrows(StoragePropertiesException.class, () -> binding.validateAndNormalizeUri(uri));
        }
    }

    @Test
    void customOauthAccountHostWithoutAnExplicitEndpointDoesNotClaimHttp() {
        Map<String, String> raw = input("OAUTH2", "account.example.test");
        raw.remove("azure.endpoint");
        FileSystemProperties binding = new AzureFileSystemProvider().bind(raw);

        Assertions.assertFalse(binding.claimsUri("https://account.example.test/container/file"));
    }

    @Test
    void customEndpointAlsoWorksWithTypedVendedSasWithoutGuessingItsAccount() {
        AzureFileSystemProperties binding = new AzureFileSystemProvider().bindVended(
                Map.of("adls.sas-token." + ACCOUNT_HOST, TOKEN),
                Map.of("fs.azure.support", "true", "azure.endpoint", "https://proxy.blob.example.test:8443"))
                .orElseThrow();
        String uri = "https://proxy.blob.example.test:8443/container/file";

        Assertions.assertTrue(binding.claimsUri(uri));
        Assertions.assertEquals(uri, binding.validateAndNormalizeUri(uri));
        Assertions.assertEquals("account", binding.resolveBackendProperties(uri).orElseThrow()
                .toMap().get("AZURE_ACCOUNT_NAME"));
    }

    private static Map<String, String> input(String authType, String accountHost) {
        Map<String, String> raw = new HashMap<>(Map.of(
                "fs.azure.support", "true", "azure.auth_type", authType,
                "azure.endpoint", "https://" + accountHost,
                "azure.account_name", accountHost.substring(0, accountHost.indexOf('.')),
                "AZURE_CONTAINER", "container", "use_path_style", "true",
                "fs.azure.readaheadqueue.depth", "8", "fs.abfss.impl.disable.cache", "no"));
        switch (authType) {
            case "SHARED_KEY":
                raw.put("azure.account_key", "shared-key");
                break;
            case "SAS":
                raw.put("azure.sas_token", TOKEN);
                break;
            case "OAUTH2":
                raw.put("azure.oauth2_account_host", accountHost);
                raw.put("azure.oauth2_client_id", "client");
                raw.put("azure.oauth2_client_secret", "client-secret");
                raw.put("azure.oauth2_server_uri", TOKEN_ENDPOINT);
                break;
            default:
                throw new AssertionError("Unexpected test authentication type");
        }
        return raw;
    }
}
