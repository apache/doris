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

import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

class AzureCredentialScopeTest {
    private static final String ABFS_ROOT = "abfss://container@account.dfs.core.windows.net/";
    private static final String HTTPS_ROOT = "https://account.blob.core.windows.net/container/";

    @ParameterizedTest
    @CsvSource({
            "data/file.parquet, data/file.parquet, true",
            "data/partition/file.parquet, data, true",
            "database/file.parquet, data, false",
            "data, data/, false",
            "data/, data/, true",
            "data/file.parquet, data/, true",
            "Data/file.parquet, data/, false"
    })
    void matchesOnlyTheSameObjectOrDescendants(String key, String prefix, boolean matches) {
        AzureFileSystemProperties properties = sasProperties();

        Assertions.assertEquals(matches, properties.matchesLocationPrefix(ABFS_ROOT + key, ABFS_ROOT + prefix));
        Assertions.assertEquals(matches, properties.matchesLocationPrefix(HTTPS_ROOT + key, HTTPS_ROOT + prefix));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "abfss://container@account.dfs.core.windows.net",
            "abfss://container@account.dfs.core.windows.net/",
            "https://account.blob.core.windows.net/container",
            "https://account.blob.core.windows.net/container/"
    })
    void containerRootIncludesEveryObject(String prefix) {
        AzureFileSystemProperties properties = sasProperties();

        Assertions.assertTrue(properties.matchesLocationPrefix(ABFS_ROOT + "data/file.parquet", prefix));
        Assertions.assertTrue(properties.matchesLocationPrefix(ABFS_ROOT, prefix));
    }

    @Test
    void literalLeadingSlashIsNotTheContainerRoot() {
        AzureFileSystemProperties properties = sasProperties();

        Assertions.assertFalse(properties.matchesLocationPrefix(ABFS_ROOT + "data/file", ABFS_ROOT + "/"));
        Assertions.assertTrue(properties.matchesLocationPrefix(ABFS_ROOT + "/data/file", ABFS_ROOT + "/"));
    }

    @Test
    void differentContainersNeverShareAScope() {
        AzureFileSystemProperties properties = sasProperties();
        String otherContainer = "abfss://other-container@account.dfs.core.windows.net/data/";

        Assertions.assertFalse(properties.matchesLocationPrefix(ABFS_ROOT + "data/file", otherContainer));
        Assertions.assertFalse(properties.matchesLocationPrefix(otherContainer + "file", ABFS_ROOT + "data/"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "abfss://container@other.dfs.core.windows.net/data/",
            "abfss://container@account.dfs.core.chinacloudapi.cn/data/"
    })
    void bothLocationsMustBelongToTheBoundAccountAndCloud(String otherIdentity) {
        AzureFileSystemProperties properties = sasProperties();

        Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.matchesLocationPrefix(ABFS_ROOT + "data/file", otherIdentity));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.matchesLocationPrefix(otherIdentity + "file", ABFS_ROOT + "data/"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "abfs://container@account.dfs.core.windows.net/data/file",
            "abfss://container@account.dfs.core.windows.net/data/file",
            "wasb://container@account.blob.core.windows.net/data/file",
            "wasbs://container@account.blob.core.windows.net/data/file",
            "ABFSS://container@ACCOUNT.DFS.CORE.WINDOWS.NET/data/file",
            "https://account.blob.core.windows.net/container/data/file"
    })
    void dfsAndBlobSpellingsUseTheSameStorageIdentity(String location) {
        Assertions.assertTrue(sasProperties().matchesLocationPrefix(location, ABFS_ROOT + "data/"));
    }

    @ParameterizedTest
    @CsvSource({
            "data/a%2Fb, data/a%252Fb, true",
            "data/a%2Fb, data/a%2Fb, false",
            "data/a%2Fb, data/a%25252Fb, false",
            "data/a/b, data/a%2Fb, true",
            "data/a+b, data/a+b, true",
            "data/a+b, data/a%2Bb, true",
            "data/a+b, data/a%20b, false",
            "data/a%2Bb, data/a%252Bb, true"
    })
    void httpsDecodesOnceAndAbfsKeepsPercentSequencesLiteral(
            String literalKey, String encodedKey, boolean matches) {
        AzureFileSystemProperties properties = sasProperties();

        Assertions.assertEquals(matches, properties.matchesLocationPrefix(
                HTTPS_ROOT + encodedKey + "/file", ABFS_ROOT + literalKey + "/"));
        Assertions.assertEquals(matches, properties.matchesLocationPrefix(
                ABFS_ROOT + literalKey + "/file", HTTPS_ROOT + encodedKey + "/"));
    }

    @ParameterizedTest
    @CsvSource({
            "data//file, data//, true",
            "data/file, data//, false",
            "data/./file, data/./, true",
            "data/file, data/./, false",
            "data/../file, data/, true",
            "data/../file, data/../, true",
            "file, data/../, false",
            "data/http://example/file, data/http://example/, true",
            "data/http:/example/file, data/http://example/, false"
    })
    void separatorsAndDotSegmentsRemainObjectNameCharacters(String key, String prefix, boolean matches) {
        AzureFileSystemProperties properties = sasProperties();

        Assertions.assertEquals(matches, properties.matchesLocationPrefix(ABFS_ROOT + key, ABFS_ROOT + prefix));
        Assertions.assertEquals(matches, properties.matchesLocationPrefix(HTTPS_ROOT + key, HTTPS_ROOT + prefix));
    }

    @Test
    void scopeComparisonDoesNotAccessAnExpiredCredential() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS", "azure.account_name", "account",
                "azure.sas_token", "sig=scope-test", "azure.sas_expiry_ms", "1"),
                Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC));

        Assertions.assertThrows(StoragePropertiesException.class, properties::validateForAccess);
        Assertions.assertTrue(properties.matchesLocationPrefix(ABFS_ROOT + "data/file", ABFS_ROOT + "data/"));
        Assertions.assertFalse(properties.matchesLocationPrefix(ABFS_ROOT + "other/file", ABFS_ROOT + "data/"));
        Assertions.assertThrows(StoragePropertiesException.class, properties::validateForAccess);
    }

    @Test
    void legacySharedKeyLocationsUseTheBindingAccountAndKeepTheirExistingDecoding() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "account", "azure.account_key", "scope-test-key"));

        Assertions.assertTrue(properties.matchesLocationPrefix("s3://container/data/a%252Fb/file",
                ABFS_ROOT + "data/a%2Fb/"));
        Assertions.assertFalse(properties.matchesLocationPrefix("s3://container/data/a/b/file",
                ABFS_ROOT + "data/a%2Fb/"));
        Assertions.assertTrue(properties.matchesLocationPrefix(ABFS_ROOT + "data/file", "s3://container/data/"));
    }

    @Test
    void customHttpsEndpointUsesItsBoundAccountForAnAbfsCredentialPrefix() {
        AzureFileSystemProperties properties = customEndpointProperties();
        String location = "https://proxy.blob.example.test:8443/container/data/file";
        Assertions.assertEquals(location, properties.validateAndNormalizeUri(location));

        Assertions.assertTrue(properties.matchesLocationPrefix(location, ABFS_ROOT + "data/"));
        Assertions.assertTrue(properties.matchesLocationPrefix(ABFS_ROOT + "data/file",
                "https://proxy.blob.example.test:8443/container/data/"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://other.blob.example.test:8443",
            "https://proxy.blob.example.test.other.invalid:8443",
            "https://proxy.blob.example.test:443",
            "http://proxy.blob.example.test:8443"
    })
    void customEndpointScopeStillRequiresTheExactHttpOrigin(String otherOrigin) {
        AzureFileSystemProperties properties = customEndpointProperties();

        Assertions.assertThrows(StoragePropertiesException.class, () -> properties.matchesLocationPrefix(
                otherOrigin + "/container/data/file", ABFS_ROOT + "data/"));
        Assertions.assertThrows(StoragePropertiesException.class, () -> properties.matchesLocationPrefix(
                ABFS_ROOT + "data/file", otherOrigin + "/container/data/"));
    }

    @Test
    void customEndpointScopeKeepsContainerAndObjectBoundaries() {
        AzureFileSystemProperties properties = customEndpointProperties();

        Assertions.assertFalse(properties.matchesLocationPrefix(
                "https://proxy.blob.example.test:8443/container/database/file", ABFS_ROOT + "data/"));
        Assertions.assertFalse(properties.matchesLocationPrefix(
                "https://proxy.blob.example.test:8443/other/data/file", ABFS_ROOT + "data/"));
    }

    @Test
    void anUnrecognizedAbfsAuthorityIsNotAnAliasForTheBoundAccount() {
        AzureFileSystemProperties properties = customEndpointProperties();
        String otherRoot = "abfss://container@account.example.test/";

        Assertions.assertFalse(properties.matchesLocationPrefix(otherRoot + "data/file", ABFS_ROOT + "data/"));
        Assertions.assertFalse(properties.matchesLocationPrefix(ABFS_ROOT + "data/file", otherRoot + "data/"));
    }

    private static AzureFileSystemProperties customEndpointProperties() {
        return new AzureFileSystemProvider().bindVended(
                Map.of("adls.sas-token.account.dfs.core.windows.net", "sig=scope-test"),
                Map.of("fs.azure.support", "true", "azure.endpoint", "https://proxy.blob.example.test:8443"))
                .orElseThrow();
    }

    private static AzureFileSystemProperties sasProperties() {
        return AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS", "azure.account_name", "account",
                "azure.sas_token", "sig=scope-test"));
    }
}
