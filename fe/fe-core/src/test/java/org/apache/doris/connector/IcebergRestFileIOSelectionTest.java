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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

class IcebergRestFileIOSelectionTest {

    @Test
    void staticClientSecretUsesHadoopMetadataAndNativeAzureData() throws Exception {
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(clientSecretProperties())) {
            Table table = fixture.load();

            HadoopFileIO fileIO = Assertions.assertInstanceOf(HadoopFileIO.class, table.io());
            Assertions.assertEquals("OAuth",
                    fileIO.getConf().get("fs.azure.account.auth.type.account.dfs.core.windows.net"));
            Assertions.assertEquals("test-client-secret",
                    fileIO.getConf().get("fs.azure.account.oauth2.client.secret.account.dfs.core.windows.net"));
            ConnectorStorageAccess access = fixture.storageContext().newStorageAccessResolver(fileIO.properties())
                    .apply("abfss://container@account.dfs.core.windows.net/table/data.parquet");
            Assertions.assertEquals("FILE_S3", access.getBackendFileType());
            Assertions.assertEquals("OAUTH2", access.getBackendProperties().get("AZURE_AUTH_TYPE"));
            Assertions.assertFalse(access.getBackendProperties().keySet().stream()
                    .anyMatch(key -> key.startsWith("fs.")));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"org.apache.iceberg.azure.adlsv2.ADLSFileIO", "org.apache.iceberg.io.ResolvingFileIO"})
    void explicitNativeFileIoCannotSilentlyReplaceTheConfiguredClientSecretIdentity(String ioImpl) throws Exception {
        Map<String, String> properties = new HashMap<>(clientSecretProperties());
        properties.put("io-impl", ioImpl);
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(properties)) {
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class, fixture::load);

            Assertions.assertTrue(failure.getMessage().contains("client-secret"));
            Assertions.assertTrue(failure.getMessage().contains("HadoopFileIO"));
            Assertions.assertFalse(failure.getMessage().contains("test-client-secret"));
        }
    }

    private static Map<String, String> clientSecretProperties() {
        return Map.of("fs.azure.support", "true", "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "account.dfs.core.windows.net",
                "azure.oauth2_client_id", "test-client", "azure.oauth2_client_secret", "test-client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/test-tenant/oauth2/token");
    }

    @Test
    void explicitHadoopFileIoCannotIgnoreNewVendedAzureAuthentication() throws Exception {
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(Map.of(
                        "azure.account_name", "account", "azure.account_key", "a2V5LWE=",
                        "io-impl", HadoopFileIO.class.getName()))) {
            fixture.tableConfig(Map.of("adls.sas-token.account.dfs.core.windows.net", "sig=fresh-test-signature"));

            RuntimeException failure = Assertions.assertThrows(RuntimeException.class, fixture::load);

            Assertions.assertTrue(failure.getMessage().contains("HadoopFileIO"));
            Assertions.assertTrue(failure.getMessage().contains("cannot consume"));
            Assertions.assertFalse(failure.getMessage().contains("fresh-test-signature"));
        }
    }

    @Test
    void newSharedKeyRetainsConnectionsWithoutAccessingTheExpiredSasItReplaced() throws Exception {
        String endpoint = "https://account.blob.core.windows.net:10443/proxy%2Fpath";
        ConfigResponse serverConfig = ConfigResponse.builder()
                .withOverride("adls.auth.shared-key.account.name", "account")
                .withOverride("adls.auth.shared-key.account.key", "a2V5LWI=").build();
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(Map.of(
                        "azure.account_name", "account", "azure.sas_token", "sig=expired-test-signature",
                        "azure.sas_expiry_ms", "1", "azure.endpoint", endpoint), serverConfig)) {
            Table table = fixture.load();

            Assertions.assertEquals(endpoint,
                    table.io().properties().get("adls.connection-string.account.blob.core.windows.net"));
            Assertions.assertTrue(table.io().properties().keySet().stream()
                    .noneMatch(key -> key.startsWith("adls.sas-token")));
            // The fixture overrides only the DFS transport; the provider's Blob endpoint above
            // must remain intact. The actual official FileIO still reads using the new SharedKey.
            TableMetadata actual = TableMetadataParser.read(table.io(),
                    "abfss://container@account.dfs.core.windows.net/table/v1.metadata.json");
            Assertions.assertEquals(table.schema().asStruct(), actual.schema().asStruct());
        }
    }

    @Test
    void serverSelectedHadoopFileIORetainsStaticSasConfiguration() throws Exception {
        String sas = "si=stored-access-policy&sig=static-test-signature";
        ConfigResponse serverConfig = ConfigResponse.builder()
                .withOverride("io-impl", HadoopFileIO.class.getName()).build();
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(Map.of(
                        "azure.account_name", "account", "azure.sas_token", sas), serverConfig)) {
            Table table = fixture.load();

            HadoopFileIO fileIO = Assertions.assertInstanceOf(HadoopFileIO.class, table.io());
            Assertions.assertEquals("SAS",
                    fileIO.getConf().get("fs.azure.account.auth.type.account.dfs.core.windows.net"));
            Assertions.assertEquals(sas, fileIO.getConf().get("fs.azure.sas.fixed.token.account.dfs.core.windows.net"));
        }
    }

    @Test
    void tableSelectedHadoopFileIORetainsStaticSasConfiguration() throws Exception {
        String sas = "si=stored-access-policy&sig=static-test-signature";
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(Map.of(
                        "azure.account_name", "account", "azure.sas_token", sas))) {
            fixture.tableConfig(Map.of("io-impl", HadoopFileIO.class.getName()));

            Table table = fixture.load();

            HadoopFileIO fileIO = Assertions.assertInstanceOf(HadoopFileIO.class, table.io());
            Assertions.assertEquals("SAS",
                    fileIO.getConf().get("fs.azure.account.auth.type.account.dfs.core.windows.net"));
            Assertions.assertEquals(sas, fileIO.getConf().get("fs.azure.sas.fixed.token.account.dfs.core.windows.net"));

            Configuration originalConf = fileIO.getConf();
            HadoopFileIO reloadedFileIO = Assertions.assertInstanceOf(HadoopFileIO.class, fixture.reload().io());
            Assertions.assertNotSame(fileIO, reloadedFileIO);
            Assertions.assertNotSame(originalConf, reloadedFileIO.getConf());
            Assertions.assertSame(originalConf, fileIO.getConf());
            Assertions.assertEquals(sas, originalConf.get("fs.azure.sas.fixed.token.account.dfs.core.windows.net"));
            Assertions.assertEquals(sas,
                    reloadedFileIO.getConf().get("fs.azure.sas.fixed.token.account.dfs.core.windows.net"));

            ConnectorStorageAccess access = fixture.storageContext().newStorageAccessResolver(fileIO.properties())
                    .apply("abfss://container@account.dfs.core.windows.net/table/data.parquet");
            Assertions.assertEquals("FILE_S3", access.getBackendFileType());
            Assertions.assertEquals("SAS", access.getBackendProperties().get("AZURE_AUTH_TYPE"));
        }
    }

    @Test
    void explicitHadoopConfigurationOverridesProviderDefaults() throws Exception {
        String sasProperty = "fs.azure.sas.fixed.token.account.dfs.core.windows.net";
        String hadoopSas = "si=stored-access-policy&sig=explicit-hadoop-test-signature";
        ConfigResponse serverConfig = ConfigResponse.builder()
                .withOverride("io-impl", HadoopFileIO.class.getName()).build();
        try (IcebergAzureFileIOIntegrationTest.RestFixture fixture =
                new IcebergAzureFileIOIntegrationTest.RestFixture(Map.of(
                        "azure.account_name", "account", "azure.sas_token", "sig=provider-test-signature",
                        sasProperty, hadoopSas), serverConfig)) {
            Table table = fixture.load();

            HadoopFileIO fileIO = Assertions.assertInstanceOf(HadoopFileIO.class, table.io());
            Assertions.assertEquals(hadoopSas, fileIO.getConf().get(sasProperty),
                    "the static provider view must not overwrite an explicit Hadoop option");
        }
    }
}
