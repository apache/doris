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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class IcebergRestFileIOPropertiesTest {
    private static final String ACCOUNT_ROOT = "abfss://container@account.dfs.core.windows.net/";
    private static final String METADATA_LOCATION = ACCOUNT_ROOT + "metadata/v1.metadata.json";
    private static final String SAS_PROPERTY = "adls.sas-token.account.dfs.core.windows.net";
    private static final String EXPIRY_PROPERTY = "adls.sas-token-expires-at-ms.account.dfs.core.windows.net";
    private static final String SAS = "si=test-policy&sig=test-scoped-signature";

    @Test
    void credentialsOnlySasUsesTheMetadataFileLocationAndPreservesItsScope() {
        Credential credential = sasCredential(ACCOUNT_ROOT + "metadata/", SAS);
        LoadTableResponse original = scopedTableResponse(METADATA_LOCATION, List.of(credential));

        LoadTableResponse adapted = (LoadTableResponse) azurePolicy(Map.of(), Map.of()).adapt(original);

        Assertions.assertEquals(SAS, adapted.config().get(SAS_PROPERTY),
                "official ADLSFileIO consumes properties, not the preserved scoped-credentials list");
        Assertions.assertEquals(METADATA_LOCATION, adapted.metadataLocation());
        Assertions.assertEquals(ACCOUNT_ROOT + "table", adapted.tableMetadata().location());
        Assertions.assertEquals(original.credentials(), adapted.credentials());
        Assertions.assertFalse(original.config().containsKey(SAS_PROPERTY));
    }

    @Test
    void credentialsOnlySasDoesNotInheritThePreviousStaticExpiry() {
        Map<String, String> previous = Map.of(SAS_PROPERTY, "sig=test-expired-signature", EXPIRY_PROPERTY, "1");
        LoadTableResponse original = scopedTableResponse(METADATA_LOCATION,
                List.of(sasCredential(ACCOUNT_ROOT + "metadata/", SAS)));

        LoadTableResponse adapted = (LoadTableResponse) azurePolicy(previous, Map.of()).adapt(original);

        Assertions.assertEquals(SAS, adapted.config().get(SAS_PROPERTY));
        Assertions.assertTrue(adapted.config().keySet().stream()
                .noneMatch(key -> key.startsWith("adls.sas-token-expires-at-ms.")),
                "the replacement has unknown expiry and must not reuse the previous generation's expiry");
        Assertions.assertFalse(adapted.config().containsValue("sig=test-expired-signature"));
        Assertions.assertEquals(original.credentials(), adapted.credentials());
        Assertions.assertEquals("1", previous.get(EXPIRY_PROPERTY));
    }

    @Test
    void explicitHadoopFileIoCannotIgnoreCredentialsOnlyAzureAuthentication() {
        LoadTableResponse response = scopedTableResponse(METADATA_LOCATION,
                List.of(sasCredential(ACCOUNT_ROOT + "metadata/", SAS)));
        IcebergRestFileIOProperties policy = azurePolicy(Map.of(),
                Map.of("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO"));

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> policy.adapt(response));

        Assertions.assertTrue(failure.getMessage().contains("HadoopFileIO"));
        Assertions.assertFalse(failure.getMessage().contains(SAS));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void differentAzureSasIdentitiesForTheSameAccountCannotBecomeOneFileIo(boolean differentScope) {
        List<Credential> credentials = List.of(
                sasCredential(ACCOUNT_ROOT + "metadata/", SAS),
                sasCredential(differentScope ? ACCOUNT_ROOT : ACCOUNT_ROOT + "metadata/",
                        "sig=test-other-scoped-signature"));
        LoadTableResponse response = scopedTableResponse(METADATA_LOCATION, credentials);

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> azurePolicy(Map.of(), Map.of()).adapt(response));

        Assertions.assertFalse(failure.getMessage().contains(SAS));
        Assertions.assertFalse(failure.getMessage().contains("test-other-scoped-signature"));
        Assertions.assertEquals(credentials, response.credentials());
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3://other-bucket/metadata/v1.metadata.json", METADATA_LOCATION})
    void dataOnlyAzureCredentialDoesNotBecomeTheMetadataIdentity(String metadataLocation) {
        LoadTableResponse original = scopedTableResponse(metadataLocation,
                List.of(sasCredential(ACCOUNT_ROOT + "table/", SAS)));

        LoadTableResponse adapted = (LoadTableResponse) azurePolicy(Map.of(), Map.of()).adapt(original);

        Assertions.assertFalse(adapted.config().containsKey(SAS_PROPERTY),
                "the table data location cannot stand in for the metadata-file credential scope");
        Assertions.assertEquals(metadataLocation, adapted.metadataLocation());
        Assertions.assertEquals(original.credentials(), adapted.credentials());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void responseAdaptationPreservesMetadataLocationAndEveryCredentialScope(boolean stagedCreate) {
        TableMetadata metadata = tableResponse().tableMetadata();
        if (!stagedCreate) {
            metadata = TableMetadataParser.fromJson(
                    "abfss://container@account.dfs.core.windows.net/metadata/v1.metadata.json",
                    TableMetadataParser.toJson(metadata));
        }
        List<Credential> credentials = List.of(
                ImmutableCredential.builder().prefix("abfss://container@account.dfs.core.windows.net/table/")
                        .putConfig("adls.token", "test-scoped-data-token").build(),
                ImmutableCredential.builder().prefix("s3://other-bucket/metadata/")
                        .putConfig("s3.session-token", "test-scoped-metadata-token").build());
        LoadTableResponse original = LoadTableResponse.builder().withTableMetadata(metadata)
                .addAllCredentials(credentials).build();

        LoadTableResponse adapted = (LoadTableResponse) policy().adapt(original);

        Assertions.assertEquals(original.metadataLocation(), adapted.metadataLocation());
        Assertions.assertEquals(original.tableMetadata().location(), adapted.tableMetadata().location());
        Assertions.assertEquals(credentials, adapted.credentials());
        Assertions.assertEquals("test-sdk-token", adapted.config().get("adls.token"),
                "scoped credentials remain for the SDK instead of being flattened into a table-wide identity");
    }

    @Test
    void onlyInitialTableGetAdaptsFileIoProperties() {
        IcebergRestFileIOProperties policy = policy();
        LoadTableResponse response = tableResponse();

        Assertions.assertSame(response, policy.adaptGet(response));
        LoadTableResponse loaded = (LoadTableResponse) policy.withTableLoad(() -> policy.adaptGet(response));

        Assertions.assertNotSame(response, loaded);
        Assertions.assertEquals("test-sdk-token", loaded.config().get("adls.token"));
        Assertions.assertSame(response.tableMetadata(), loaded.tableMetadata());
        Assertions.assertSame(response, policy.adaptGet(response));
    }

    @Test
    void creationResponseDoesNotRequireATableGetScope() {
        LoadTableResponse loaded = (LoadTableResponse) policy().adapt(tableResponse());

        Assertions.assertEquals("test-sdk-token", loaded.config().get("adls.token"));
    }

    @Test
    void nestedLoadRestoresTheOuterScopeAndRemovesItAfterFailure() {
        IcebergRestFileIOProperties policy = policy();
        LoadTableResponse response = tableResponse();
        IllegalArgumentException failure = new IllegalArgumentException("test load failure");

        Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                () -> policy.withTableLoad(() -> {
                    Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                            () -> policy.withTableLoad(() -> {
                                throw failure;
                            })));
                    Assertions.assertNotSame(response, policy.adaptGet(response));
                    throw failure;
                })));

        Assertions.assertSame(response, policy.adaptGet(response));
    }

    @Test
    void catalogScopesAreIndependentOnTheSameThread() {
        IcebergRestFileIOProperties first = policy();
        IcebergRestFileIOProperties second = policy();
        LoadTableResponse response = tableResponse();

        first.withTableLoad(() -> {
            Assertions.assertNotSame(response, first.adaptGet(response));
            Assertions.assertSame(response, second.adaptGet(response));
            return null;
        });

        Assertions.assertSame(response, first.adaptGet(response));
    }

    @Test
    void loadScopeDoesNotPropagateToAnotherThread() {
        IcebergRestFileIOProperties policy = policy();
        LoadTableResponse response = tableResponse();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            policy.withTableLoad(() -> {
                Assertions.assertSame(response, Assertions.assertDoesNotThrow(
                        () -> executor.submit(() -> policy.adaptGet(response)).get(5, TimeUnit.SECONDS)));
                return null;
            });
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void configurationIsAdaptedOutsideTableLoadWithoutRemovingRestAuthentication() {
        IcebergRestFileIOProperties policy = policy();
        ConfigResponse original = ConfigResponse.builder().withOverride("adls.token", "test-override-token")
                .withOverride("token", "test-rest-token").withDefault("fixture-option", "default").build();

        ConfigResponse adapted = (ConfigResponse) policy.adaptGet(original);

        Assertions.assertFalse(adapted.overrides().containsKey("adls.token"));
        Assertions.assertEquals("test-rest-token", adapted.overrides().get("token"));
        Assertions.assertEquals(original.defaults(), adapted.defaults());
        LoadTableResponse loaded = (LoadTableResponse) policy.withTableLoad(() -> policy.adaptGet(tableResponse()));
        Assertions.assertEquals("test-override-token", loaded.config().get("adls.token"));
        Assertions.assertEquals("test-rest-token", loaded.config().get("token"));
    }

    private static IcebergRestFileIOProperties policy() {
        // Raw SDK tokens belong only to Java FileIO; this test needs no native storage provider.
        return new IcebergRestFileIOProperties(ConnectorStorageContext.NOOP, Map.of("adls.token", "test-sdk-token"));
    }

    private static IcebergRestFileIOProperties azurePolicy(Map<String, String> staticAuthentication,
            Map<String, String> clientProperties) {
        // The engine/provider side is an external SPI boundary for this connector module. The
        // real provider's credential parsing and group replacement have separate integration tests.
        ConnectorStorageContext storage = new ConnectorStorageContext() {
            @Override
            public List<StorageProperties> getStorageProperties() {
                return List.of(new AzureFileIoProperties(staticAuthentication));
            }

            @Override
            public List<StorageProperties> resolveStorageProperties(Map<String, String> rawVendedCredentials) {
                return List.of(new AzureFileIoProperties(rawVendedCredentials.isEmpty()
                        ? staticAuthentication : rawVendedCredentials));
            }
        };
        return new IcebergRestFileIOProperties(storage, clientProperties);
    }

    private static Credential sasCredential(String prefix, String token) {
        return ImmutableCredential.builder().prefix(prefix).putConfig(SAS_PROPERTY, token).build();
    }

    private static LoadTableResponse scopedTableResponse(String metadataLocation, List<Credential> credentials) {
        TableMetadata metadata = TableMetadataParser.fromJson(metadataLocation,
                TableMetadataParser.toJson(tableResponse().tableMetadata()));
        return LoadTableResponse.builder().withTableMetadata(metadata).addAllCredentials(credentials).build();
    }

    private static final class AzureFileIoProperties implements FileSystemProperties {
        private final Map<String, String> authentication;

        private AzureFileIoProperties(Map<String, String> authentication) {
            this.authentication = Map.copyOf(authentication);
        }

        @Override
        public String providerName() {
            return "AZURE";
        }

        @Override
        public StorageKind kind() {
            return StorageKind.OBJECT_STORAGE;
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.AZURE;
        }

        @Override
        public Set<String> getSupportedSchemes() {
            return Set.of("abfs", "abfss", "wasb", "wasbs");
        }

        @Override
        public Map<String, String> rawProperties() {
            return authentication;
        }

        @Override
        public Map<String, String> matchedProperties() {
            return authentication;
        }

        @Override
        public Map<String, String> toIcebergFileIOProperties() {
            return authentication;
        }
    }

    private static LoadTableResponse tableResponse() {
        TableMetadata metadata = TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned(), "abfss://container@account.dfs.core.windows.net/table", Map.of());
        return LoadTableResponse.builder().withTableMetadata(metadata).build();
    }
}
