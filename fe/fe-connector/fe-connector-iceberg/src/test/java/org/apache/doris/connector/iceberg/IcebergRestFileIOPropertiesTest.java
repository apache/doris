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
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class IcebergRestFileIOPropertiesTest {
    private static final String SAS_KEY = "adls.sas-token.account.dfs.core.windows.net";
    private static final String HADOOP_KEY = "fs.azure.account.key.account.dfs.core.windows.net";

    @Test
    void staticClientAuthenticationIsNotMistakenForVendedCredentials() {
        IcebergRestFileIOProperties properties = new IcebergRestFileIOProperties(
                ConnectorStorageContext.NOOP, Map.of(SAS_KEY, "sig=static"));

        IcebergRestFileIOProperties.AuthenticationSelection selection =
                properties.selectAuthentication(Map.of(), List.of(), metadataLocation());

        Assertions.assertFalse(selection.vended());
        Assertions.assertEquals(Map.of(SAS_KEY, "sig=static"), selection.properties());
    }

    @Test
    void tableResponseAuthenticationIsMarkedAsVended() {
        IcebergRestFileIOProperties properties = new IcebergRestFileIOProperties(
                ConnectorStorageContext.NOOP, Map.of(SAS_KEY, "sig=static"));

        IcebergRestFileIOProperties.AuthenticationSelection selection =
                properties.selectAuthentication(Map.of(SAS_KEY, "sig=vended"), List.of(), metadataLocation());

        Assertions.assertTrue(selection.vended());
        Assertions.assertEquals(Map.of(SAS_KEY, "sig=vended"), selection.properties());
    }

    @Test
    void catalogPropertiesRemoveStaticAzureAuthenticationBeforeRestMerge() {
        IcebergRestFileIOProperties properties = new IcebergRestFileIOProperties(
                ConnectorStorageContext.NOOP, Map.of("warehouse", "abfss://container@account.dfs.core.windows.net",
                        SAS_KEY, "sig=static"));

        Assertions.assertEquals(Map.of("warehouse", "abfss://container@account.dfs.core.windows.net"),
                properties.catalogProperties());
    }

    @Test
    void staticHadoopFileIOKeepsItsConfiguredIdentity() {
        FileSystemProperties binding = azureBinding();
        IcebergRestFileIOProperties properties = new IcebergRestFileIOProperties(storage(binding), Map.of(
                CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName(),
                "adls.auth.shared-key.account.name", "account",
                "adls.auth.shared-key.account.key", "static-key"));
        LoadTableResponse response = tableResponse(Map.of());

        properties.withTableLoad(() -> {
            LoadTableResponse adapted = (LoadTableResponse) properties.adaptGet(response);
            Assertions.assertEquals(HadoopFileIO.class.getName(), adapted.config().get(CatalogProperties.FILE_IO_IMPL));
            HadoopFileIO fileIO = new HadoopFileIO(new Configuration(false));
            properties.configureTableFileIO(fileIO);
            Assertions.assertEquals("static-key", fileIO.getConf().get(HADOOP_KEY));
            return null;
        });
        // A refresh GET keeps the existing table-owned FileIO and must not rebind credentials.
        Assertions.assertSame(response, properties.adaptGet(response));
    }

    @Test
    void hadoopFileIORejectsAnActualVendedReplacement() {
        IcebergRestFileIOProperties properties = new IcebergRestFileIOProperties(storage(azureBinding()), Map.of(
                CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName()));

        Assertions.assertThrows(DorisConnectorException.class,
                () -> properties.withTableLoad(() -> properties.adaptGet(tableResponse(Map.of(SAS_KEY, "sig=vended")))));
    }

    @Test
    void newCredentialGenerationDoesNotInheritOldExpiry() {
        IcebergRestFileIOProperties properties = new IcebergRestFileIOProperties(storage(), Map.of());
        String expiryKey = "adls.sas-token-expires-at-ms.account.dfs.core.windows.net";
        LoadTableResponse first = (LoadTableResponse) properties.adapt(tableResponse(
                Map.of(SAS_KEY, "sig=first", expiryKey, "4102444800000")));
        LoadTableResponse next = (LoadTableResponse) properties.adapt(tableResponse(Map.of(SAS_KEY, "sig=next")));

        Assertions.assertEquals("4102444800000", first.config().get(expiryKey));
        Assertions.assertEquals("sig=next", next.config().get(SAS_KEY));
        Assertions.assertFalse(next.config().containsKey(expiryKey));
    }

    @Test
    void nonAzureResponseIsUnchanged() {
        IcebergRestFileIOProperties properties = new IcebergRestFileIOProperties(ConnectorStorageContext.NOOP, Map.of());
        LoadTableResponse response = tableResponse("s3://bucket/table", Map.of("s3.access-key-id", "s3-id"));

        Assertions.assertSame(response, properties.adapt(response));
    }

    private static FileSystemProperties azureBinding() {
        return new FileSystemProperties() {
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
            public Map<String, String> rawProperties() {
                return Map.of();
            }

            @Override
            public Map<String, String> matchedProperties() {
                return Map.of();
            }

            @Override
            public Set<String> getSupportedSchemes() {
                return Set.of("abfss");
            }

            @Override
            public Optional<HadoopStorageProperties> toHadoopProperties() {
                return Optional.of(() -> Map.of(HADOOP_KEY, "static-key"));
            }

            @Override
            public Map<String, String> toIcebergFileIOProperties() {
                return Map.of("adls.auth.shared-key.account.name", "account",
                        "adls.auth.shared-key.account.key", "static-key");
            }
        };
    }

    private static ConnectorStorageContext storage(FileSystemProperties... bindings) {
        return new ConnectorStorageContext() {
            @Override
            public List<StorageProperties> getStorageProperties() {
                return List.of(bindings);
            }

            @Override
            public List<StorageProperties> resolveStorageProperties(Map<String, String> credentials) {
                return getStorageProperties();
            }
        };
    }

    private static LoadTableResponse tableResponse(Map<String, String> config) {
        return tableResponse("abfss://container@account.dfs.core.windows.net/table", config);
    }

    private static LoadTableResponse tableResponse(String location, Map<String, String> config) {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), location, Map.of());
        return LoadTableResponse.builder().withTableMetadata(metadata).addAllConfig(config).build();
    }

    private static String metadataLocation() {
        return "abfss://container@account.dfs.core.windows.net/metadata.json";
    }
}
