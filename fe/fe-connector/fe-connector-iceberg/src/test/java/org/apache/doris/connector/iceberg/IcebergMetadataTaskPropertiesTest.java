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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

class IcebergMetadataTaskPropertiesTest {
    private static final String HOST = "account.dfs.core.windows.net";
    private static final String TABLE_ROOT = "abfss://container@" + HOST + "/table";
    private static final String TOKEN_KEY = "adls.sas-token." + HOST;
    private static final String EXPIRY_KEY = "adls.sas-token-expires-at-ms." + HOST;
    private static final String ENDPOINT_KEY = "adls.connection-string." + HOST;
    private static final String FIXED_SAS = "sig=expiry-test&se=2030-01-01T00:00:00Z";

    @Test
    void fixedSasExpiryTravelsWithTheColdTaskWithoutCopyingItsCredential() throws Exception {
        try (ResolvingFileIO fileIO = new ResolvingFileIO()) {
            fileIO.initialize(Map.of(TOKEN_KEY, "sig=task-only-test-token", EXPIRY_KEY, "4102444800000"));
            FileScanTask task = metadataTask(fileIO);

            Long expiry = IcebergMetadataTaskProperties.fixedSasExpiryMs(fileIO, task);

            Assertions.assertEquals(4102444800000L, expiry);
            IcebergScanRange range = new IcebergScanRange.Builder()
                    .path("/dummyPath").serializedSplit(SerializationUtil.serializeToBase64(task))
                    .fileIoExpiryMs(expiry).build();
            TTableFormatFileDesc format = new TTableFormatFileDesc();
            range.populateRangeParams(format, new TFileRangeDesc());
            Assertions.assertEquals(expiry.longValue(), format.getIcebergParams().getFileIoExpiryMs());
            Assertions.assertTrue(range.getProperties().isEmpty());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("expiryCases")
    void onlyTheTasksEffectiveFixedCredentialSetsItsExpiry(String description,
            Map<String, String> properties, Long expected) throws Exception {
        try (ResolvingFileIO fileIO = new ResolvingFileIO()) {
            fileIO.initialize(properties);
            Assertions.assertEquals(expected, IcebergMetadataTaskProperties.fixedSasExpiryMs(
                    fileIO, metadataTask(fileIO)), description);
        }
    }

    private static Stream<Arguments> expiryCases() {
        return Stream.of(
                Arguments.of("embedded expiry", Map.of(TOKEN_KEY, FIXED_SAS), 1893456000000L),
                Arguments.of("earlier embedded expiry wins", Map.of(TOKEN_KEY, FIXED_SAS,
                        EXPIRY_KEY, "4102444800000"), 1893456000000L),
                Arguments.of("earlier explicit expiry wins", Map.of(TOKEN_KEY, FIXED_SAS, EXPIRY_KEY, "1"), 1L),
                Arguments.of("stored policy has unknown expiry", Map.of(TOKEN_KEY, "si=policy&sig=test"), null),
                Arguments.of("unrelated account ignored", Map.of("adls.sas-token.other.dfs.core.windows.net",
                        "sig=other", "adls.sas-token-expires-at-ms.other.dfs.core.windows.net", "1"), null),
                Arguments.of("expiry without SAS ignored", Map.of(EXPIRY_KEY, "1"), null),
                Arguments.of("endpoint SAS replaces property SAS", Map.of(TOKEN_KEY, FIXED_SAS,
                        EXPIRY_KEY, "1", ENDPOINT_KEY,
                        "https://account.blob.core.windows.net?sig=endpoint&se=2100-01-01T00:00:00Z"), 4102444800000L),
                Arguments.of("endpoint policy does not inherit expiry", Map.of(TOKEN_KEY, FIXED_SAS,
                        EXPIRY_KEY, "1", ENDPOINT_KEY, "https://account.blob.core.windows.net?si=policy&sig=test"), null),
                Arguments.of("connection endpoint retains property SAS", Map.of(TOKEN_KEY, FIXED_SAS,
                        ENDPOINT_KEY, "https://account.blob.core.windows.net"), 1893456000000L),
                Arguments.of("absolute refresh endpoint", Map.of(TOKEN_KEY, FIXED_SAS,
                        "adls.refresh-credentials-endpoint", "https://catalog.invalid/credentials"), null),
                Arguments.of("relative refresh endpoint", Map.of(TOKEN_KEY, FIXED_SAS,
                        "uri", "https://catalog.invalid", "adls.refresh-credentials-endpoint", "/credentials"), null),
                Arguments.of("disabled refresh retains fixed expiry", Map.of(TOKEN_KEY, FIXED_SAS,
                        "adls.refresh-credentials-endpoint", "https://catalog.invalid/credentials",
                        "adls.refresh-credentials-enabled", "false"), 1893456000000L));
    }

    @Test
    void malformedExpiryIsRejectedWithoutEchoingCredentialMaterial() throws Exception {
        try (ResolvingFileIO fileIO = new ResolvingFileIO()) {
            fileIO.initialize(Map.of(TOKEN_KEY, FIXED_SAS, EXPIRY_KEY, "sensitive-invalid-input"));
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> IcebergMetadataTaskProperties.fixedSasExpiryMs(fileIO, metadataTask(fileIO)));
            Assertions.assertEquals("Invalid Iceberg FileIO SAS expiry", error.getMessage());
            Assertions.assertNull(error.getCause());
        }
    }

    @Test
    void otherFileIoKeepsItsOwnCredentialLifecycle() throws Exception {
        try (InMemoryFileIO fileIO = new InMemoryFileIO()) {
            Assertions.assertNull(IcebergMetadataTaskProperties.fixedSasExpiryMs(fileIO, metadataTask(fileIO)));
        }
    }

    private static FileScanTask metadataTask(FileIO fileIO) throws Exception {
        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize("expiry-test", Map.of());
            catalog.createNamespace(Namespace.of("db"));
            Table source = catalog.createTable(TableIdentifier.of("db", "t"),
                    new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                    PartitionSpec.unpartitioned(), TABLE_ROOT, Map.of());
            source.newFastAppend().appendFile(DataFiles.builder(source.spec())
                    .withPath(TABLE_ROOT + "/data/one.parquet")
                    .withFileSizeInBytes(16).withRecordCount(3).build()).commit();
            Table table = new BaseTable(new StaticTableOperations(
                    ((BaseTable) source).operations().current(), fileIO), source.name());
            Table metadata = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ALL_MANIFESTS);
            try (CloseableIterable<FileScanTask> tasks = metadata.newScan().select("path").planFiles();
                    CloseableIterator<FileScanTask> iterator = tasks.iterator()) {
                FileScanTask task = iterator.next();
                Assertions.assertFalse(iterator.hasNext());
                return task;
            }
        }
    }
}
