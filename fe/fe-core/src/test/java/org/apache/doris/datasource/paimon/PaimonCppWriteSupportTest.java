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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPaimonTableDescriptor;
import org.apache.doris.thrift.TPaimonWriteMode;

import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PaimonCppWriteSupportTest {
    private static final List<String> COLUMNS = Arrays.asList("id", "name");
    private static final String SHREDDING_SCHEMA = "{\"type\":\"ROW\",\"fields\":[{\"id\":0,\"name\":\"name\","
            + "\"type\":{\"type\":\"ROW\",\"fields\":[{\"id\":1,\"name\":\"age\","
            + "\"type\":\"INT\"}]}}]}";
    private static final String SHREDDING_SCHEMA_WITHOUT_IDS =
            "{\"type\":\"ROW\",\"fields\":[{\"name\":\"name\","
            + "\"type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"age\",\"type\":\"INT\"}]}}]}";

    private String unsupportedReason(FileStoreTable table, List<String> columns, TPaimonWriteMode mode) {
        return unsupportedReason(table, columns, mode, Collections.emptyMap());
    }

    private String unsupportedReason(FileStoreTable table, List<String> columns, TPaimonWriteMode mode,
            Map<StorageProperties.Type, StorageProperties> storage) {
        return PaimonCppWriteSupport.decide(table, columns, mode, storage).getFallbackReason();
    }

    private PaimonCppWriteSupport.Decision decision(FileStoreTable table) {
        return PaimonCppWriteSupport.decide(
                table, COLUMNS, TPaimonWriteMode.APPEND, Collections.emptyMap());
    }

    private TPaimonTableDescriptor describe(FileStoreTable table) {
        return describe(table, Collections.emptyMap());
    }

    private TPaimonTableDescriptor describe(FileStoreTable table,
            Map<StorageProperties.Type, StorageProperties> storage) {
        TPaimonTableDescriptor descriptor = PaimonWriteBinding.describeTable(
                table, Collections.emptyMap(), Collections.emptyMap());
        descriptor.setStorage(PaimonCppWriteSupport.decide(
                table, COLUMNS, TPaimonWriteMode.APPEND, storage).getStorage());
        return descriptor;
    }

    private FileStoreTable table(Map<String, String> overrides, List<String> partitions,
            List<String> keys, String location) {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "-1");
        options.put("file.format", "parquet");
        options.putAll(overrides);
        TableSchema schema = new TableSchema(7,
                Arrays.asList(new DataField(0, "id", DataTypes.INT().notNull()),
                        new DataField(1, "name", DataTypes.STRING())),
                1, partitions, keys, options, null);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.options()).thenReturn(options);
        Mockito.when(table.location()).thenReturn(new Path(location));
        Mockito.when(table.catalogEnvironment()).thenReturn(CatalogEnvironment.empty());
        return table;
    }

    private FileStoreTable table(Map<String, String> overrides) {
        return table(overrides, Collections.emptyList(), Collections.emptyList(), "file:///tmp/paimon");
    }

    private FileStoreTable variantTable(Map<String, String> overrides, DataType type) {
        FileStoreTable table = table(overrides);
        TableSchema schema = new TableSchema(7,
                Arrays.asList(new DataField(0, "id", DataTypes.INT().notNull()), new DataField(1, "name", type)),
                1, Collections.emptyList(), Collections.emptyList(), table.options(), null);
        Mockito.when(table.schema()).thenReturn(schema);
        return table;
    }

    @Test
    public void testVariantDecisionWithoutChangingDescriptor() {
        for (Map<String, String> options : Arrays.asList(
                Collections.<String, String>emptyMap(),
                Collections.singletonMap("parquet.variant.shreddingSchema", SHREDDING_SCHEMA),
                Collections.singletonMap("variant.inferShreddingSchema", "true"))) {
            FileStoreTable table = variantTable(options, DataTypes.VARIANT());
            Assert.assertTrue(decision(table).isSupported());
            TableSchema restored = TableSchema.fromJson(describe(table).getSchemaJson());
            Assert.assertEquals(table.schema().fields(), restored.fields());
            Assert.assertEquals(table.options(), restored.options());
        }
    }

    @Test
    public void testVariantShreddingFallbackBoundaries() {
        Assert.assertNull(unsupportedReason(variantTable(Collections.emptyMap(), DataTypes.VARIANT()),
                COLUMNS, TPaimonWriteMode.APPEND));
        Map<String, String> options = new HashMap<>();
        options.put("parquet.variant.shreddingSchema", SHREDDING_SCHEMA);
        for (String format : Arrays.asList("orc", "avro")) {
            options.put("file.format", format);
            // This is not a Java-compatible fallback. Let the selected SDK report that VARIANT
            // is unsupported by this file format.
            Assert.assertTrue(decision(variantTable(options, DataTypes.VARIANT())).isSupported());
        }
        options.put("file.format", "parquet");
        Assert.assertTrue(decision(variantTable(
                options, DataTypes.ARRAY(DataTypes.VARIANT()))).isSupported());
        Assert.assertNull(unsupportedReason(
                variantTable(options, DataTypes.VARIANT()), COLUMNS, TPaimonWriteMode.OVERWRITE));
        Assert.assertNotNull(unsupportedReason(
                variantTable(options, DataTypes.VARIANT()), COLUMNS, TPaimonWriteMode.CHANGELOG));

        options.put("parquet.variant.shreddingSchema", SHREDDING_SCHEMA_WITHOUT_IDS);
        Assert.assertEquals("native VARIANT shredding schema requires explicit field IDs",
                unsupportedReason(variantTable(options, DataTypes.VARIANT()), COLUMNS,
                        TPaimonWriteMode.APPEND));
    }

    @Test
    public void testNestedVariantWithShreddingOptions() {
        DataType row = DataTypes.ROW(new DataField(2, "label", DataTypes.STRING()),
                new DataField(3, "payload", DataTypes.VARIANT()));
        DataType deep = DataTypes.ROW(new DataField(4, "deep",
                DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), row))));
        FileStoreTable table = variantTable(
                Collections.singletonMap("variant.inferShreddingSchema", "true"), deep);
        Assert.assertNull(unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertEquals(table.schema().fields(),
                TableSchema.fromJson(describe(table).getSchemaJson()).fields());
    }

    @Test
    public void testCppDataTypes() {
        DataType nested = DataTypes.ROW(
                new DataField(2, "amounts", DataTypes.ARRAY(DataTypes.DECIMAL(38, 10))),
                new DataField(3, "dates", DataTypes.MAP(DataTypes.STRING(), DataTypes.DATE())),
                new DataField(4, "event", DataTypes.ROW(
                        new DataField(5, "ntz", DataTypes.TIMESTAMP(6)),
                        new DataField(6, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)))));
        for (DataType type : Arrays.asList(
                DataTypes.CHAR(10), DataTypes.STRING(), DataTypes.BINARY(10), DataTypes.BYTES(),
                DataTypes.DECIMAL(38, 10), DataTypes.DATE(), DataTypes.TIMESTAMP(0),
                DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP(9),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(0),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9), nested)) {
            Assert.assertNull(type.toString(), unsupportedReason(
                    variantTable(Collections.emptyMap(), type), COLUMNS, TPaimonWriteMode.APPEND));
        }
        for (DataType type : Arrays.asList(DataTypes.TIME(), DataTypes.MULTISET(DataTypes.INT()),
                DataTypes.TIMESTAMP(1), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(8),
                DataTypes.ARRAY(DataTypes.TIME()))) {
            Assert.assertNotNull(type.toString(), unsupportedReason(
                    variantTable(Collections.emptyMap(), type), COLUMNS, TPaimonWriteMode.APPEND));
        }
    }

    @Test
    public void testLocalPathNormalization() {
        for (String location : Arrays.asList("/tmp/p", "file:///tmp/p")) {
            FileStoreTable table = table(Collections.emptyMap(), Collections.emptyList(),
                    Collections.emptyList(), location);
            Assert.assertNull(unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND));
            Path original = table.location();
            TPaimonTableDescriptor descriptor = describe(table);
            Assert.assertEquals(original.toString(), descriptor.getRootPath());
            Assert.assertEquals("/tmp/p", descriptor.getStorage().getRootPath());
        }
        FileStoreTable table = table(Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyList(), "file:/tmp/a b+%20?#");
        Assert.assertEquals("/tmp/a b+%20?#", describe(table).getStorage().getRootPath());
    }

    @Test
    public void testUnsupportedLocationComponents() {
        for (String location : Arrays.asList("file://host/tmp/p", "relative/p",
                "file:/tmp/p?query", "file:/tmp/p#fragment", "file:/tmp/%00p")) {
            FileStoreTable table = table(Collections.emptyMap());
            Mockito.when(table.location()).thenReturn(new Path(URI.create(location)));
            Assert.assertNotNull(location,
                    unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND));
        }
    }

    @Test
    public void testWriteFormatsOptionsAndModes() {
        for (String format : Arrays.asList("parquet", "orc", "avro", "blob")) {
            Assert.assertNull(unsupportedReason(
                    table(Collections.singletonMap("file.format", format)), COLUMNS, TPaimonWriteMode.APPEND));
        }
        for (String format : Arrays.asList("csv", "text", "json")) {
            Assert.assertNotNull(unsupportedReason(
                    table(Collections.singletonMap("file.format", format)), COLUMNS,
                    TPaimonWriteMode.APPEND));
        }
        Assert.assertNull(unsupportedReason(
                table(Collections.emptyMap()), COLUMNS, TPaimonWriteMode.OVERWRITE));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.singletonMap("file.format.per.level", "0:orc,3:blob")),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.singletonMap("changelog-producer", "input")),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.singletonMap("data-file.external-paths", "s3://bucket/external")),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.singletonMap("global-index.external-path", "s3://bucket/index")),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap()), COLUMNS, TPaimonWriteMode.CHANGELOG));
    }

    @Test
    public void testDorisObjectStorageRouting() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        properties.put("s3.access_key", "test-account");
        properties.put("s3.secret_key", "test-secret");
        Map<StorageProperties.Type, StorageProperties> storage = StorageProperties.createAll(properties).stream()
                .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
        FileStoreTable table = table(Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyList(), "s3://bucket/table");
        TPaimonTableDescriptor desc = describe(table, storage);
        Assert.assertEquals(TFileType.FILE_S3, desc.getStorage().getFileType());
        Assert.assertEquals("test-secret", desc.getStorage().getProperties().get("AWS_SECRET_KEY"));
        Assert.assertFalse(TableSchema.fromJson(desc.getSchemaJson()).options().containsKey("AWS_SECRET_KEY"));
    }

    @Test
    public void testMissingOrInvalidStorageFallsBack() throws Exception {
        FileStoreTable table = table(Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyList(), "s3://bucket/table");
        Assert.assertNotNull(unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND));
        StorageProperties properties = Mockito.mock(StorageProperties.class);
        Mockito.when(properties.validateAndNormalizeUri("s3://bucket/table"))
                .thenThrow(new IllegalArgumentException("secret configuration"));
        Map<StorageProperties.Type, StorageProperties> storage =
                Collections.singletonMap(StorageProperties.Type.S3, properties);
        String reason = unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND, storage);
        Assert.assertNotNull(reason);
        Assert.assertFalse(reason.contains("secret"));
        Map<String, String> hdfsOptions = new HashMap<>();
        hdfsOptions.put("fs.defaultFS", "hdfs://ns");
        Map<StorageProperties.Type, StorageProperties> hdfsStorage = StorageProperties.createAll(hdfsOptions).stream()
                .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
        FileStoreTable hdfsTable = table(Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyList(), "hdfs://ns/table");
        Assert.assertNull(unsupportedReason(
                hdfsTable, COLUMNS, TPaimonWriteMode.APPEND, hdfsStorage));
        Assert.assertEquals(TFileType.FILE_HDFS,
                describe(hdfsTable, hdfsStorage).getStorage().getFileType());
    }

    @Test
    public void testUnsupportedTableLayout() {
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap(), Collections.singletonList("id"), Collections.emptyList(), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.singletonMap("bucket", "-2"), Collections.emptyList(),
                        Collections.singletonList("id"), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.singletonMap("bucket", "1"), Collections.emptyList(),
                        Collections.singletonList("id"), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.singletonMap("bucket", "-1"), Collections.emptyList(),
                        Collections.singletonList("id"), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNull(unsupportedReason(
                table(Collections.emptyMap()), Arrays.asList("name", "id"), TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap()), Arrays.asList("name", "name"), TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap()), Arrays.asList("id", "missing"), TPaimonWriteMode.APPEND));
    }
}
