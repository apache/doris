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

    private String unsupportedReason(FileStoreTable table, List<String> columns, TPaimonWriteMode mode) {
        return unsupportedReason(table, columns, mode, Collections.emptyMap());
    }

    private String unsupportedReason(FileStoreTable table, List<String> columns, TPaimonWriteMode mode,
            Map<StorageProperties.Type, StorageProperties> storage) {
        return PaimonCppWriteSupport.decide(table, columns, mode, storage).getFallbackReason();
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
        options.put("write-only", "true");
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

    @Test
    public void testLocalPathNormalization() {
        for (String location : Arrays.asList("/tmp/p", "file:/tmp/p", "file:///tmp/p", "FILE:/tmp/p")) {
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
    public void testUnsupportedOptionsAndModes() {
        Assert.assertNull(unsupportedReason(
                table(Collections.singletonMap("owner", "hadoop")), COLUMNS, TPaimonWriteMode.APPEND));
        for (Map<String, String> options : Arrays.asList(
                Collections.singletonMap("unknown-option", "true"),
                Collections.singletonMap("bucket", "4"),
                Collections.singletonMap("write-only", "false"),
                Collections.singletonMap("file.format", "orc"),
                Collections.singletonMap("variant.inferShreddingSchema", "true"),
                Collections.singletonMap("changelog-producer", "input"))) {
            Assert.assertNotNull(unsupportedReason(
                    table(options), COLUMNS, TPaimonWriteMode.APPEND));
        }
        for (TPaimonWriteMode mode : Arrays.asList(TPaimonWriteMode.OVERWRITE, TPaimonWriteMode.CHANGELOG)) {
            Assert.assertNotNull(unsupportedReason(table(Collections.emptyMap()), COLUMNS, mode));
        }
    }

    @Test
    public void testDorisObjectStorageRouting() throws Exception {
        // Each catalog has its own provider; do not merge credentials across providers.
        String[][] cases = {
                {"s3", "s3://bucket/table", "https://s3.us-east-1.amazonaws.com"},
                {"s3", "s3a://bucket/table", "https://s3.us-east-1.amazonaws.com"},
                {"oss", "oss://bucket/table", "https://oss-cn-beijing.aliyuncs.com"},
                {"cos", "cosn://bucket/table", "https://cos.ap-guangzhou.myqcloud.com"},
                {"obs", "obs://bucket/table", "https://obs.cn-north-4.myhuaweicloud.com"},
                {"gs", "gs://bucket/table", "https://storage.googleapis.com"},
                {"minio", "s3://bucket/table", "https://minio.example.com"},
                {"azure", "abfss://container@account.dfs.core.windows.net/table",
                        "https://account.blob.core.windows.net"}
        };
        for (String[] testCase : cases) {
            Map<String, String> properties = new HashMap<>();
            properties.put(testCase[0] + ".endpoint", testCase[2]);
            properties.put(testCase[0] + ".access_key", "test-account");
            properties.put(testCase[0] + ".secret_key", "test-secret");
            Map<StorageProperties.Type, StorageProperties> storage = StorageProperties.createAll(properties).stream()
                    .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
            FileStoreTable table = table(Collections.emptyMap(), Collections.emptyList(),
                    Collections.emptyList(), testCase[1]);
            Assert.assertNull(testCase[1], unsupportedReason(
                    table, COLUMNS, TPaimonWriteMode.APPEND, storage));
            TPaimonTableDescriptor desc = describe(table, storage);
            Assert.assertEquals(testCase[1], desc.getRootPath());
            Assert.assertEquals(TFileType.FILE_S3, desc.getStorage().getFileType());
            Assert.assertTrue(desc.getStorage().getRootPath().startsWith("s3://"));
            Assert.assertTrue(desc.getStorage().getRootPath().endsWith("/table"));
            Assert.assertEquals("test-secret", desc.getStorage().getProperties().get("AWS_SECRET_KEY"));
            Assert.assertFalse(TableSchema.fromJson(desc.getSchemaJson()).options().containsKey("AWS_SECRET_KEY"));
            if ("azure".equals(testCase[0])) {
                Assert.assertEquals("azure", desc.getStorage().getProperties().get("provider"));
            }
        }
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
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), "hdfs://ns/table"),
                COLUMNS, TPaimonWriteMode.APPEND, storage));
    }

    @Test
    public void testUnsupportedTableLayout() {
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap(), Collections.singletonList("id"), Collections.emptyList(), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap(), Collections.emptyList(), Collections.singletonList("id"), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap()), Arrays.asList("name", "id"), TPaimonWriteMode.APPEND));
    }
}
