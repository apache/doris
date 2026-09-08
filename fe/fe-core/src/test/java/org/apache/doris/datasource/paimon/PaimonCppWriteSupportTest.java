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
import org.apache.doris.thrift.TPaimonCppWriteDescriptor;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteBackendType;
import org.apache.doris.thrift.TPaimonWriteMode;

import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
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

    private TPaimonCppWriteDescriptor describe(FileStoreTable table) {
        return describe(table, Collections.emptyMap());
    }

    private TPaimonCppWriteDescriptor describe(FileStoreTable table,
            Map<StorageProperties.Type, StorageProperties> storage) {
        return PaimonCppWriteSupport.decide(table, COLUMNS, TPaimonWriteMode.APPEND, storage).getDescriptor();
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
        return table;
    }

    private FileStoreTable table(Map<String, String> overrides) {
        return table(overrides, Collections.emptyList(), Collections.emptyList(), "file:///tmp/paimon");
    }

    @Test
    public void testSupportedAppendDescriptor() {
        FileStoreTable table = table(Collections.emptyMap());
        Assert.assertNull(unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND));
        TPaimonCppWriteDescriptor desc = describe(table);
        Assert.assertEquals(TFileType.FILE_LOCAL, desc.getStorage().getFileType());
        Assert.assertEquals(7, desc.getSchemaId());
        Assert.assertEquals("/tmp/paimon", desc.getRootPath());
        Assert.assertEquals(COLUMNS.size(), desc.getColumnsSize());
        Assert.assertEquals("INTEGER", desc.getColumns().get(0).getType());
        Assert.assertEquals("VARCHAR", desc.getColumns().get(1).getType());
        Assert.assertFalse(desc.getColumns().get(0).isNullable());
        Assert.assertTrue(desc.getColumns().get(1).isNullable());
        Assert.assertFalse(table.options().containsKey("manifest.format"));
        Assert.assertEquals("avro", desc.getOptions().get("manifest.format"));
    }

    @Test
    public void testNativeSinkThriftRoundTrip() throws Exception {
        TPaimonTableSink sink = new TPaimonTableSink();
        sink.setBackendType(TPaimonWriteBackendType.CPP);
        sink.setWriteMode(TPaimonWriteMode.APPEND);
        sink.setCommitUser("test-writer");
        sink.setColumnNames(COLUMNS);
        sink.setCppDescriptor(describe(table(Collections.emptyMap())));

        byte[] bytes = new TSerializer(new TCompactProtocol.Factory()).serialize(sink);
        TPaimonTableSink restored = new TPaimonTableSink();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, bytes);
        Assert.assertEquals(sink, restored);
        Assert.assertFalse(restored.isSetSerializedTable());
        Assert.assertFalse(restored.isSetHadoopConfig());
        Assert.assertEquals(COLUMNS, restored.getColumnNames());
        Assert.assertEquals(COLUMNS.size(), restored.getCppDescriptor().getColumnsSize());
    }

    @Test
    public void testOwnerDoesNotChangeWriteCapability() {
        FileStoreTable table = table(Collections.singletonMap("owner", "hadoop"));
        Assert.assertNull(unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertEquals("hadoop", describe(table).getOptions().get("owner"));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.singletonMap("unknown-option", "true")), COLUMNS, TPaimonWriteMode.APPEND));
    }

    @Test
    public void testLocalPathNormalization() {
        for (String location : Arrays.asList("/tmp/p", "file:/tmp/p", "file:///tmp/p", "FILE:/tmp/p")) {
            FileStoreTable table = table(Collections.emptyMap(), Collections.emptyList(),
                    Collections.emptyList(), location);
            Assert.assertNull(unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND));
            Path original = table.location();
            Assert.assertEquals("/tmp/p", describe(table).getRootPath());
            Assert.assertSame(original, table.location());
        }
        FileStoreTable table = table(Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyList(), "file:/tmp/a b+%20?#");
        Assert.assertEquals("/tmp/a b+%20?#", describe(table).getRootPath());
    }

    @Test
    public void testUnsupportedLocationComponents() {
        for (String location : Arrays.asList("file://host/tmp/p", "relative/p",
                "file:/tmp/p?query", "file:/tmp/p#fragment", "file:/tmp/%00p")) {
            FileStoreTable table = table(Collections.emptyMap());
            Mockito.when(table.location()).thenReturn(new Path(URI.create(location)));
            Assert.assertNotNull(location,
                    unsupportedReason(table, COLUMNS, TPaimonWriteMode.APPEND));
            Assert.assertThrows(IllegalStateException.class, () -> describe(table));
        }
    }

    @Test
    public void testUnsupportedOptionsAndModes() {
        for (Map<String, String> options : Arrays.asList(
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
            TPaimonCppWriteDescriptor desc = describe(table, storage);
            Assert.assertEquals(testCase[1], desc.getRootPath());
            Assert.assertEquals(TFileType.FILE_S3, desc.getStorage().getFileType());
            Assert.assertTrue(desc.getStorage().getRootPath().startsWith("s3://"));
            Assert.assertTrue(desc.getStorage().getRootPath().endsWith("/table"));
            Assert.assertEquals("test-secret", desc.getStorage().getProperties().get("AWS_SECRET_KEY"));
            Assert.assertFalse(desc.getOptions().containsKey("AWS_SECRET_KEY"));
            if ("azure".equals(testCase[0])) {
                Assert.assertEquals("azure", desc.getStorage().getProperties().get("provider"));
            }
        }
    }

    @Test
    public void testDecisionResolvesStorageOnlyOnce() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        properties.put("s3.access_key", "test-account");
        properties.put("s3.secret_key", "test-secret");
        StorageProperties provider = Mockito.spy(StorageProperties.createAll(properties).stream()
                .filter(p -> p.getType() == StorageProperties.Type.S3).findFirst().get());
        FileStoreTable table = table(Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyList(), "s3://bucket/table");
        PaimonCppWriteSupport.Decision decision = PaimonCppWriteSupport.decide(table, COLUMNS,
                TPaimonWriteMode.APPEND, Collections.singletonMap(StorageProperties.Type.S3, provider));
        Assert.assertTrue(decision.isSupported());
        Assert.assertSame(decision.getDescriptor(), decision.getDescriptor());
        Assert.assertNull(decision.getFallbackReason());
        Mockito.verify(provider, Mockito.times(1)).validateAndNormalizeUri("s3://bucket/table");
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
    public void testUnsupportedRoutingAndStorage() {
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap(), Collections.singletonList("id"), Collections.emptyList(), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap(), Collections.emptyList(), Collections.singletonList("id"), "/tmp/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), "s3://bucket/p"),
                COLUMNS, TPaimonWriteMode.APPEND));
        Assert.assertNotNull(unsupportedReason(
                table(Collections.emptyMap()), Arrays.asList("name", "id"), TPaimonWriteMode.APPEND));
    }
}
