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

import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPaimonTableDesc;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonRustWriteSupportTest {
    private AppendOnlyFileStoreTable table(String location, DataField... fields) {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "-1");
        options.put("file.format", "parquet");
        TableSchema schema = new TableSchema(7, Arrays.asList(fields), fields.length - 1,
                Collections.emptyList(), Collections.emptyList(), options, null);
        AppendOnlyFileStoreTable table = Mockito.mock(AppendOnlyFileStoreTable.class);
        Mockito.when(table.options()).thenReturn(options);
        Mockito.when(table.catalogEnvironment()).thenReturn(CatalogEnvironment.empty());
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(table.primaryKeys()).thenReturn(Collections.emptyList());
        Mockito.when(table.location()).thenReturn(new Path(location));
        return table;
    }

    @Test
    public void testWriterSwitchIsForwardedToMaster() {
        SessionVariable session = new SessionVariable();
        session.setEnablePaimonRustWriter(true);
        SessionVariable forwarded = new SessionVariable();
        forwarded.setForwardedSessionVariables(session.getForwardVariables());
        Assert.assertTrue(forwarded.isEnablePaimonRustWriter());
    }

    @Test
    public void testDefaultDisabledAndLocalAppend() {
        Assert.assertFalse(new SessionVariable().isEnablePaimonRustWriter());
        AppendOnlyFileStoreTable table = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "value", DataTypes.STRING()));
        Assert.assertTrue(PaimonRustWriteSupport.isSupported(table,
                Arrays.asList("id", "value"), Collections.emptyMap()));
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table,
                Arrays.asList("value", "id"), Collections.emptyMap()));
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table,
                Collections.singletonList("id"), Collections.emptyMap()));
    }

    @Test
    public void testDefaultTableOptionsUseSdkDefaults() {
        AppendOnlyFileStoreTable table = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        table.options().remove("bucket");
        table.options().remove("file.format");
        Assert.assertTrue(PaimonRustWriteSupport.isSupported(table,
                Collections.singletonList("id"), Collections.emptyMap()));
    }

    @Test
    public void testUnsupportedTableAndTypeUseJni() {
        AppendOnlyFileStoreTable table = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        List<String> columns = Collections.singletonList("id");
        table.options().put("bucket", "1");
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        table.options().put("bucket", "-1");
        table.options().put("file-index.bloom-filter.columns", "id");
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        table.options().remove("file-index.bloom-filter.columns");
        Mockito.when(table.partitionKeys()).thenReturn(columns);
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        AppendOnlyFileStoreTable nested = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.ARRAY(DataTypes.INT())));
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(nested, columns, Collections.emptyMap()));
        AppendOnlyFileStoreTable nanos = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.TIMESTAMP(9)));
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(nanos, columns, Collections.emptyMap()));
    }

    @Test
    public void testSchemaDefaultsUseJni() {
        AppendOnlyFileStoreTable table = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "value", DataTypes.INT()).newDefaultValue("42"));
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table,
                Arrays.asList("id", "value"), Collections.emptyMap()));

        AppendOnlyFileStoreTable emptyDefault = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "value", DataTypes.STRING()).newDefaultValue("''"));
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(emptyDefault,
                Collections.singletonList("value"), Collections.emptyMap()));
    }

    @Test
    public void testS3RequiresExplicitCredentials() {
        AppendOnlyFileStoreTable table = table("s3://bucket/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        List<String> columns = Collections.singletonList("id");
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        Map<String, String> storage = new HashMap<>();
        storage.put("AWS_ACCESS_KEY", "test-access");
        storage.put("AWS_SECRET_KEY", "test-secret");
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, storage));
        storage.put("AWS_REGION", "us-east-1");
        Assert.assertTrue(PaimonRustWriteSupport.isSupported(table, columns, storage));
        storage.put("AWS_ROLE_ARN", "role");
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, storage));
        storage.remove("AWS_ROLE_ARN");
        storage.put("AWS_ACCESS_KEY", "");
        Assert.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, storage));
    }

    @Test
    public void testDescriptorCarriesHadoopOverridesWithoutBuiltInDefaults() {
        AppendOnlyFileStoreTable table = table("file:/tmp/jni-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        Configuration configuration = new Configuration();
        configuration.set("fs.s3a.endpoint", "test-endpoint");
        configuration.set("io.file.buffer.size", "12345");
        CatalogEnvironment environment = new CatalogEnvironment(new Identifier("RemoteDB", "RemoteTable"), "uuid-7",
                null, null, null, CatalogContext.create(new Options(), configuration), false, false);
        Mockito.when(table.catalogEnvironment()).thenReturn(environment);
        TPaimonTableDesc desc = PaimonTableDescriptor.describe(table, "db", "t",
                Collections.emptyMap(), false, false);
        Assert.assertEquals("test-endpoint", desc.getCatalogContext().getHadoopConfig().get("fs.s3a.endpoint"));
        Assert.assertEquals("12345", desc.getCatalogContext().getHadoopConfig().get("io.file.buffer.size"));
        Assert.assertFalse(desc.getCatalogContext().getHadoopConfig().containsKey("ipc.client.connect.max.retries"));
        Assert.assertEquals("uuid-7", desc.getCatalogContext().getTableUuid());
        Assert.assertEquals("RemoteDB", desc.getDatabaseName());
        Assert.assertEquals("RemoteTable", desc.getTableName());
    }

    @Test
    public void testJniDescriptorPreservesOptionsAndReportsFallbackReason() {
        AppendOnlyFileStoreTable table = table("file:/tmp/jni-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()).newDefaultValue("42"));
        TPaimonTableDesc desc = PaimonTableDescriptor.describe(table, "db", "t",
                Collections.emptyMap(), false, false);
        TableSchema schema = JsonSerdeUtil.fromJson(desc.getSchemaJson(), TableSchema.class);
        Assert.assertEquals(table.schema().options(), schema.options());
        Assert.assertEquals(table.schema().fields(), schema.fields());
        Assert.assertTrue(desc.isSetCatalogContext());
        Assert.assertEquals("schema_defaults", PaimonRustWriteSupport.unsupportedReason(table,
                Collections.singletonList("id"), Collections.emptyMap()).get());
    }

    @Test
    public void testDescriptorPreservesPinnedSchemaWithoutJavaSerialization() {
        AppendOnlyFileStoreTable table = table("s3://bucket/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        Map<String, String> storage = new HashMap<>();
        storage.put("AWS_ACCESS_KEY", "test-access");
        storage.put("AWS_SECRET_KEY", "test-secret");
        TPaimonTableDesc desc = PaimonTableDescriptor.describe(table, "db", "t", storage, true, false);
        TableSchema restored = JsonSerdeUtil.fromJson(desc.getSchemaJson(), TableSchema.class);
        Assert.assertEquals(7, restored.id());
        Assert.assertEquals(table.schema().fields(), restored.fields());
        Assert.assertEquals("8 mb", restored.options().get("write.parquet-buffer-size"));
        Assert.assertFalse(table.options().containsKey("write.parquet-buffer-size"));
        Assert.assertEquals("test-access", desc.getStorageOptions().get("s3.access-key"));
        Assert.assertEquals(11, desc.getCommitMessageVersion());
        Assert.assertEquals("main", desc.getBranch());
        Assert.assertEquals("db", desc.getDatabaseName());
        Assert.assertEquals("t", desc.getTableName());
    }
}
