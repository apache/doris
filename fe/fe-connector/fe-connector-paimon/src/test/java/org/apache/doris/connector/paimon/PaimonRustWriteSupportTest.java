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

package org.apache.doris.connector.paimon;

import org.apache.doris.thrift.TPaimonTableDesc;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        return new AppendOnlyFileStoreTable(LocalFileIO.create(), new Path(location), schema,
                CatalogEnvironment.empty());
    }

    @Test
    public void testDefaultEnabledAndLocalAppend() {
        AppendOnlyFileStoreTable table = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "value", DataTypes.STRING()));
        Assertions.assertTrue(PaimonRustWriteSupport.isSupported(table,
                Arrays.asList("id", "value"), Collections.emptyMap()));
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table,
                Arrays.asList("value", "id"), Collections.emptyMap()));
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table,
                Collections.singletonList("id"), Collections.emptyMap()));
    }

    @Test
    public void testDefaultTableOptionsUseSdkDefaults() {
        AppendOnlyFileStoreTable table = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        table.options().remove("bucket");
        table.options().remove("file.format");
        Assertions.assertTrue(PaimonRustWriteSupport.isSupported(table,
                Collections.singletonList("id"), Collections.emptyMap()));
    }

    @Test
    public void testPartitionedAndFixedBucketWritesUseRust() {
        AppendOnlyFileStoreTable base = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "region", DataTypes.STRING()));
        Map<String, String> options = new HashMap<>(base.options());
        options.put("bucket", "4");
        options.put("bucket-key", "id");
        FileStoreTable partitioned = FileStoreTableFactory.create(base.fileIO(), base.location(),
                new TableSchema(7, base.schema().fields(), 1, Collections.singletonList("region"),
                        Collections.emptyList(), options, null));
        Assertions.assertTrue(PaimonRustWriteSupport.isSupported(partitioned,
                Arrays.asList("id", "region"), Collections.emptyMap()));
        options.put("bucket", "-1");
        options.remove("bucket-key");
        FileStoreTable unaware = FileStoreTableFactory.create(base.fileIO(), base.location(),
                new TableSchema(7, base.schema().fields(), 1, Collections.singletonList("region"),
                        Collections.emptyList(), options, null));
        Assertions.assertTrue(PaimonRustWriteSupport.isSupported(unaware,
                Arrays.asList("id", "region"), Collections.emptyMap()));
    }

    @Test
    public void testPrimaryKeyWritesUseRustForFixedAndDynamicBuckets() {
        List<DataField> fields = Arrays.asList(DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "region", DataTypes.STRING().notNull()));
        // Dynamic tables use the existing GATHER distribution, including cross-partition upserts.
        for (String bucket : Arrays.asList("1", "4", "-1")) {
            for (String merge : Arrays.asList("deduplicate", "partial-update", "aggregation", "first-row")) {
                Map<String, String> options = new HashMap<>();
                options.put("bucket", bucket);
                options.put("file.format", "parquet");
                options.put("merge-engine", merge);
                FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(),
                        new Path("file:/tmp/rust-primary-key-test"), new TableSchema(0, fields, 1,
                                Collections.singletonList("region"), bucket.equals("-1")
                                        ? Collections.singletonList("id") : Arrays.asList("id", "region"), options, null));
                if (merge.equals("first-row")) {
                    Assertions.assertEquals("first_row_requires_compaction", PaimonRustWriteSupport.unsupportedReason(
                            table, Arrays.asList("id", "region"), Collections.emptyMap()).get());
                } else {
                    Assertions.assertTrue(PaimonRustWriteSupport.isSupported(table,
                            Arrays.asList("id", "region"), Collections.emptyMap()), bucket + ":" + merge);
                }
                if (bucket.equals("-1")) {
                    table.options().put("cross-partition-upsert.index-ttl", "1 h");
                    Assertions.assertEquals("cross_partition_index_ttl", PaimonRustWriteSupport.unsupportedReason(
                            table, Arrays.asList("id", "region"), Collections.emptyMap()).get());
                }
            }
        }
    }

    @Test
    public void testUnsupportedFeaturesAndTypesUseJni() {
        AppendOnlyFileStoreTable table = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        List<String> columns = Collections.singletonList("id");
        table.options().put("file-index.bloom-filter.columns", "id");
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        table.options().remove("file-index.bloom-filter.columns");
        table.options().put("fields.id.ignore-delete", "true");
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        table.options().remove("fields.id.ignore-delete");
        table.options().put("bucket", "-2");
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        table.options().put("bucket", "-1");
        table.options().put("branch", "dev");
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        table.options().remove("branch");
        table.options().put("file.format", "orc");
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        AppendOnlyFileStoreTable nested = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.ARRAY(DataTypes.INT())));
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(nested, columns, Collections.emptyMap()));
        AppendOnlyFileStoreTable nanos = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.TIMESTAMP(9)));
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(nanos, columns, Collections.emptyMap()));
    }

    @Test
    public void testSchemaDefaultsUseJni() {
        AppendOnlyFileStoreTable table = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "value", DataTypes.INT()).newDefaultValue("42"));
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table,
                Arrays.asList("id", "value"), Collections.emptyMap()));

        AppendOnlyFileStoreTable emptyDefault = table("file:/tmp/rust-writer-test",
                DataTypes.FIELD(0, "value", DataTypes.STRING()).newDefaultValue("''"));
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(emptyDefault,
                Collections.singletonList("value"), Collections.emptyMap()));
    }

    @Test
    public void testS3RequiresExplicitCredentials() {
        AppendOnlyFileStoreTable table = table("s3://bucket/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        List<String> columns = Collections.singletonList("id");
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, Collections.emptyMap()));
        Map<String, String> storage = new HashMap<>();
        storage.put("AWS_ACCESS_KEY", "test-access");
        storage.put("AWS_SECRET_KEY", "test-secret");
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, storage));
        storage.put("AWS_REGION", "us-east-1");
        Assertions.assertTrue(PaimonRustWriteSupport.isSupported(table, columns, storage));
        storage.put("AWS_ROLE_ARN", "role");
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, storage));
        storage.remove("AWS_ROLE_ARN");
        storage.put("AWS_ACCESS_KEY", "");
        Assertions.assertFalse(PaimonRustWriteSupport.isSupported(table, columns, storage));
    }

    @Test
    public void testRestDataTokensSelectJniEvenWithCapturedS3Credentials() {
        AppendOnlyFileStoreTable local = table("s3://bucket/rest-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        Options options = Options.fromMap(Collections.singletonMap(RESTTokenFileIO.DATA_TOKEN_ENABLED.key(), "true"));
        CatalogContext context = CatalogContext.create(options);
        CatalogEnvironment environment = new CatalogEnvironment(new Identifier("db", "t"), null,
                null, null, null, context, false, false);
        Map<String, String> storage = new HashMap<>();
        storage.put("s3.access-key", "current-access");
        storage.put("s3.secret-key", "current-secret");
        storage.put("s3.region", "us-east-1");
        // The effective catalog option also detects a cached/wrapped REST FileIO.
        for (org.apache.paimon.fs.FileIO fileIO : Arrays.asList(local.fileIO(),
                new RESTTokenFileIO(context, null, environment.identifier(), local.location()))) {
            AppendOnlyFileStoreTable rest = new AppendOnlyFileStoreTable(fileIO, local.location(),
                    local.schema(), environment);
            Assertions.assertEquals("rest_vended_credentials", PaimonRustWriteSupport.unsupportedReason(rest,
                    Collections.singletonList("id"), storage).get());
        }
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
        table = new AppendOnlyFileStoreTable(LocalFileIO.create(), table.location(), table.schema(), environment);
        TPaimonTableDesc desc = PaimonTableDescriptor.describe(table, table.schema(), "db", "t",
                Collections.emptyMap(), false);
        Assertions.assertEquals("test-endpoint", desc.getCatalogContext().getHadoopConfig().get("fs.s3a.endpoint"));
        Assertions.assertEquals("12345", desc.getCatalogContext().getHadoopConfig().get("io.file.buffer.size"));
        Assertions.assertFalse(desc.getCatalogContext().getHadoopConfig()
                .containsKey("ipc.client.connect.max.retries"));
        Assertions.assertEquals("uuid-7", desc.getCatalogContext().getTableUuid());
        Assertions.assertEquals("RemoteDB", desc.getDatabaseName());
        Assertions.assertEquals("RemoteTable", desc.getTableName());
    }

    @Test
    public void testJniDescriptorPreservesOptionsAndReportsFallbackReason() {
        AppendOnlyFileStoreTable table = table("file:/tmp/jni-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()).newDefaultValue("42"));
        TPaimonTableDesc desc = PaimonTableDescriptor.describe(table, table.schema(), "db", "t",
                Collections.emptyMap(), false);
        TableSchema schema = JsonSerdeUtil.fromJson(desc.getSchemaJson(), TableSchema.class);
        Assertions.assertEquals(table.schema().options(), schema.options());
        Assertions.assertEquals(table.schema().fields(), schema.fields());
        Assertions.assertTrue(desc.isSetCatalogContext());
        Assertions.assertEquals("schema_defaults", PaimonRustWriteSupport.unsupportedReason(table,
                Collections.singletonList("id"), Collections.emptyMap()).get());
    }

    @Test
    public void testDescriptorPreservesPinnedSchemaWithoutJavaSerialization() {
        AppendOnlyFileStoreTable table = table("s3://bucket/rust-writer-test",
                DataTypes.FIELD(0, "id", DataTypes.INT()));
        Map<String, String> storage = new HashMap<>();
        storage.put("AWS_ACCESS_KEY", "test-access");
        storage.put("AWS_SECRET_KEY", "test-secret");
        Map<String, String> writerOptions = new HashMap<>(table.schema().options());
        writerOptions.put("write.parquet-buffer-size", "8 mb");
        TPaimonTableDesc desc = PaimonTableDescriptor.describe(
                table, table.schema().copy(writerOptions), "db", "t", storage, false);
        TableSchema restored = JsonSerdeUtil.fromJson(desc.getSchemaJson(), TableSchema.class);
        Assertions.assertEquals(7, restored.id());
        Assertions.assertEquals(table.schema().fields(), restored.fields());
        Assertions.assertEquals("8 mb", restored.options().get("write.parquet-buffer-size"));
        Assertions.assertFalse(table.options().containsKey("write.parquet-buffer-size"));
        Assertions.assertEquals("test-access", desc.getStorageOptions().get("s3.access-key"));
        Assertions.assertEquals("main", desc.getBranch());
        Assertions.assertEquals("db", desc.getDatabaseName());
        Assertions.assertEquals("t", desc.getTableName());
    }
}
