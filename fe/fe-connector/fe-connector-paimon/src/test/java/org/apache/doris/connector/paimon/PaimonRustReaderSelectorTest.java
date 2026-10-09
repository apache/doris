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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonRustReaderSelectorTest {
    @TempDir
    java.nio.file.Path tempDir;

    @Test
    public void compatibleSplitUsesRustOnlyWhenEveryPlanGateIsEnabled() throws Exception {
        FileStoreTable table = table(fileLocation("compatible"), Collections.emptyMap(), DataTypes.INT());
        DataSplit split = split(file("data.parquet", table.schema().id(), null));

        Assertions.assertTrue(selector(enabledSession(), true, table, columns(), Collections.emptyMap(),
                false, false, false).canRead(split));
        Assertions.assertFalse(selector(session(false, true), true, table, columns(), Collections.emptyMap(),
                false, false, false).canRead(split));
        Assertions.assertFalse(selector(session(true, false), true, table, columns(), Collections.emptyMap(),
                false, false, false).canRead(split));
        Assertions.assertFalse(selector(enabledSession(), false, table, columns(), Collections.emptyMap(),
                false, false, false).canRead(split));
        Assertions.assertFalse(selector(enabledSession(), true, table, columns(), Collections.emptyMap(),
                true, false, false).canRead(split));
        Assertions.assertFalse(selector(enabledSession(), true, table, columns(), Collections.emptyMap(),
                false, true, false).canRead(split));
        Assertions.assertFalse(selector(enabledSession(), true, table, columns(), Collections.emptyMap(),
                false, false, true).canRead(split));
    }

    @Test
    public void locationSchemeAndCredentialsStayOnTheVerifiedAllowlist() {
        Assertions.assertTrue(PaimonRustReaderSelector.isRustVerifiedLocationScheme("file:///warehouse/t"));
        Assertions.assertTrue(PaimonRustReaderSelector.isRustVerifiedLocationScheme("s3://bucket/t"));
        Assertions.assertTrue(PaimonRustReaderSelector.isRustVerifiedLocationScheme("relative/path"));
        Assertions.assertFalse(PaimonRustReaderSelector.isRustVerifiedLocationScheme("gs://bucket/t"));
        Assertions.assertFalse(PaimonRustReaderSelector.isRustVerifiedLocationScheme(null));
    }

    @Test
    public void s3RequiresTranslatableCredentials() throws Exception {
        FileStoreTable table = table("s3://bucket/table", Collections.emptyMap(), DataTypes.INT());
        DataSplit split = split(file("data.parquet", table.schema().id(), null));

        Assertions.assertFalse(selector(enabledSession(), true, table, columns(), Collections.emptyMap(),
                false, false, false).canRead(split));

        Map<String, String> staticKeys = new HashMap<>();
        staticKeys.put("AWS_ACCESS_KEY", "access");
        staticKeys.put("AWS_SECRET_KEY", "secret");
        Assertions.assertTrue(selector(enabledSession(), true, table, columns(), staticKeys,
                false, false, false).canRead(split));

        Map<String, String> anonymous = Collections.singletonMap("AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS");
        Assertions.assertTrue(selector(enabledSession(), true, table, columns(), anonymous,
                false, false, false).canRead(split));

        Map<String, String> conflicting = new HashMap<>(staticKeys);
        conflicting.put("AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS");
        Assertions.assertFalse(selector(enabledSession(), true, table, columns(), conflicting,
                false, false, false).canRead(split));
    }

    @Test
    public void hdfsRejectsAuthenticationHaAndUnknownFilesystemOptions() {
        Assertions.assertTrue(PaimonRustReaderSelector.isRustVerifiedHdfsBackend(Collections.emptyMap()));
        Assertions.assertTrue(PaimonRustReaderSelector.isRustVerifiedHdfsBackend(
                Collections.singletonMap("fs.defaultFS", "hdfs://namenode")));
        Assertions.assertFalse(PaimonRustReaderSelector.isRustVerifiedHdfsBackend(
                Collections.singletonMap("hadoop.security.authentication", "kerberos")));
        Assertions.assertFalse(PaimonRustReaderSelector.isRustVerifiedHdfsBackend(
                Collections.singletonMap("dfs.nameservices", "cluster")));
        Assertions.assertFalse(PaimonRustReaderSelector.isRustVerifiedHdfsBackend(
                Collections.singletonMap("dfs.client.failover.proxy.provider.cluster", "provider")));
        Assertions.assertFalse(PaimonRustReaderSelector.isRustVerifiedHdfsBackend(
                Collections.singletonMap("hadoop.username", "alice")));
    }

    @Test
    public void orcTimestampLtzAndExternalFilesUseJni() throws Exception {
        FileStoreTable table = table(fileLocation("timestamp-ltz"), Collections.emptyMap(),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        PaimonRustReaderSelector selector = selector(enabledSession(), true, table, columns(),
                Collections.emptyMap(), false, false, false);

        Assertions.assertFalse(selector.canRead(split(file("data.orc", table.schema().id(), null))));
        Assertions.assertTrue(selector.canRead(split(file("data.parquet", table.schema().id(), null))));
        Assertions.assertFalse(selector.canRead(split(
                file("data.parquet", table.schema().id(), "s3://external/data.parquet"))));
    }

    @Test
    public void nestedProjectionUsesJni() throws Exception {
        FileStoreTable table = table(fileLocation("nested-projection"), Collections.emptyMap(),
                DataTypes.ROW(DataTypes.FIELD(1, "nested", DataTypes.INT())));
        ConnectorColumnHandle projected = new PaimonColumnHandle("v", 0)
                .withProjectedFieldIds(Collections.singleton(1));

        Assertions.assertFalse(selector(enabledSession(), true, table, Collections.singletonList(projected),
                Collections.emptyMap(), false, false, false)
                .canRead(split(file("data.parquet", table.schema().id(), null))));
    }

    @Test
    public void unsupportedMergeOptionsAreRejectedByEngine() {
        Map<String, String> options = new HashMap<>();
        options.put("fields.v.distinct", "true");
        Assertions.assertTrue(PaimonRustReaderSelector.hasRustUnsupportedMergeOption(
                options, CoreOptions.MergeEngine.PARTIAL_UPDATE));
        Assertions.assertTrue(PaimonRustReaderSelector.hasRustUnsupportedMergeOption(
                Collections.singletonMap("ignore-delete", "true"), CoreOptions.MergeEngine.AGGREGATE));
        Assertions.assertFalse(PaimonRustReaderSelector.hasRustUnsupportedMergeOption(
                Collections.singletonMap("partial-update.ignore-delete", "true"),
                CoreOptions.MergeEngine.PARTIAL_UPDATE));
    }

    @Test
    public void timestampLtzDetectionTraversesNestedTypes() {
        DataType nested = DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(),
                DataTypes.ROW(DataTypes.FIELD(1, "ts", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))));
        Assertions.assertTrue(PaimonRustReaderSelector.containsTimestampLtz(nested));
        Assertions.assertFalse(PaimonRustReaderSelector.containsTimestampLtz(
                DataTypes.ROW(DataTypes.FIELD(1, "ts", DataTypes.TIMESTAMP(3)))));
    }

    private String fileLocation(String name) throws Exception {
        java.nio.file.Path path = Files.createDirectories(tempDir.resolve(name));
        return path.toUri().toString();
    }

    private static FileStoreTable table(String location, Map<String, String> options, DataType valueType) {
        List<DataField> fields = Collections.singletonList(new DataField(0, "v", valueType));
        TableSchema schema = new TableSchema(0, fields, 0, Collections.emptyList(),
                Collections.emptyList(), options, null);
        return FileStoreTableFactory.create(LocalFileIO.create(), new Path(location), schema,
                CatalogEnvironment.empty());
    }

    private static List<ConnectorColumnHandle> columns() {
        return Collections.singletonList(new PaimonColumnHandle("v", 0));
    }

    private static DataFileMeta file(String name, long schemaId, String externalPath) {
        return DataFileMeta.forAppend(name, 1024, 1, SimpleStats.EMPTY_STATS,
                1, 1, schemaId, Collections.emptyList(), null, FileSource.APPEND,
                Collections.emptyList(), externalPath, null, Collections.emptyList());
    }

    private static DataSplit split(DataFileMeta... files) {
        return DataSplit.builder()
                .withSnapshot(1)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withDataFiles(Arrays.asList(files))
                .rawConvertible(true)
                .build();
    }

    private static PaimonRustReaderSelector selector(ConnectorSession session,
            boolean backendsSupportRust, FileStoreTable table, List<ConnectorColumnHandle> columns,
            Map<String, String> storageProperties, boolean usesFallbackRead,
            boolean incrementalRead, boolean projectedVariant) {
        return new PaimonRustReaderSelector(session, backendsSupportRust, table, columns,
                storageProperties, usesFallbackRead, incrementalRead, projectedVariant);
    }

    private static ConnectorSession enabledSession() {
        return session(true, true);
    }

    private static ConnectorSession session(boolean rustReader, boolean fileScannerV2) {
        Map<String, String> properties = new HashMap<>();
        properties.put("enable_paimon_rust_reader", Boolean.toString(rustReader));
        properties.put("enable_file_scanner_v2", Boolean.toString(fileScannerV2));
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "query";
            }

            @Override
            public String getUser() {
                return "user";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 1;
            }

            @Override
            public String getCatalogName() {
                return "catalog";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return properties;
            }
        };
    }
}
