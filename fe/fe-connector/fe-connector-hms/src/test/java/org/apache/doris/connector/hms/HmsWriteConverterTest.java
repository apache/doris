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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link HmsWriteConverter} — the CREATE TABLE / CREATE DATABASE direction, the SPI-clean
 * equivalent of fe-core {@code HiveUtil.toHiveTable}/{@code toHiveDatabase} plus
 * {@code HiveProperties.setTableProperties}.
 *
 * <p>WHY: this converter decides exactly what metastore object Doris writes when a user creates a
 * Hive table. Bugs here silently produce an unreadable table (wrong serde/format), lose the
 * data/partition-column split, or misplace serde properties. These pin the storage descriptor,
 * the per-format compression defaults, the property split, and the column split — the behavior
 * the connector's DDL path depends on (Rule 9: encode the contract).</p>
 */
public class HmsWriteConverterTest {

    private static ConnectorColumn col(String name, String type) {
        return new ConnectorColumn(name, ConnectorType.of(type), null, true, null);
    }

    private static HmsCreateTableRequest.Builder baseTable(String fileFormat, List<ConnectorColumn> columns,
            List<String> partitionKeys) {
        return HmsCreateTableRequest.builder()
                .dbName("db")
                .tableName("t")
                .columns(columns)
                .partitionKeys(partitionKeys)
                .fileFormat(fileFormat)
                .properties(new HashMap<>());
    }

    @Test
    public void testOrcTableFormatAndColumnSplit() {
        List<ConnectorColumn> columns = Arrays.asList(
                col("id", "INT"),
                col("name", "STRING"),
                col("dt", "DATEV2"));
        Table table = HmsWriteConverter.toHiveTable(
                baseTable("orc", columns, Collections.singletonList("dt"))
                        .location("hdfs://ns/db/t")
                        .dorisVersion("2.1.0-abc123")
                        .comment("hello")
                        .properties(mutableMap("owner", "alice"))
                        .build());

        Assertions.assertEquals("db", table.getDbName());
        Assertions.assertEquals("t", table.getTableName());
        Assertions.assertEquals("MANAGED_TABLE", table.getTableType());
        Assertions.assertEquals("alice", table.getOwner());

        // Data columns exclude the partition key; partition key is carried separately.
        StorageDescriptor sd = table.getSd();
        Assertions.assertEquals(Arrays.asList("id", "name"), names(sd.getCols()));
        Assertions.assertEquals(Arrays.asList("int", "string"), types(sd.getCols()));
        Assertions.assertEquals(Collections.singletonList("dt"), names(table.getPartitionKeys()));
        Assertions.assertEquals(Collections.singletonList("date"), types(table.getPartitionKeys()));

        // ORC storage formats + serde.
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", sd.getInputFormat());
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat", sd.getOutputFormat());
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                sd.getSerdeInfo().getSerializationLib());
        Assertions.assertEquals("hdfs://ns/db/t", sd.getLocation());
        Assertions.assertEquals("doris external hive table", sd.getParameters().get("tag"));

        // Table params: doris.version stamped, comment set, default ORC compression applied.
        Assertions.assertEquals("2.1.0-abc123", table.getParameters().get("doris.version"));
        Assertions.assertEquals("hello", table.getParameters().get("comment"));
        Assertions.assertEquals("zlib", table.getParameters().get("orc.compress"));
    }

    @Test
    public void testParquetDefaultCompression() {
        Table table = HmsWriteConverter.toHiveTable(
                baseTable("parquet", Collections.singletonList(col("id", "INT")),
                        Collections.emptyList()).build());
        StorageDescriptor sd = table.getSd();
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                sd.getInputFormat());
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                sd.getSerdeInfo().getSerializationLib());
        Assertions.assertEquals("snappy", table.getParameters().get("parquet.compression"));
    }

    @Test
    public void testTextCompressionUsesRequestDefaultThenPlainFallback() {
        // The connector-resolved session default flows through for a text table.
        Table withDefault = HmsWriteConverter.toHiveTable(
                baseTable("text", Collections.singletonList(col("id", "INT")),
                        Collections.emptyList()).defaultTextCompression("gzip").build());
        Assertions.assertEquals("org.apache.hadoop.mapred.TextInputFormat",
                withDefault.getSd().getInputFormat());
        Assertions.assertEquals("gzip", withDefault.getParameters().get("text.compression"));

        // No request default -> "plain" fallback (matches the legacy session-var default).
        Table noDefault = HmsWriteConverter.toHiveTable(
                baseTable("text", Collections.singletonList(col("id", "INT")),
                        Collections.emptyList()).build());
        Assertions.assertEquals("plain", noDefault.getParameters().get("text.compression"));
    }

    @Test
    public void testExplicitCompressionHonoredAndKeyRemoved() {
        Table table = HmsWriteConverter.toHiveTable(
                baseTable("orc", Collections.singletonList(col("id", "INT")),
                        Collections.emptyList()).properties(mutableMap("compression", "snappy")).build());
        Assertions.assertEquals("snappy", table.getParameters().get("orc.compress"));
        // The transient "compression" property must not leak onto the table.
        Assertions.assertFalse(table.getParameters().containsKey("compression"));
    }

    @Test
    public void testUnsupportedCompressionAndFormatThrow() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                HmsWriteConverter.toHiveTable(
                        baseTable("orc", Collections.singletonList(col("id", "INT")),
                                Collections.emptyList())
                                .properties(mutableMap("compression", "bogus")).build()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                HmsWriteConverter.toHiveTable(
                        baseTable("avro", Collections.singletonList(col("id", "INT")),
                                Collections.emptyList()).build()));
    }

    @Test
    public void testSerdePropertiesSplitFromTableProperties() {
        Map<String, String> props = mutableMap("field.delim", ",");
        props.put("my.custom", "v");
        Table table = HmsWriteConverter.toHiveTable(
                baseTable("text", Collections.singletonList(col("id", "INT")),
                        Collections.emptyList()).properties(props).build());
        // field.delim is a serde property -> goes to the SerDe params, not the table params.
        Assertions.assertEquals(",", table.getSd().getSerdeInfo().getParameters().get("field.delim"));
        Assertions.assertFalse(table.getParameters().containsKey("field.delim"));
        // A non-serde property stays on the table.
        Assertions.assertEquals("v", table.getParameters().get("my.custom"));
    }

    @Test
    public void testDorisVersionOmittedWhenAbsent() {
        Table table = HmsWriteConverter.toHiveTable(
                baseTable("orc", Collections.singletonList(col("id", "INT")),
                        Collections.emptyList()).build());
        Assertions.assertFalse(table.getParameters().containsKey("doris.version"));
    }

    @Test
    public void testBucketingCarriedToStorageDescriptor() {
        Table table = HmsWriteConverter.toHiveTable(
                baseTable("orc", Collections.singletonList(col("id", "INT")),
                        Collections.emptyList())
                        .bucketCols(Collections.singletonList("id")).numBuckets(8).build());
        Assertions.assertEquals(Collections.singletonList("id"), table.getSd().getBucketCols());
        Assertions.assertEquals(8, table.getSd().getNumBuckets());
    }

    @Test
    public void testToHiveDatabase() {
        Database db = HmsWriteConverter.toHiveDatabase(new HmsCreateDatabaseRequest(
                "mydb", "hdfs://ns/mydb", "a comment", mutableMap("owner", "bob")));
        Assertions.assertEquals("mydb", db.getName());
        Assertions.assertEquals("hdfs://ns/mydb", db.getLocationUri());
        Assertions.assertEquals("a comment", db.getDescription());
        Assertions.assertEquals("bob", db.getOwnerName());
        Assertions.assertEquals(PrincipalType.USER, db.getOwnerType());
    }

    @Test
    public void testToHiveDatabaseNoLocationNoOwner() {
        Database db = HmsWriteConverter.toHiveDatabase(new HmsCreateDatabaseRequest(
                "mydb", null, null, new HashMap<>()));
        Assertions.assertEquals("mydb", db.getName());
        Assertions.assertFalse(db.isSetLocationUri());
        Assertions.assertNull(db.getOwnerName());
        // Comment normalizes to empty string, never null.
        Assertions.assertEquals("", db.getDescription());
    }

    @Test
    public void testToHivePartitionsBuildsPartitionWithStatsAndSd() {
        // WHY: the add-partition commit path depends on the storage descriptor (location/serde/format)
        // and the basic-stats parameters being stamped exactly as HMS expects, or the created
        // partition is unreadable or its row/byte counts are wrong.
        HmsPartitionWithStatistics part = HmsPartitionWithStatistics.builder()
                .name("dt=2024-01-01")
                .partitionValues(Collections.singletonList("2024-01-01"))
                .location("hdfs://ns/db/t/dt=2024-01-01")
                .columns(Collections.singletonList(fieldSchema("id", "int")))
                .inputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
                .outputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
                .serde("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
                .parameters(new HashMap<>())
                .statistics(HmsPartitionStatistics.fromCommonStatistics(10, 2, 100))
                .build();

        List<Partition> partitions = HmsWriteConverter.toHivePartitions("db", "t",
                Collections.singletonList(part));

        Assertions.assertEquals(1, partitions.size());
        Partition p = partitions.get(0);
        Assertions.assertEquals("db", p.getDbName());
        Assertions.assertEquals("t", p.getTableName());
        Assertions.assertEquals(Collections.singletonList("2024-01-01"), p.getValues());

        StorageDescriptor sd = p.getSd();
        Assertions.assertEquals("hdfs://ns/db/t/dt=2024-01-01", sd.getLocation());
        Assertions.assertEquals(Collections.singletonList("id"), names(sd.getCols()));
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", sd.getInputFormat());
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat", sd.getOutputFormat());
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                sd.getSerdeInfo().getSerializationLib());

        // Basic statistics land on the partition parameters (numRows / numFiles / totalSize).
        Assertions.assertEquals("10", p.getParameters().get(StatsSetupConst.ROW_COUNT));
        Assertions.assertEquals("2", p.getParameters().get(StatsSetupConst.NUM_FILES));
        Assertions.assertEquals("100", p.getParameters().get(StatsSetupConst.TOTAL_SIZE));
    }

    @Test
    public void testToHivePartitionsEmptyLocationBecomesNull() {
        // Strings.emptyToNull parity: an empty location must not be written as "" (HMS treats "" and
        // null differently for partition location resolution).
        HmsPartitionWithStatistics part = HmsPartitionWithStatistics.builder()
                .partitionValues(Collections.singletonList("2024"))
                .location("")
                .columns(Collections.emptyList())
                .parameters(new HashMap<>())
                .statistics(HmsPartitionStatistics.EMPTY)
                .build();
        Partition p = HmsWriteConverter.toHivePartitions("db", "t",
                Collections.singletonList(part)).get(0);
        Assertions.assertNull(p.getSd().getLocation());
    }

    @Test
    public void testToStatisticsParametersStampsBasicStatsAndWorkaroundFlag() {
        Map<String, String> origin = mutableMap("existing", "kept");
        Map<String, String> result = HmsWriteConverter.toStatisticsParameters(
                origin, new HmsCommonStatistics(7, 3, 70));
        Assertions.assertEquals("7", result.get(StatsSetupConst.ROW_COUNT));
        Assertions.assertEquals("3", result.get(StatsSetupConst.NUM_FILES));
        Assertions.assertEquals("70", result.get(StatsSetupConst.TOTAL_SIZE));
        // Pre-existing params survive.
        Assertions.assertEquals("kept", result.get("existing"));
        // CDH workaround flag added when absent.
        Assertions.assertEquals("workaround for potential lack of HIVE-12730",
                result.get("STATS_GENERATED_VIA_STATS_TASK"));
        // The source map is not mutated (defensive copy).
        Assertions.assertFalse(origin.containsKey(StatsSetupConst.ROW_COUNT));
    }

    @Test
    public void testToStatisticsParametersKeepsExistingWorkaroundFlag() {
        Map<String, String> origin = mutableMap("STATS_GENERATED_VIA_STATS_TASK", "already-set");
        Map<String, String> result = HmsWriteConverter.toStatisticsParameters(
                origin, HmsCommonStatistics.EMPTY);
        Assertions.assertEquals("already-set", result.get("STATS_GENERATED_VIA_STATS_TASK"));
    }

    @Test
    public void testToPartitionStatisticsReadsBackParamsAndDefaults() {
        Map<String, String> params = new HashMap<>();
        params.put(StatsSetupConst.ROW_COUNT, "42");
        params.put(StatsSetupConst.NUM_FILES, "4");
        params.put(StatsSetupConst.TOTAL_SIZE, "420");
        HmsCommonStatistics stats =
                HmsWriteConverter.toPartitionStatistics(params).getCommonStatistics();
        Assertions.assertEquals(42, stats.getRowCount());
        Assertions.assertEquals(4, stats.getFileCount());
        Assertions.assertEquals(420, stats.getTotalFileBytes());

        // Missing params default to -1 (legacy sentinel for "unknown").
        HmsCommonStatistics missing =
                HmsWriteConverter.toPartitionStatistics(new HashMap<>()).getCommonStatistics();
        Assertions.assertEquals(-1, missing.getRowCount());
        Assertions.assertEquals(-1, missing.getFileCount());
        Assertions.assertEquals(-1, missing.getTotalFileBytes());
    }

    private static FieldSchema fieldSchema(String name, String type) {
        FieldSchema fs = new FieldSchema();
        fs.setName(name);
        fs.setType(type);
        return fs;
    }

    private static List<String> names(List<FieldSchema> schemas) {
        return schemas.stream().map(FieldSchema::getName).collect(java.util.stream.Collectors.toList());
    }

    private static List<String> types(List<FieldSchema> schemas) {
        return schemas.stream().map(FieldSchema::getType).collect(java.util.stream.Collectors.toList());
    }

    private static Map<String, String> mutableMap(String k, String v) {
        Map<String, String> m = new HashMap<>();
        m.put(k, v);
        return m;
    }
}
