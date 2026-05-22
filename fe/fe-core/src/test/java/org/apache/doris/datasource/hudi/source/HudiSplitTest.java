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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.common.util.LocationPath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HudiSplitTest {

    private HudiSplit split;

    @BeforeEach
    public void setUp() {
        LocationPath path = LocationPath.of("hdfs://namenode:9000/hudi/test_table/data.parquet");
        split = new HudiSplit(path, 0, 1024, 2048, new String[]{"host1", "host2"},
                Arrays.asList("part1_val", "part2_val"));
    }

    @Test
    public void testConstructor() {
        Assertions.assertNotNull(split);
        Assertions.assertEquals(0, split.getStart());
        Assertions.assertEquals(1024, split.getLength());
        Assertions.assertEquals(2048, split.getFileLength());
        Assertions.assertArrayEquals(new String[]{"host1", "host2"}, split.getHosts());
        Assertions.assertEquals(Arrays.asList("part1_val", "part2_val"), split.getPartitionValues());
    }

    @Test
    public void testConstructorWithNullHosts() {
        LocationPath path = LocationPath.of("hdfs://namenode:9000/hudi/test_table/data.parquet");
        HudiSplit s = new HudiSplit(path, 0, 512, 1024, null, Collections.emptyList());
        Assertions.assertNotNull(s);
        Assertions.assertNotNull(s.getHosts());
        Assertions.assertEquals(0, s.getHosts().length);
    }

    @Test
    public void testConstructorWithEmptyPartitionValues() {
        LocationPath path = LocationPath.of("hdfs://namenode:9000/hudi/test_table/data.parquet");
        HudiSplit s = new HudiSplit(path, 100, 200, 500, new String[]{}, Collections.emptyList());
        Assertions.assertEquals(100, s.getStart());
        Assertions.assertEquals(200, s.getLength());
        Assertions.assertTrue(s.getPartitionValues().isEmpty());
    }

    // ===================== Old fields =====================

    @Test
    public void testInstantTime() {
        Assertions.assertNull(split.getInstantTime());
        split.setInstantTime("20230401010101");
        Assertions.assertEquals("20230401010101", split.getInstantTime());
    }

    @Test
    public void testSerde() {
        Assertions.assertNull(split.getSerde());
        split.setSerde("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", split.getSerde());
    }

    @Test
    public void testInputFormat() {
        Assertions.assertNull(split.getInputFormat());
        split.setInputFormat("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        Assertions.assertEquals("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat", split.getInputFormat());
    }

    @Test
    public void testBasePath() {
        Assertions.assertNull(split.getBasePath());
        split.setBasePath("/tmp/hudi_table");
        Assertions.assertEquals("/tmp/hudi_table", split.getBasePath());
    }

    @Test
    public void testDataFilePath() {
        Assertions.assertNull(split.getDataFilePath());
        split.setDataFilePath("/tmp/hudi_table/data.parquet");
        Assertions.assertEquals("/tmp/hudi_table/data.parquet", split.getDataFilePath());
    }

    @Test
    public void testHudiDeltaLogs() {
        Assertions.assertNull(split.getHudiDeltaLogs());
        List<String> deltaLogs = Arrays.asList("/tmp/delta1.log", "/tmp/delta2.log");
        split.setHudiDeltaLogs(deltaLogs);
        Assertions.assertEquals(2, split.getHudiDeltaLogs().size());
        Assertions.assertEquals("/tmp/delta1.log", split.getHudiDeltaLogs().get(0));
        Assertions.assertEquals("/tmp/delta2.log", split.getHudiDeltaLogs().get(1));
    }

    @Test
    public void testHudiColumnNames() {
        Assertions.assertNull(split.getHudiColumnNames());
        List<String> columnNames = Arrays.asList("id", "name", "age");
        split.setHudiColumnNames(columnNames);
        Assertions.assertEquals(3, split.getHudiColumnNames().size());
        Assertions.assertEquals("id", split.getHudiColumnNames().get(0));
    }

    @Test
    public void testHudiColumnTypes() {
        Assertions.assertNull(split.getHudiColumnTypes());
        List<String> columnTypes = Arrays.asList("int", "string", "int");
        split.setHudiColumnTypes(columnTypes);
        Assertions.assertEquals(3, split.getHudiColumnTypes().size());
        Assertions.assertEquals("string", split.getHudiColumnTypes().get(1));
    }

    @Test
    public void testPrimaryKeys() {
        Assertions.assertNull(split.getPrimaryKeys());
        List<String> keys = Arrays.asList("id", "ts");
        split.setPrimaryKeys(keys);
        Assertions.assertEquals(2, split.getPrimaryKeys().size());
        Assertions.assertEquals("id", split.getPrimaryKeys().get(0));
        Assertions.assertEquals("ts", split.getPrimaryKeys().get(1));
    }

    @Test
    public void testNestedFields() {
        Assertions.assertNull(split.getNestedFields());
        List<String> nested = Arrays.asList("struct_field.inner");
        split.setNestedFields(nested);
        Assertions.assertEquals(1, split.getNestedFields().size());
        Assertions.assertEquals("struct_field.inner", split.getNestedFields().get(0));
    }

    // ===================== New fields for new reader =====================

    @Test
    public void testSerializedInputSplit() {
        Assertions.assertNull(split.getSerializedInputSplit());
        split.setSerializedInputSplit("base64EncodedInputSplit");
        Assertions.assertEquals("base64EncodedInputSplit", split.getSerializedInputSplit());
    }

    @Test
    public void testDatabaseName() {
        Assertions.assertNull(split.getDatabaseName());
        split.setDatabaseName("test_db");
        Assertions.assertEquals("test_db", split.getDatabaseName());
    }

    @Test
    public void testTableName() {
        Assertions.assertNull(split.getTableName());
        split.setTableName("test_table");
        Assertions.assertEquals("test_table", split.getTableName());
    }

    @Test
    public void testStartInstant() {
        Assertions.assertNull(split.getStartInstant());
        split.setStartInstant("20230401010101");
        Assertions.assertEquals("20230401010101", split.getStartInstant());
    }

    @Test
    public void testEndInstant() {
        Assertions.assertNull(split.getEndInstant());
        split.setEndInstant("20230501010101");
        Assertions.assertEquals("20230501010101", split.getEndInstant());
    }

    @Test
    public void testSerializedHoodieTable() {
        Assertions.assertNull(split.getSerializedHoodieTable());
        split.setSerializedHoodieTable("base64EncodedHoodieTable");
        Assertions.assertEquals("base64EncodedHoodieTable", split.getSerializedHoodieTable());
    }

    // ===================== Combined / Integration tests =====================

    @Test
    public void testAllOldFieldsTogether() {
        split.setInstantTime("20230401010101");
        split.setSerde("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        split.setInputFormat("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        split.setBasePath("/base/path");
        split.setDataFilePath("/base/path/data.parquet");
        split.setHudiDeltaLogs(Arrays.asList("/delta1.log", "/delta2.log"));
        split.setHudiColumnNames(Arrays.asList("id", "name"));
        split.setHudiColumnTypes(Arrays.asList("int", "string"));
        split.setPrimaryKeys(Collections.singletonList("id"));
        split.setNestedFields(Collections.emptyList());

        Assertions.assertEquals("20230401010101", split.getInstantTime());
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", split.getSerde());
        Assertions.assertEquals("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat", split.getInputFormat());
        Assertions.assertEquals("/base/path", split.getBasePath());
        Assertions.assertEquals("/base/path/data.parquet", split.getDataFilePath());
        Assertions.assertEquals(2, split.getHudiDeltaLogs().size());
        Assertions.assertEquals(2, split.getHudiColumnNames().size());
        Assertions.assertEquals(2, split.getHudiColumnTypes().size());
        Assertions.assertEquals(1, split.getPrimaryKeys().size());
        Assertions.assertTrue(split.getNestedFields().isEmpty());
    }

    @Test
    public void testAllNewReaderFieldsTogether() {
        split.setSerializedInputSplit("serializedSplitData");
        split.setDatabaseName("production_db");
        split.setTableName("events_table");
        split.setStartInstant("20230101000000");
        split.setEndInstant("20230201000000");
        split.setSerializedHoodieTable("serializedTableData");

        Assertions.assertEquals("serializedSplitData", split.getSerializedInputSplit());
        Assertions.assertEquals("production_db", split.getDatabaseName());
        Assertions.assertEquals("events_table", split.getTableName());
        Assertions.assertEquals("20230101000000", split.getStartInstant());
        Assertions.assertEquals("20230201000000", split.getEndInstant());
        Assertions.assertEquals("serializedTableData", split.getSerializedHoodieTable());
    }

    @Test
    public void testOldAndNewFieldsCoexist() {
        split.setInstantTime("20230401010101");
        split.setBasePath("/base");
        split.setDataFilePath("/base/data.parquet");
        split.setSerde("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        split.setInputFormat("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        split.setHudiColumnNames(Arrays.asList("id", "name"));
        split.setHudiColumnTypes(Arrays.asList("int", "string"));
        split.setPrimaryKeys(Collections.singletonList("id"));
        split.setHudiDeltaLogs(Collections.emptyList());
        split.setNestedFields(Collections.emptyList());

        split.setSerializedInputSplit("splitData");
        split.setDatabaseName("db1");
        split.setTableName("tbl1");
        split.setStartInstant("start");
        split.setEndInstant("end");
        split.setSerializedHoodieTable("tableData");

        Assertions.assertEquals("20230401010101", split.getInstantTime());
        Assertions.assertEquals("splitData", split.getSerializedInputSplit());
        Assertions.assertEquals("db1", split.getDatabaseName());
        Assertions.assertEquals("tbl1", split.getTableName());
        Assertions.assertEquals("start", split.getStartInstant());
        Assertions.assertEquals("end", split.getEndInstant());
        Assertions.assertEquals("tableData", split.getSerializedHoodieTable());
    }

    @Test
    public void testNewFieldsWithNullValues() {
        Assertions.assertNull(split.getSerializedInputSplit());
        Assertions.assertNull(split.getDatabaseName());
        Assertions.assertNull(split.getTableName());
        Assertions.assertNull(split.getStartInstant());
        Assertions.assertNull(split.getEndInstant());
        Assertions.assertNull(split.getSerializedHoodieTable());

        split.setSerializedInputSplit(null);
        split.setDatabaseName(null);
        split.setTableName(null);
        split.setStartInstant(null);
        split.setEndInstant(null);
        split.setSerializedHoodieTable(null);

        Assertions.assertNull(split.getSerializedInputSplit());
        Assertions.assertNull(split.getDatabaseName());
        Assertions.assertNull(split.getTableName());
        Assertions.assertNull(split.getStartInstant());
        Assertions.assertNull(split.getEndInstant());
        Assertions.assertNull(split.getSerializedHoodieTable());
    }

    @Test
    public void testNewFieldsWithEmptyStrings() {
        split.setSerializedInputSplit("");
        split.setDatabaseName("");
        split.setTableName("");
        split.setStartInstant("");
        split.setEndInstant("");
        split.setSerializedHoodieTable("");

        Assertions.assertEquals("", split.getSerializedInputSplit());
        Assertions.assertEquals("", split.getDatabaseName());
        Assertions.assertEquals("", split.getTableName());
        Assertions.assertEquals("", split.getStartInstant());
        Assertions.assertEquals("", split.getEndInstant());
        Assertions.assertEquals("", split.getSerializedHoodieTable());
    }

    @Test
    public void testInheritedFileSplitFields() {
        Assertions.assertNotNull(split.getPath());
        Assertions.assertEquals(0, split.getModificationTime());
    }

    @Test
    public void testSetAndOverwriteNewFields() {
        split.setDatabaseName("db_v1");
        Assertions.assertEquals("db_v1", split.getDatabaseName());
        split.setDatabaseName("db_v2");
        Assertions.assertEquals("db_v2", split.getDatabaseName());

        split.setSerializedInputSplit("split_v1");
        split.setSerializedInputSplit("split_v2");
        Assertions.assertEquals("split_v2", split.getSerializedInputSplit());
    }

    @Test
    public void testLongBase64SerializedInputSplit() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("ABCDEFGHabcdefgh12345678");
        }
        String longBase64 = sb.toString();
        split.setSerializedInputSplit(longBase64);
        Assertions.assertEquals(longBase64, split.getSerializedInputSplit());
        Assertions.assertEquals(24000, split.getSerializedInputSplit().length());
    }

    @Test
    public void testLongBase64SerializedHoodieTable() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            sb.append("SERIALIZED_TABLE_DATA_");
        }
        String longData = sb.toString();
        split.setSerializedHoodieTable(longData);
        Assertions.assertEquals(longData, split.getSerializedHoodieTable());
    }
}
