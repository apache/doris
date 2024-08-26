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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ConfigException;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CreateTableCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
    }

    @Override
    public void createTable(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        ((CreateTableCommand) plan).run(connectContext, null);
    }

    private <T extends Throwable> void checkThrow(Class<T> exception, String msg, Executable executable) {
        Assertions.assertThrows(exception, executable, msg);
    }

    private <T extends Throwable> void checkThrow(Class<T> exception, Executable executable) {
        Assertions.assertThrows(exception, executable);
    }

    @Test
    public void testDuplicateCreateTable() throws Exception {
        // test
        Env env = Env.getCurrentEnv();
        String sql = "create table if not exists test.tbl1_colocate\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1','colocate_with'='test'); ";
        createTable(sql);
        Set<Long> tabletIdSetAfterCreateFirstTable = env.getTabletInvertedIndex().getReplicaMetaTable().rowKeySet();
        Set<TabletMeta> tabletMetaSetBeforeCreateFirstTable =
                new HashSet<>(env.getTabletInvertedIndex().getTabletMetaTable().values());
        Set<Long> colocateTableIdBeforeCreateFirstTable = env.getColocateTableIndex().getTable2Group().keySet();
        Assertions.assertTrue(colocateTableIdBeforeCreateFirstTable.size() > 0);
        Assertions.assertTrue(tabletIdSetAfterCreateFirstTable.size() > 0);

        createTable(sql);
        // check whether tablet is cleared after duplicate create table
        Set<Long> tabletIdSetAfterDuplicateCreateTable1 = env.getTabletInvertedIndex().getReplicaMetaTable()
                .rowKeySet();
        Set<Long> tabletIdSetAfterDuplicateCreateTable2 = env.getTabletInvertedIndex().getBackingReplicaMetaTable()
                .columnKeySet();
        Set<Long> tabletIdSetAfterDuplicateCreateTable3 = env.getTabletInvertedIndex().getTabletMetaMap().keySet();
        Set<TabletMeta> tabletIdSetAfterDuplicateCreateTable4 =
                new HashSet<>(env.getTabletInvertedIndex().getTabletMetaTable().values());

        Assertions.assertEquals(tabletIdSetAfterCreateFirstTable, tabletIdSetAfterDuplicateCreateTable1);
        Assertions.assertEquals(tabletIdSetAfterCreateFirstTable, tabletIdSetAfterDuplicateCreateTable2);
        Assertions.assertEquals(tabletIdSetAfterCreateFirstTable, tabletIdSetAfterDuplicateCreateTable3);
        Assertions.assertEquals(tabletMetaSetBeforeCreateFirstTable, tabletIdSetAfterDuplicateCreateTable4);

        // check whether table id is cleared from colocate group after duplicate create table
        Set<Long> colocateTableIdAfterCreateFirstTable = env.getColocateTableIndex().getTable2Group().keySet();
        Assertions.assertEquals(colocateTableIdBeforeCreateFirstTable, colocateTableIdAfterCreateFirstTable);
    }

    @Test
    public void testNormal() throws DdlException, ConfigException {
        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.tbl1\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        Assertions.assertDoesNotThrow(() -> createTable("create table test.tbl2\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.tbl3\n" + "(k1 varchar(40), k2 int)\n" + "duplicate key(k1)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.tbl4\n" + "(k1 varchar(40), k2 int, v1 int sum)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        Assertions.assertDoesNotThrow(() -> createTable(
                "create table test.tbl5\n" + "(k1 varchar(40), k2 int, v1 int sum)\n" + "aggregate key(k1,k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        Assertions.assertDoesNotThrow(() -> createTable(
                "create table test.tbl6\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        Assertions.assertDoesNotThrow(() -> createTable("create table test.tbl7\n" + "(k1 varchar(40), k2 int)\n"
                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        ConfigBase.setMutableConfig("disable_storage_medium_check", "true");
        Assertions.assertDoesNotThrow(() -> createTable("create table test.tb7(key1 int, key2 varchar(10)) \n"
                + "distributed by hash(key1) buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');"));

        Assertions.assertDoesNotThrow(() -> createTable("create table test.compression1(key1 int, key2 varchar(10)) \n"
                + "distributed by hash(key1) buckets 1 \n"
                + "properties('replication_num' = '1', 'compression' = 'lz4f');"));

        Assertions.assertDoesNotThrow(() -> createTable("create table test.compression2(key1 int, key2 varchar(10)) \n"
                + "distributed by hash(key1) buckets 1 \n"
                + "properties('replication_num' = '1', 'compression' = 'snappy');"));

        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.tbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                        + "unique key(k1, k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                        + "'function_column.sequence_type' = 'int');"));

        /*
         * create table with list partition
         */
        // single partition column with single key
        Assertions.assertDoesNotThrow(() -> createTable("create table test.tbl9\n"
                + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                + "partition by list(k1)\n"
                + "(\n"
                + "partition p1 values in (\"1\"),\n"
                + "partition p2 values in (\"2\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');"));

        // single partition column with multi keys
        Assertions.assertDoesNotThrow(() -> createTable("create table test.tbl10\n"
                + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                + "partition by list(k1)\n"
                + "(\n"
                + "partition p1 values in (\"1\", \"3\", \"5\"),\n"
                + "partition p2 values in (\"2\", \"4\", \"6\"),\n"
                + "partition p3 values in (\"7\", \"8\")\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');"));

        // multi partition columns with single key
        Assertions.assertDoesNotThrow(() -> createTable("create table test.tbl11\n"
                + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                + "partition by list(k1, k2)\n"
                + "(\n"
                + "partition p1 values in ((\"1\", \"beijing\")),\n"
                + "partition p2 values in ((\"2\", \"beijing\"))\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');"));

        // multi partition columns with multi keys
        Assertions.assertDoesNotThrow(() -> createTable("create table test.tbl12\n"
                + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                + "partition by list(k1, k2)\n"
                + "(\n"
                + "partition p1 values in ((\"1\", \"beijing\"), (\"1\", \"shanghai\")),\n"
                + "partition p2 values in ((\"2\", \"beijing\"), (\"2\", \"shanghai\")),\n"
                + "partition p3 values in ((\"3\", \"tianjin\"))\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');"));

        // table with sequence col
        Assertions.assertDoesNotThrow(() -> createTable("create table test.tbl13\n"
                + "(k1 varchar(40), k2 int, v1 int)\n"
                + "unique key(k1, k2)\n"
                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                + "'function_column.sequence_col' = 'v1');"));

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        OlapTable tbl6 = (OlapTable) db.getTableOrDdlException("tbl6");
        Assertions.assertTrue(tbl6.getColumn("k1").isKey());
        Assertions.assertTrue(tbl6.getColumn("k2").isKey());
        Assertions.assertTrue(tbl6.getColumn("k3").isKey());

        OlapTable tbl7 = (OlapTable) db.getTableOrDdlException("tbl7");
        Assertions.assertTrue(tbl7.getColumn("k1").isKey());
        Assertions.assertFalse(tbl7.getColumn("k2").isKey());
        Assertions.assertSame(tbl7.getColumn("k2").getAggregationType(), AggregateType.NONE);

        OlapTable tbl8 = (OlapTable) db.getTableOrDdlException("tbl8");
        Assertions.assertTrue(tbl8.getColumn("k1").isKey());
        Assertions.assertTrue(tbl8.getColumn("k2").isKey());
        Assertions.assertFalse(tbl8.getColumn("v1").isKey());
        Assertions.assertSame(tbl8.getColumn(Column.SEQUENCE_COL).getAggregationType(), AggregateType.NONE);

        OlapTable tbl13 = (OlapTable) db.getTableOrDdlException("tbl13");
        Assertions.assertSame(tbl13.getColumn(Column.SEQUENCE_COL).getAggregationType(), AggregateType.NONE);
        Assertions.assertSame(tbl13.getColumn(Column.SEQUENCE_COL).getType(), Type.INT);
        Assertions.assertEquals(tbl13.getSequenceMapCol(), "v1");
    }

    @Test
    public void testAbnormal() throws ConfigException {
        checkThrow(org.apache.doris.common.DdlException.class,
                "Unknown properties: {aa=bb}",
                () -> createTable("create table test.atbl1\n" + "(k1 int, k2 float)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1','aa'='bb'); "));

        checkThrow(org.apache.doris.common.DdlException.class,
                "Floating point type should not be used in distribution column",
                () -> createTable("create table test.atbl1\n" + "(k1 int, k2 float)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        checkThrow(AnalysisException.class,
                "Floating point type column can not be partition column",
                () -> createTable("create table test.atbl3\n" + "(k1 int, k2 int, k3 float)\n" + "duplicate key(k1)\n"
                        + "partition by range(k3)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        checkThrow(org.apache.doris.common.DdlException.class,
                "Varchar should not in the middle of short keys",
                () -> createTable("create table test.atbl3\n" + "(k1 varchar(40), k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1', 'short_key' = '3');"));

        checkThrow(org.apache.doris.common.DdlException.class, "Short key is too large. should less than: 3",
                () -> createTable("create table test.atbl4\n" + "(k1 int, k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1', 'short_key' = '4');"));

        checkThrow(org.apache.doris.common.DdlException.class,
                "replication num should be less than the number of available backends. replication num is 3, available backend num is 1",
                () -> createTable("create table test.atbl5\n" + "(k1 int, k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '3');"));

        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.atbl6\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        checkThrow(org.apache.doris.common.DdlException.class, "Table 'atbl6' already exists",
                () -> createTable("create table test.atbl6\n" + "(k1 int, k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        ConfigBase.setMutableConfig("disable_storage_medium_check", "false");
        checkThrow(org.apache.doris.common.DdlException.class,
                "Failed to find enough backend, please check the replication num,replication tag and storage medium.\n"
                        + "Create failed replications:\n"
                        + "replication tag: {\"location\" : \"default\"}, replication num: 1, storage medium: SSD",
                () -> createTable("create table test.tb7(key1 int, key2 varchar(10)) distributed by hash(key1) \n"
                        + "buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');"));

        checkThrow(org.apache.doris.common.DdlException.class, "sequence column only support UNIQUE_KEYS",
                () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int sum)\n"
                        + "aggregate key(k1, k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                        + "'function_column.sequence_type' = 'int');"));

        checkThrow(org.apache.doris.common.DdlException.class,
                "sequence type only support integer types and date types",
                () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                        + "unique key(k1, k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                        + "'function_column.sequence_type' = 'double');"));

        checkThrow(org.apache.doris.common.DdlException.class, "The sequence_col and sequence_type cannot be set at the same time",
                () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                        + "unique key(k1, k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                        + "'function_column.sequence_type' = 'int', 'function_column.sequence_col' = 'v1');"));

        checkThrow(org.apache.doris.common.DdlException.class, "The specified sequence column[v3] not exists",
                () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                        + "unique key(k1, k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                        + "'function_column.sequence_col' = 'v3');"));

        checkThrow(org.apache.doris.common.DdlException.class, "Sequence type only support integer types and date types",
                () -> createTable("create table test.atbl8\n" + "(k1 varchar(40), k2 int, v1 int)\n"
                        + "unique key(k1, k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1',\n"
                        + "'function_column.sequence_col' = 'k1');"));

        /*
         * create table with list partition
         */
        // single partition column with single key
        checkThrow(ParseException.class, () -> createTable("create table test.tbl9\n"
                + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                + "partition by list(k1)\n"
                + "(\n"
                + "partition p1 values in (\"1\"),\n"
                + "partition p2 values in ()\n"
                + ")\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');"));

        // single partition column with multi keys
        checkThrow(org.apache.doris.common.AnalysisException.class,
                "partition key desc list size[2] is not equal to partition column size[1]",
                () -> createTable("create table test.tbl10\n"
                        + "(k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int)\n"
                        + "partition by list(k1)\n"
                        + "(\n"
                        + "partition p1 values in (\"1\", \"3\", \"5\"),\n"
                        + "partition p2 values in (\"2\", \"4\", \"6\"),\n"
                        + "partition p3 values in ((\"7\", \"8\"))\n"
                        + ")\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        // multi partition columns with single key
        checkThrow(IllegalArgumentException.class,
                "partition key desc list size[1] is not equal to partition column size[2]",
                () -> createTable("create table test.tbl11\n"
                        + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                        + "partition by list(k1, k2)\n"
                        + "(\n"
                        + "partition p1 values in ((\"1\", \"beijing\")),\n"
                        + "partition p2 values in (\"2\", \"beijing\")\n"
                        + ")\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        // multi partition columns with multi keys
        checkThrow(org.apache.doris.common.AnalysisException.class,
                "partition key desc list size[3] is not equal to partition column size[2]",
                () -> createTable("create table test.tbl12\n"
                        + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                        + "partition by list(k1, k2)\n"
                        + "(\n"
                        + "partition p1 values in ((\"1\", \"beijing\"), (\"1\", \"shanghai\")),\n"
                        + "partition p2 values in ((\"2\", \"beijing\"), (\"2\", \"shanghai\")),\n"
                        + "partition p3 values in ((\"3\", \"tianjin\", \"3\"))\n"
                        + ")\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        // multi partition columns with multi keys
        checkThrow(ParseException.class, () -> createTable("create table test.tbl13\n"
                        + "(k1 int not null, k2 varchar(128) not null, k3 int, v1 int, v2 int)\n"
                        + "partition by list(k1, k2)\n"
                        + "(\n"
                        + "partition p1 values in ((\"1\", \"beijing\"), (\"1\", \"shanghai\")),\n"
                        + "partition p2 values in ((\"2\", \"beijing\"), (\"2\", \"shanghai\")),\n"
                        + "partition p3 values in ()\n"
                        + ")\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        /*
         * create table with both list and range partition
         */
        // list contain less than
        checkThrow(AnalysisException.class,
                "partitions types is invalid, expected FIXED or LESS in range partitions and IN in list partitions",
                () -> createTable("CREATE TABLE test.tbl14 (\n"
                        + "    k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int\n"
                        + ")\n"
                        + "PARTITION BY LIST(k1)\n"
                        + "(\n"
                        + "    PARTITION p1 VALUES less than (\"1\"),\n"
                        + "    PARTITION p2 VALUES less than (\"2\"),\n"
                        + "    partition p3 values less than (\"5\")\n"
                        + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                        + "PROPERTIES(\"replication_num\" = \"1\");"));

        // range contain in
        checkThrow(AnalysisException.class,
                "partitions types is invalid, expected FIXED or LESS in range partitions and IN in list partitions",
                () -> createTable("CREATE TABLE test.tbl15 (\n"
                        + "    k1 int, k2 varchar(128), k3 int, v1 int, v2 int\n"
                        + ")\n"
                        + "PARTITION BY range(k1)\n"
                        + "(\n"
                        + "    PARTITION p1 VALUES in (\"1\"),\n"
                        + "    PARTITION p2 VALUES in (\"2\"),\n"
                        + "    partition p3 values in (\"5\")\n"
                        + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                        + "PROPERTIES(\"replication_num\" = \"1\");"));

        // list contain both
        checkThrow(AnalysisException.class,
                "partitions types is invalid, expected FIXED or LESS in range partitions and IN in list partitions",
                () -> createTable("CREATE TABLE test.tbl15 (\n"
                        + "    k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int\n"
                        + ")\n"
                        + "PARTITION BY LIST(k1)\n"
                        + "(\n"
                        + "    PARTITION p1 VALUES in (\"1\"),\n"
                        + "    PARTITION p2 VALUES in (\"2\"),\n"
                        + "    partition p3 values less than (\"5\")\n"
                        + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                        + "PROPERTIES(\"replication_num\" = \"1\");"));

        // range contain both
        checkThrow(AnalysisException.class,
                "partitions types is invalid, expected FIXED or LESS in range partitions and IN in list partitions",
                () -> createTable("CREATE TABLE test.tbl16 (\n"
                        + "    k1 int, k2 varchar(128), k3 int, v1 int, v2 int\n"
                        + ")\n"
                        + "PARTITION BY RANGE(k1)\n"
                        + "(\n"
                        + "    PARTITION p1 VALUES less than (\"1\"),\n"
                        + "    PARTITION p2 VALUES less than (\"2\"),\n"
                        + "    partition p3 values in (\"5\")\n"
                        + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                        + "PROPERTIES(\"replication_num\" = \"1\");"));

        // range: partition content != partition key type
        checkThrow(org.apache.doris.common.DdlException.class, "Invalid number format: beijing",
                () -> createTable("CREATE TABLE test.tbl17 (\n"
                        + "    k1 int, k2 varchar(128), k3 int, v1 int, v2 int\n"
                        + ")\n"
                        + "PARTITION BY range(k1)\n"
                        + "(\n"
                        + "    PARTITION p1 VALUES less than (\"beijing\"),\n"
                        + "    PARTITION p2 VALUES less than (\"shanghai\"),\n"
                        + "    partition p3 values less than (\"tianjin\")\n"
                        + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                        + "PROPERTIES(\"replication_num\" = \"1\");"));

        // list: partition content != partition key type
        checkThrow(org.apache.doris.common.DdlException.class, "Invalid number format: beijing",
                () -> createTable("CREATE TABLE test.tbl18 (\n"
                        + "    k1 int not null, k2 varchar(128), k3 int, v1 int, v2 int\n"
                        + ")\n"
                        + "PARTITION BY list(k1)\n"
                        + "(\n"
                        + "    PARTITION p1 VALUES in (\"beijing\"),\n"
                        + "    PARTITION p2 VALUES in (\"shanghai\"),\n"
                        + "    partition p3 values in (\"tianjin\")\n"
                        + ")DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                        + "PROPERTIES(\"replication_num\" = \"1\");"));

        /*
         * dynamic partition table
         */
        // list partition with dynamic properties
        checkThrow(org.apache.doris.common.DdlException.class, "Only support dynamic partition properties on range partition table",
                () -> createTable("CREATE TABLE test.tbl19\n"
                        + "(\n"
                        + "    k1 DATE not null\n"
                        + ")\n"
                        + "PARTITION BY LIST(k1) ()\n"
                        + "DISTRIBUTED BY HASH(k1)\n"
                        + "PROPERTIES\n"
                        + "(\n"
                        + "    \"dynamic_partition.enable\" = \"true\",\n"
                        + "    \"dynamic_partition.time_unit\" = \"MONTH\",\n"
                        + "    \"dynamic_partition.end\" = \"2\",\n"
                        + "    \"dynamic_partition.prefix\" = \"p\",\n"
                        + "    \"dynamic_partition.buckets\" = \"8\",\n"
                        + "    \"dynamic_partition.start_day_of_month\" = \"3\"\n"
                        + ");\n"));

        // no partition table with dynamic properties
        checkThrow(org.apache.doris.common.DdlException.class, "Only support dynamic partition properties on range partition table",
                () -> createTable("CREATE TABLE test.tbl20\n"
                        + "(\n"
                        + "    k1 DATE\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(k1)\n"
                        + "PROPERTIES\n"
                        + "(\n"
                        + "    \"dynamic_partition.enable\" = \"true\",\n"
                        + "    \"dynamic_partition.time_unit\" = \"MONTH\",\n"
                        + "    \"dynamic_partition.end\" = \"2\",\n"
                        + "    \"dynamic_partition.prefix\" = \"p\",\n"
                        + "    \"dynamic_partition.buckets\" = \"8\",\n"
                        + "    \"dynamic_partition.start_day_of_month\" = \"3\"\n"
                        + ");"));

        checkThrow(AnalysisException.class,
                "Create unique keys table should not contain random distribution desc",
                () -> createTable("CREATE TABLE test.tbl21\n"
                        + "(\n"
                        + "  `k1` bigint(20) NULL COMMENT \"\",\n"
                        + "  `k2` largeint(40) NULL COMMENT \"\",\n"
                        + "  `v1` varchar(204) NULL COMMENT \"\",\n"
                        + "  `v2` smallint(6) NULL DEFAULT \"10\" COMMENT \"\"\n"
                        + ") ENGINE=OLAP\n"
                        + "UNIQUE KEY(`k1`, `k2`)\n"
                        + "DISTRIBUTED BY RANDOM BUCKETS 32\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                        + ");"));

        checkThrow(AnalysisException.class,
                "Create aggregate keys table with value columns of which aggregate type"
                        + " is REPLACE or REPLACE_IF_NOT_NULL should not contain random distribution desc",
                () -> createTable("CREATE TABLE test.tbl22\n"
                        + "(\n"
                        + "  `k1` bigint(20) NULL COMMENT \"\",\n"
                        + "  `k2` largeint(40) NULL COMMENT \"\",\n"
                        + "  `v1` bigint(20) REPLACE NULL COMMENT \"\",\n"
                        + "  `v2` smallint(6) REPLACE_IF_NOT_NULL NULL DEFAULT \"10\" COMMENT \"\"\n"
                        + ") ENGINE=OLAP\n"
                        + "AGGREGATE KEY(`k1`, `k2`)\n"
                        + "DISTRIBUTED BY RANDOM BUCKETS 32\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                        + ");"));
    }

    @Test
    public void testZOrderTable() {
        // create lexically sort table
        Assertions.assertDoesNotThrow(() -> createTable(
                "create table test.zorder_tbl1\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                        + " 'data_sort.sort_type' = 'lexical');"));

        // create z-order sort table, default col_num
        checkThrow(org.apache.doris.common.AnalysisException.class, "only support lexical method now!",
                () -> createTable(
                        "create table test.zorder_tbl2\n" + "(k1 varchar(40), k2 int, k3 int)\n"
                                + "duplicate key(k1, k2, k3)\n"
                                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                                + " 'data_sort.sort_type' = 'zorder');"));

        // create z-order sort table, define sort_col_num
        checkThrow(org.apache.doris.common.AnalysisException.class, "only support lexical method now!",
                () -> createTable(
                        "create table test.zorder_tbl3\n" + "(k1 varchar(40), k2 int, k3 int)\n"
                                + "duplicate key(k1, k2, k3)\n"
                                + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                                + " 'data_sort.sort_type' = 'zorder',"
                                + " 'data_sort.col_num' = '2');"));
        // create z-order sort table, only 1 sort column
        checkThrow(org.apache.doris.common.AnalysisException.class, "only support lexical method now!",
                () -> createTable("create table test.zorder_tbl4\n" + "(k1 varchar(40), k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                        + " 'data_sort.sort_type' = 'zorder',"
                        + " 'data_sort.col_num' = '1');"));
        // create z-order sort table, sort column is empty
        checkThrow(org.apache.doris.common.AnalysisException.class, "only support lexical method now!",
                () -> createTable("create table test.zorder_tbl4\n" + "(k1 varchar(40), k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1',"
                        + " 'data_sort.sort_type' = 'zorder',"
                        + " 'data_sort.col_num' = '');"));
    }

    @Test
    public void testCreateTableWithArrayType() {
        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.table1(k1 INT, k2 Array<int>) duplicate key (k1) "
                        + "distributed by hash(k1) buckets 1 properties('replication_num' = '1');"));
        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.table2(k1 INT, k2 Array<Array<int>>) duplicate key (k1) "
                        + "distributed by hash(k1) buckets 1 properties('replication_num' = '1');"));
        Assertions.assertDoesNotThrow(
                () -> createTable("CREATE TABLE test.table3 (\n"
                        + "  `k1` INT(11) NULL COMMENT \"\",\n"
                        + "  `k2` ARRAY<ARRAY<SMALLINT>> NULL COMMENT \"\",\n"
                        + "  `k3` ARRAY<ARRAY<ARRAY<INT(11)>>> NULL COMMENT \"\",\n"
                        + "  `k4` ARRAY<ARRAY<ARRAY<ARRAY<BIGINT>>>> NULL COMMENT \"\",\n"
                        + "  `k5` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<CHAR>>>>> NULL COMMENT \"\",\n"
                        + "  `k6` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<VARCHAR(20)>>>>>> NULL COMMENT \"\",\n"
                        + "  `k7` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<DATE>>>>>>> NULL COMMENT \"\",\n"
                        + "  `k8` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<DATETIME>>>>>>>> NULL COMMENT \"\",\n"
                        + "  `k11` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<DECIMAL(20, 6)>>>>>>>>> NULL COMMENT \"\"\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`k1`)\n"
                        + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                        + ");"));
        checkThrow(AnalysisException.class, "Type exceeds the maximum nesting depth of 9",
                () -> createTable("CREATE TABLE test.table4 (\n"
                        + "  `k1` INT(11) NULL COMMENT \"\",\n"
                        + "  `k2` ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<DECIMAL(20, 6)>>>>>>>>>> NULL COMMENT \"\"\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`k1`)\n"
                        + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                        + ");"));

        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.table5(\n"
                        + "\tk1 int,\n"
                        + "\tv1 array<int>\n"
                        + ") distributed by hash(k1) buckets 1\n"
                        + "properties(\"replication_num\" = \"1\");"));

        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.test_array( \n"
                        + "task_insert_time BIGINT NOT NULL DEFAULT \"0\" COMMENT \"\" , \n"
                        + "task_project ARRAY<VARCHAR(64)>  DEFAULT NULL COMMENT \"\" ,\n"
                        + "route_key DATEV2 NOT NULL COMMENT \"range分区键\"\n"
                        + ") \n"
                        + "DUPLICATE KEY(`task_insert_time`)  \n"
                        + " COMMENT \"\"\n"
                        + "PARTITION BY RANGE(route_key) \n"
                        + "(PARTITION `p202209` VALUES LESS THAN (\"2022-10-01\"),\n"
                        + "PARTITION `p202210` VALUES LESS THAN (\"2022-11-01\"),\n"
                        + "PARTITION `p202211` VALUES LESS THAN (\"2022-12-01\")) \n"
                        + "DISTRIBUTED BY HASH(`task_insert_time` ) BUCKETS 32 \n"
                        + "PROPERTIES\n"
                        + "(\n"
                        + "    \"replication_num\" = \"1\",    \n"
                        + "    \"light_schema_change\" = \"true\"    \n"
                        + ");"));

        checkThrow(AnalysisException.class, "Complex type column can't be partition column: ARRAY<VARCHAR(64)>",
                () -> createTable("create table test.test_array2( \n"
                        + "task_insert_time BIGINT NOT NULL DEFAULT \"0\" COMMENT \"\" , \n"
                        + "task_project ARRAY<VARCHAR(64)>  DEFAULT NULL COMMENT \"\" ,\n"
                        + "route_key DATEV2 NOT NULL COMMENT \"range分区键\"\n"
                        + ") \n"
                        + "DUPLICATE KEY(`task_insert_time`)  \n"
                        + " COMMENT \"\"\n"
                        + "PARTITION BY RANGE(task_project) \n"
                        + "(PARTITION `p202209` VALUES LESS THAN (\"2022-10-01\"),\n"
                        + "PARTITION `p202210` VALUES LESS THAN (\"2022-11-01\"),\n"
                        + "PARTITION `p202211` VALUES LESS THAN (\"2022-12-01\")) \n"
                        + "DISTRIBUTED BY HASH(`task_insert_time` ) BUCKETS 32 \n"
                        + "PROPERTIES\n"
                        + "(\n"
                        + "    \"replication_num\" = \"1\",    \n"
                        + "    \"light_schema_change\" = \"true\"    \n"
                        + ");"));
    }

    @Test
    public void testCreateTableWithMapType() {
        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.test_map(k1 INT, k2 Map<int, VARCHAR(20)>) duplicate key (k1) "
                        + "distributed by hash(k1) buckets 1 properties('replication_num' = '1');"));
    }

    @Test
    public void testCreateTableWithStructType() {
        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.test_struct(k1 INT, k2 Struct<f1:int, f2:VARCHAR(20)>) duplicate key (k1) "
                        + "distributed by hash(k1) buckets 1 properties('replication_num' = '1');"));
    }

    @Test
    public void testCreateTableWithInMemory() {
        checkThrow(org.apache.doris.common.AnalysisException.class, "Not support set 'in_memory'='true' now!",
                () -> createTable("create table test.test_inmemory(k1 INT, k2 INT) duplicate key (k1) "
                        + "distributed by hash(k1) buckets 1 properties('replication_num' = '1','in_memory'='true');"));
    }

    @Test
    public void testCreateTableWithStringLen() throws DdlException {
        Assertions.assertDoesNotThrow(
                () -> createTable("create table test.test_strLen(k1 CHAR, k2 CHAR(10), k3 VARCHAR, k4 VARCHAR(10))"
                        + " duplicate key (k1) distributed by hash(k1) buckets 1 properties('replication_num' = '1');"));
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        OlapTable tb = (OlapTable) db.getTableOrDdlException("test_strLen");
        Assertions.assertEquals(1, tb.getColumn("k1").getStrLen());
        Assertions.assertEquals(10, tb.getColumn("k2").getStrLen());
        Assertions.assertEquals(ScalarType.MAX_VARCHAR_LENGTH, tb.getColumn("k3").getStrLen());
        Assertions.assertEquals(10, tb.getColumn("k4").getStrLen());
    }

    @Test
    public void testCreateTablePartitionForExternalCatalog() {

        PartitionDesc par = null;

        par = getCreateTableStmt("create table tb1 (id int not null, id2 int not null, id3 int not null)"
                + "partition by (id, id3) () distributed by hash (id) properties (\"a\"=\"b\")");
        Assertions.assertEquals("PARTITION BY LIST(`id`, `id3`)\n(\n\n)", par.toSql());

        try {
            getCreateTableStmt("create table tb1 (id int not null, id2 int not null, id3 int not null)"
                    + "partition by (id, func1(id2, 1), func(3,id1), id3) () "
                    + "distributed by hash (id) properties (\"a\"=\"b\")");
        } catch (Exception e) {
            Assertions.assertEquals(
                    "errCode = 2, detailMessage = auto create partition only support slotRef in list partitions. func1(`id2`, '1')",
                    e.getMessage());
        }

        try {
            getCreateTableStmt("create table tb1 (id int not null, id2 int not null, id3 int not null) "
                    + "ENGINE=iceberg partition by (id, func1(id2, 1), func(3,id1), id3) () "
                    + "distributed by hash (id) properties (\"a\"=\"b\")");
        } catch (Exception e) {
            Assertions.assertEquals(
                    "Iceberg doesn't support 'DISTRIBUTE BY', "
                            + "and you can use 'bucket(num, column)' in 'PARTITIONED BY'.",
                    e.getMessage());
        }

        par = getCreateTableStmt("create table tb1 (id int not null, id2 int not null, id3 int not null) "
                + "ENGINE=iceberg partition by (id, func1(id2, 1), func(3,id1), id3) () properties (\"a\"=\"b\")");
        Assertions.assertEquals(
                "PARTITION BY LIST(`id`, func1(`id2`, '1'), func('3', `id1`), `id3`)\n" + "(\n" + "\n" + ")",
                par.toSql());

        try {
            getCreateTableStmt(
                    "create table tb1 (id int) "
                    + "engine = iceberg rollup (ab (cd)) properties (\"a\"=\"b\")");
        } catch (Exception e) {
            Assertions.assertEquals(
                    "iceberg catalog doesn't support rollup tables.",
                     e.getMessage());
        }

        try {
            getCreateTableStmt("create table tb1 (id int) engine = hive rollup (ab (cd)) properties (\"a\"=\"b\")");
        } catch (Exception e) {
            Assertions.assertEquals(
                    "hive catalog doesn't support rollup tables.",
                    e.getMessage());
        }

        // test with empty partitions
        LogicalPlan plan = new NereidsParser().parseSingle(
                "create table tb1 (id int) engine = iceberg properties (\"a\"=\"b\")");
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        CreateTableInfo createTableInfo = ((CreateTableCommand) plan).getCreateTableInfo();
        createTableInfo.validate(connectContext);
        Assertions.assertNull(createTableInfo.translateToLegacyStmt().getPartitionDesc());

        // test with multi partitions
        LogicalPlan plan2 = new NereidsParser().parseSingle(
                "create table tb1 (id int) engine = iceberg "
                    + "partition by (val, bucket(2, id), par, day(ts), efg(a,b,c)) () properties (\"a\"=\"b\")");
        Assertions.assertTrue(plan2 instanceof CreateTableCommand);
        CreateTableInfo createTableInfo2 = ((CreateTableCommand) plan2).getCreateTableInfo();
        createTableInfo2.validate(connectContext);
        PartitionDesc partitionDesc2 = createTableInfo2.translateToLegacyStmt().getPartitionDesc();
        List<Expr> partitionFields2 = partitionDesc2.getPartitionExprs();
        Assertions.assertEquals(5, partitionFields2.size());

        Expr expr0 = partitionFields2.get(0);
        Assertions.assertInstanceOf(SlotRef.class, expr0);
        Assertions.assertEquals("val", expr0.getExprName());

        Expr expr1 = partitionFields2.get(1);
        Assertions.assertInstanceOf(FunctionCallExpr.class, expr1);
        List<Expr> params1 = ((FunctionCallExpr) expr1).getParams().exprs();
        Assertions.assertEquals("bucket", expr1.getExprName());
        Assertions.assertEquals(2, params1.size());
        Assertions.assertInstanceOf(StringLiteral.class, params1.get(0));
        Assertions.assertEquals("2", params1.get(0).getStringValue());
        Assertions.assertInstanceOf(SlotRef.class, params1.get(1));
        Assertions.assertEquals("id", params1.get(1).getExprName());

        Expr expr2 = partitionFields2.get(2);
        Assertions.assertInstanceOf(SlotRef.class, expr2);
        Assertions.assertEquals("par", expr2.getExprName());

        Expr expr3 = partitionFields2.get(3);
        Assertions.assertInstanceOf(FunctionCallExpr.class, expr3);
        List<Expr> params3 = ((FunctionCallExpr) expr3).getParams().exprs();
        Assertions.assertEquals("day", expr3.getExprName());
        Assertions.assertEquals(1, params3.size());
        Assertions.assertInstanceOf(SlotRef.class, params3.get(0));
        Assertions.assertEquals("ts", params3.get(0).getExprName());

        Expr expr4 = partitionFields2.get(4);
        Assertions.assertInstanceOf(FunctionCallExpr.class, expr4);
        List<Expr> params4 = ((FunctionCallExpr) expr4).getParams().exprs();
        Assertions.assertEquals("efg", expr4.getExprName());
        Assertions.assertEquals(3, params4.size());
        Assertions.assertInstanceOf(SlotRef.class, params4.get(0));
        Assertions.assertEquals("a", params4.get(0).getExprName());
        Assertions.assertInstanceOf(SlotRef.class, params4.get(1));
        Assertions.assertEquals("b", params4.get(1).getExprName());
        Assertions.assertInstanceOf(SlotRef.class, params4.get(2));
        Assertions.assertEquals("c", params4.get(2).getExprName());

        Assertions.assertEquals(
                "PARTITION BY LIST(`val`, bucket('2', `id`), `par`, day(`ts`), efg(`a`, `b`, `c`))\n(\n\n)",
                partitionDesc2.toSql());
    }

    private PartitionDesc getCreateTableStmt(String sql) {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        CreateTableInfo createTableInfo = ((CreateTableCommand) plan).getCreateTableInfo();
        createTableInfo.validate(connectContext);
        return createTableInfo.translateToLegacyStmt().getPartitionDesc();
    }

    @Test
    public void testPartitionCheckForHive() {
        try {
            getCreateTableStmt("CREATE TABLE `tb11`(\n"
                    + "    `par1` int\n"
                    + ") ENGINE = hive PARTITION BY LIST (\n"
                    + "    par1\n"
                    + ")();");
            Assertions.assertTrue(false);
        } catch (Exception e) {
            Assertions.assertEquals("Cannot set all columns as partitioning columns.", e.getMessage());
        }
        try {
            getCreateTableStmt("CREATE TABLE `tb11`(\n"
                    + "    `par1` int,\n"
                    + "    `c1` bigint\n"
                    + ") ENGINE = hive PARTITION BY LIST (\n"
                    + "    par1\n"
                    + ")();");
            Assertions.assertTrue(false);
        } catch (Exception e) {
            Assertions.assertEquals(
                    "The partition field must be at the end of the schema.",
                    e.getMessage());
        }
        try {
            getCreateTableStmt("CREATE TABLE `tb11`(\n"
                    + "    `c1` bigint,\n"
                    + "    `par2` int,\n"
                    + "    `par1` int\n"
                    + ") ENGINE = hive PARTITION BY LIST (\n"
                    + "    par1, par2\n"
                    + ")();");
            Assertions.assertTrue(false);
        } catch (Exception e) {
            Assertions.assertEquals(
                    "The order of partition fields in the schema "
                            + "must be consistent with the order defined in `PARTITIONED BY LIST()`",
                    e.getMessage());
        }
        try {
            getCreateTableStmt("CREATE TABLE `tb11`(\n"
                    + "    `c1` bigint,\n"
                    + "    `par2` int\n"
                    + ") ENGINE = hive PARTITION BY LIST (\n"
                    + "    par1, par2, par3 ,par4\n"
                    + ")();");
            Assertions.assertTrue(false);
        } catch (Exception e) {
            Assertions.assertEquals("partition key par1 is not exists", e.getMessage());
        }

        try {
            getCreateTableStmt("CREATE TABLE `tb11`(\n"
                    + "    `c1` bigint,\n"
                    + "    `par1` int,\n"
                    + "    `par2` int,\n"
                    + "    `par3` int\n"
                    + ") ENGINE = hive PARTITION BY LIST (\n"
                    + "    par1, par2\n"
                    + ")();");
            Assertions.assertTrue(false);
        } catch (Exception e) {
            Assertions.assertEquals("The partition field must be at the end of the schema.", e.getMessage());
        }
    }
}
