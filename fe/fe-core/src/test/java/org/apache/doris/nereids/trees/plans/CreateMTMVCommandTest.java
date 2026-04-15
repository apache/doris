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

import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.Config;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.FixedRangePartition;
import org.apache.doris.nereids.trees.plans.commands.info.InPartition;
import org.apache.doris.nereids.trees.plans.commands.info.LessThanPartition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class CreateMTMVCommandTest extends TestWithFeService {
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

    @Test
    public void testUnpartitionConvertToPartitionTableInfo() throws Exception {
        String partitionTable = "CREATE TABLE aa1 (\n"
                + " `user_id` LARGEINT NOT NULL COMMENT '\\\"用户id\\\"',\n"
                + " `date` DATE NOT NULL COMMENT '\\\"数据灌入日期时间\\\"',\n"
                + " `num` SMALLINT NOT NULL COMMENT '\\\"数量\\\"'\n"
                + " ) ENGINE=OLAP\n"
                + " DUPLICATE KEY(`user_id`, `date`, `num`)\n"
                + " COMMENT 'OLAP'\n"
                + " PARTITION BY RANGE(`date`)\n"
                + " (\n"
                + " PARTITION `p201701` VALUES [(\"2017-01-01\"),  (\"2017-02-01\")),\n"
                + " PARTITION `p201702` VALUES [(\"2017-02-01\"), (\"2017-03-01\")),\n"
                + " PARTITION `p201703` VALUES [(\"2017-03-01\"), (\"2017-04-01\"))\n"
                + " )\n"
                + " DISTRIBUTED BY HASH(`user_id`) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1') ;\n";
        createTable(partitionTable);

        String mv = "CREATE MATERIALIZED VIEW mtmv5\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT * FROM aa1;";

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo(mv);
        Assertions.assertEquals(PartitionTableInfo.EMPTY, createMTMVInfo.getPartitionTableInfo());
    }

    @Test
    public void testRangePartitionConvertToPartitionTableInfo() throws Exception {
        String fixedRangePartitionTable = "CREATE TABLE mm1 (\n"
                + " `user_id` LARGEINT NOT NULL COMMENT '\\\"用户id\\\"',\n"
                + " `date` DATE NOT NULL COMMENT '\\\"数据灌入日期时间\\\"',\n"
                + " `num` SMALLINT NOT NULL COMMENT '\\\"数量\\\"'\n"
                + " ) ENGINE=OLAP\n"
                + " DUPLICATE KEY(`user_id`, `date`, `num`)\n"
                + " COMMENT 'OLAP'\n"
                + " PARTITION BY RANGE(`date`)\n"
                + " (\n"
                + " PARTITION `p201701` VALUES [(\"2017-01-01\"),  (\"2017-02-01\")),\n"
                + " PARTITION `p201702` VALUES [(\"2017-02-01\"), (\"2017-03-01\")),\n"
                + " PARTITION `p201703` VALUES [(\"2017-03-01\"), (\"2017-04-01\"))\n"
                + " )\n"
                + " DISTRIBUTED BY HASH(`user_id`) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1') ;\n";

        String mv = "CREATE MATERIALIZED VIEW mtmv1\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " partition by(`date`)\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT * FROM mm1;";

        check(fixedRangePartitionTable, mv);
    }

    @Test
    public void testLessThanPartitionConvertToPartitionTableInfo() throws Exception {
        String lessThanPartitionTable = "CREATE TABLE te2 (\n"
                + " `user_id` LARGEINT NOT NULL COMMENT '\\\"用户id\\\"',\n"
                + " `date` DATE NOT NULL COMMENT '\\\"数据灌入日期时间\\\"',\n"
                + " `num` SMALLINT NOT NULL COMMENT '\\\"数量\\\"'\n"
                + " ) ENGINE=OLAP\n"
                + " DUPLICATE KEY(`user_id`, `date`, `num`)\n"
                + " COMMENT 'OLAP'\n"
                + " PARTITION BY RANGE(`date`)\n"
                + "(\n"
                + " PARTITION `p201701` VALUES LESS THAN (\"2017-02-01\"),\n"
                + " PARTITION `p201702` VALUES LESS THAN (\"2017-03-01\"),\n"
                + " PARTITION `p201703` VALUES LESS THAN (\"2017-04-01\"),\n"
                + " PARTITION `p2018` VALUES [(\"2018-01-01\"), (\"2019-01-01\")),\n"
                + " PARTITION `other` VALUES LESS THAN (MAXVALUE)\n"
                + ")\n"
                + " DISTRIBUTED BY HASH(`user_id`) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1') ;";

        String mv = "CREATE MATERIALIZED VIEW mtmv2\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " partition by(`date`)\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT * FROM te2;";

        check(lessThanPartitionTable, mv);
    }

    @Test
    public void testInPartitionConvertToPartitionTableInfo() throws Exception {
        String inPartitionTable = "CREATE TABLE cc1 (\n"
                + "`user_id` LARGEINT NOT NULL COMMENT '\\\"用户id\\\"',\n"
                + "`date` DATE NOT NULL COMMENT '\\\"数据灌入日期时间\\\"',\n"
                + "`num` SMALLINT NOT NULL COMMENT '\\\"数量\\\"'\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`user_id`, `date`, `num`)\n"
                + "COMMENT 'OLAP'\n"
                + "PARTITION BY LIST(`date`,`num`)\n"
                + "(\n"
                + " PARTITION p201701_1000 VALUES IN (('2017-01-01',1), ('2017-01-01',2)),\n"
                + " PARTITION p201702_2000 VALUES IN (('2017-02-01',3), ('2017-02-01',4))\n"
                + " )\n"
                + " DISTRIBUTED BY HASH(`user_id`) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1') ;";

        String mv = "CREATE MATERIALIZED VIEW mtmv\n"
                + "BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + "partition by(`date`)\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS\n"
                + "SELECT * FROM cc1;";

        check(inPartitionTable, mv);
    }

    private void check(String sql, String mv) throws Exception {
        createTable(sql);

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo(mv);
        PartitionTableInfo partitionTableInfo = createMTMVInfo.getPartitionTableInfo();
        PartitionDesc partitionDesc = createMTMVInfo.getPartitionDesc();

        List<PartitionDefinition> partitionDefs = partitionTableInfo.getPartitionDefs();
        List<SinglePartitionDesc> singlePartitionDescs = partitionDesc.getSinglePartitionDescs();

        assertPartitionInfo(partitionDefs, singlePartitionDescs);
    }

    private void assertPartitionInfo(List<PartitionDefinition> partitionDefs, List<SinglePartitionDesc> singlePartitionDescs) {
        Assertions.assertEquals(singlePartitionDescs.size(), partitionDefs.size());

        for (int i = 0; i < singlePartitionDescs.size(); i++) {
            PartitionDefinition partitionDefinition = partitionDefs.get(i);
            SinglePartitionDesc singlePartitionDesc = singlePartitionDescs.get(i);

            if (partitionDefinition instanceof InPartition) {
                InPartition inPartition = (InPartition) partitionDefinition;

                Assertions.assertEquals(singlePartitionDesc.getPartitionName(), partitionDefinition.getPartitionName());
                Assertions.assertEquals(singlePartitionDesc.getPartitionKeyDesc().getPartitionType().name(), "IN");
                Assertions.assertEquals(singlePartitionDesc.getPartitionKeyDesc().getInValues().size(), inPartition.getValues().size());
            } else if (partitionDefinition instanceof FixedRangePartition) {
                FixedRangePartition fixedRangePartition = (FixedRangePartition) partitionDefinition;

                Assertions.assertEquals(singlePartitionDesc.getPartitionName(), partitionDefinition.getPartitionName());
                Assertions.assertEquals(singlePartitionDesc.getPartitionKeyDesc().getPartitionType().name(), "FIXED");
                Assertions.assertEquals(fixedRangePartition.getLowerBounds().size(), singlePartitionDesc.getPartitionKeyDesc().getLowerValues().size());
                Assertions.assertEquals(fixedRangePartition.getUpperBounds().size(), singlePartitionDesc.getPartitionKeyDesc().getUpperValues().size());
            } else if (partitionDefinition instanceof LessThanPartition) {
                LessThanPartition lessThanPartition = (LessThanPartition) partitionDefinition;

                Assertions.assertEquals(singlePartitionDesc.getPartitionName(), partitionDefinition.getPartitionName());
                Assertions.assertEquals(singlePartitionDesc.getPartitionKeyDesc().getPartitionType().name(), "LESS_THAN");
                Assertions.assertEquals(lessThanPartition.getValues().size(), singlePartitionDesc.getPartitionKeyDesc().getUpperValues().size());
            }
        }
    }

    private CreateMTMVInfo getPartitionTableInfo(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof CreateMTMVCommand);
        CreateMTMVCommand command = (CreateMTMVCommand) logicalPlan;
        command.getCreateMTMVInfo().analyze(connectContext);

        return command.getCreateMTMVInfo();
    }

    private void createMtmv(String sql) throws Exception {
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof CreateMTMVCommand);
        ((CreateMTMVCommand) logicalPlan).run(connectContext, null);
    }

    private MTMV getMtmv(String mvName) throws Exception {
        Database database = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        return (MTMV) database.getTableOrMetaException(mvName, org.apache.doris.catalog.TableIf.TableType.MATERIALIZED_VIEW);
    }

    @Test
    public void testMTMVRejectVarbinary() throws Exception {
        String mv = "CREATE MATERIALIZED VIEW mv_vb\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT X'AB' as vb;";

        LogicalPlan plan = new NereidsParser().parseSingle(mv);
        Assertions.assertTrue(plan instanceof CreateMTMVCommand);
        CreateMTMVCommand cmd = (CreateMTMVCommand) plan;

        org.apache.doris.nereids.exceptions.AnalysisException ex = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> cmd.getCreateMTMVInfo().analyze(connectContext));
        System.out.println(ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("MTMV do not support varbinary type"));
        Assertions.assertTrue(ex.getMessage().contains("vb"));
    }

    @Test
    public void testCreateMTMVWithIncrementRefreshMethod() throws Exception {
        String mv = "CREATE MATERIALIZED VIEW mtmv_increment\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT 1 AS k1;";

        LogicalPlan plan = new NereidsParser().parseSingle(mv);
        Assertions.assertTrue(plan instanceof CreateMTMVCommand);
        CreateMTMVCommand cmd = (CreateMTMVCommand) plan;

        Assertions.assertEquals(RefreshMethod.INCREMENTAL,
                cmd.getCreateMTMVInfo().getRefreshInfo().getRefreshMethod());
        Assertions.assertTrue(cmd.getCreateMTMVInfo().isEnableIvm());
    }

    @Test
    public void testCreateIncrementalMTMVPersistsIvmFlag() throws Exception {
        createTable("create table test.mtmv_increment_flag_base (k1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");
        createMtmv("CREATE MATERIALIZED VIEW mtmv_increment_flag\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1 FROM mtmv_increment_flag_base;");

        MTMV mtmv = getMtmv("mtmv_increment_flag");
        Assertions.assertTrue(mtmv.isIvm());
        Assertions.assertTrue(mtmv.getIvmInfo().isEnableIvm());
    }

    @Test
    public void testCreateNonIncrementalMTMVDefaultsIvmFlagFalse() throws Exception {
        createTable("create table test.mtmv_non_increment_flag_base (k1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");
        createMtmv("CREATE MATERIALIZED VIEW mtmv_non_increment_flag\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1 FROM mtmv_non_increment_flag_base;");

        MTMV mtmv = getMtmv("mtmv_non_increment_flag");
        Assertions.assertFalse(mtmv.isIvm());
        Assertions.assertFalse(mtmv.getIvmInfo().isEnableIvm());
    }

    @Test
    public void testCreateMTMVRewriteQuerySqlWithDefinedColumnsForScanPlan() throws Exception {
        createTable("create table test.mtmv_scan_base (id int, score int)\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_scan_alias"
                + " (mv_id, mv_score)\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT * FROM mtmv_scan_base;");

        Assertions.assertEquals(Column.IVM_ROW_ID_COL, createMTMVInfo.getColumns().get(0).getName());
        Assertions.assertFalse(createMTMVInfo.getColumns().get(0).isVisible());
        Assertions.assertEquals("mv_id", createMTMVInfo.getColumns().get(1).getName());
        Assertions.assertEquals("mv_score", createMTMVInfo.getColumns().get(2).getName());
        Assertions.assertTrue(createMTMVInfo.getQuerySql().contains("AS `mv_id`"));
        Assertions.assertTrue(createMTMVInfo.getQuerySql().contains("AS `mv_score`"));
    }

    @Test
    public void testCreateMTMVRewriteQuerySqlWithDefinedColumnsForProjectScanPlan() throws Exception {
        createTable("create table test.mtmv_project_scan_base (id int, score int)\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_project_scan_alias"
                + " (mv_inc_id, mv_score)\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id + 1, score FROM mtmv_project_scan_base;");

        Assertions.assertEquals(Column.IVM_ROW_ID_COL, createMTMVInfo.getColumns().get(0).getName());
        Assertions.assertFalse(createMTMVInfo.getColumns().get(0).isVisible());
        Assertions.assertEquals("mv_inc_id", createMTMVInfo.getColumns().get(1).getName());
        Assertions.assertEquals("mv_score", createMTMVInfo.getColumns().get(2).getName());
        Assertions.assertTrue(createMTMVInfo.getQuerySql().contains("AS `mv_inc_id`"));
        Assertions.assertTrue(createMTMVInfo.getQuerySql().contains("AS `mv_score`"));
    }

    @Test
    public void testCreateMTMVWithoutDefinedColumnsInjectsRowId() throws Exception {
        createTable("create table test.mtmv_no_cols_base (id int, score int)\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_no_cols"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, score FROM mtmv_no_cols_base;");

        Assertions.assertEquals(Column.IVM_ROW_ID_COL, createMTMVInfo.getColumns().get(0).getName());
        Assertions.assertFalse(createMTMVInfo.getColumns().get(0).isVisible());
        Assertions.assertEquals("id", createMTMVInfo.getColumns().get(1).getName());
        Assertions.assertEquals("score", createMTMVInfo.getColumns().get(2).getName());
    }

    @Test
    public void testCreateMTMVRewriteQuerySqlContainsAliases() throws Exception {
        createTable("create table test.mtmv_alias_base (id int, score int)\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_alias"
                + " (mv_id, mv_score)\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, score FROM mtmv_alias_base;");

        String querySql = createMTMVInfo.getQuerySql();
        Assertions.assertTrue(querySql.contains("AS `mv_id`"), "querySql should contain AS `mv_id`: " + querySql);
        Assertions.assertTrue(querySql.contains("AS `mv_score`"), "querySql should contain AS `mv_score`: " + querySql);
        Assertions.assertFalse(querySql.contains("AS `mv_" + Column.IVM_ROW_ID_COL + "`"),
                "querySql should not alias the row-id column: " + querySql);
    }

    @Test
    public void testCreateIvmMVColumnCountMismatchFails() throws Exception {
        createTable("create table test.mtmv_col_mismatch_base (id int, score int)\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1');");

        // user specifies 2 column names but query only selects 1 column — should fail
        org.apache.doris.nereids.exceptions.AnalysisException ex = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_col_mismatch"
                        + " (mv_id, mv_score)\n"
                        + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                        + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                        + " PROPERTIES ('replication_num' = '1')\n"
                        + " AS\n"
                        + " SELECT id FROM mtmv_col_mismatch_base;"));
        Assertions.assertTrue(ex.getMessage().contains("simpleColumnDefinitions size is not equal"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testVarBinaryModifyColumnRejected() throws Exception {
        createTable("create table test.vb_alt (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        org.apache.doris.nereids.trees.plans.logical.LogicalPlan plan =
                new org.apache.doris.nereids.parser.NereidsParser()
                        .parseSingle("alter table test.vb_alt modify column v1 VARBINARY");
        Assertions.assertTrue(
                plan instanceof org.apache.doris.nereids.trees.plans.commands.AlterTableCommand);
        org.apache.doris.nereids.trees.plans.commands.AlterTableCommand cmd2 =
                (org.apache.doris.nereids.trees.plans.commands.AlterTableCommand) plan;
        Assertions.assertThrows(Throwable.class, () -> cmd2.run(connectContext, null));
    }

    // ====== Aggregate IVM test cases ======

    @Test
    public void testCreateAggImmvWithMultipleAggFunctions() throws Exception {
        createTable("create table test.agg_multi_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW agg_multi_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, COUNT(*), SUM(v1) FROM agg_multi_base GROUP BY k1;");

        List<Column> cols = info.getColumns();

        // row_id at index 0, hidden
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, cols.get(0).getName());
        Assertions.assertFalse(cols.get(0).isVisible());

        // 3 visible user columns: k1, count(*), sum(v1)
        List<Column> visibleCols = cols.stream()
                .filter(Column::isVisible).collect(Collectors.toList());
        Assertions.assertEquals(3, visibleCols.size());

        // hidden trailing agg state columns:
        // __DORIS_IVM_AGG_COUNT_COL__ (group count)
        // __DORIS_IVM_AGG_1_COUNT_COL__ (SUM count hidden)
        // COUNT(*) has no hidden columns; SUM has no hidden SUM (visible stores it)
        List<Column> hiddenCols = cols.stream()
                .filter(c -> !c.isVisible()).collect(Collectors.toList());
        int expectedHiddenCols = 4 + (Config.enable_hidden_version_column_by_default ? 1 : 0);
        Assertions.assertEquals(expectedHiddenCols, hiddenCols.size()); // row_id + 2 agg states + delete_sign [+ version]
        List<String> hiddenNames = hiddenCols.stream()
                .map(Column::getName).collect(Collectors.toList());
        Assertions.assertTrue(hiddenNames.contains(Column.DELETE_SIGN));
        Assertions.assertTrue(hiddenNames.contains(Column.IVM_AGG_COUNT_COL));
        Assertions.assertFalse(hiddenNames.contains("__DORIS_IVM_AGG_0_COUNT_COL__"));
        Assertions.assertFalse(hiddenNames.contains("__DORIS_IVM_AGG_1_SUM_COL__"));
        Assertions.assertTrue(hiddenNames.contains("__DORIS_IVM_AGG_1_COUNT_COL__"));
        if (Config.enable_hidden_version_column_by_default) {
            Assertions.assertTrue(hiddenNames.contains(Column.VERSION_COL));
        }
    }

    @Test
    public void testCreateIvmPersistsAnalyzedHiddenColumnProperties() throws Exception {
        createTable("create table test.mtmv_ivm_properties_base (id int, score int)\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1');");

        createMtmv("create materialized view mtmv_ivm_properties\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS select * from test.mtmv_ivm_properties_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mtmv_ivm_properties");
        Assertions.assertEquals(Config.enable_skip_bitmap_column_by_default, mtmv.getEnableUniqueKeySkipBitmap());
        Assertions.assertEquals(Config.enable_skip_bitmap_column_by_default,
                mtmv.getBaseSchema(true).stream().anyMatch(col -> Column.SKIP_BITMAP_COL.equals(col.getName())));
    }

    @Test
    public void testCreateAggImmvWithHavingThrows() throws Exception {
        createTable("create table test.agg_having_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        // HAVING produces a Filter above Aggregate, which is rejected by IvmNormalizeMtmv
        org.apache.doris.nereids.exceptions.AnalysisException ex = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> getPartitionTableInfo(
                        "CREATE MATERIALIZED VIEW agg_having_mv\n"
                        + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                        + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                        + " PROPERTIES ('replication_num' = '1')\n"
                        + " AS\n"
                        + " SELECT k1, SUM(v1) FROM agg_having_base GROUP BY k1"
                        + " HAVING SUM(v1) > 10;"));
        Assertions.assertTrue(
                ex.getMessage().contains("IVM aggregate must be the top-level operator"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testCreateScalarAggImmv() throws Exception {
        createTable("create table test.scalar_agg_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW scalar_agg_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT COUNT(*), SUM(v1) FROM scalar_agg_base;");

        List<Column> cols = info.getColumns();

        // row_id at index 0, hidden
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, cols.get(0).getName());
        Assertions.assertFalse(cols.get(0).isVisible());

        // 2 visible: count(*), sum(v1) — no group keys for scalar agg
        List<Column> visibleCols = cols.stream()
                .filter(Column::isVisible).collect(Collectors.toList());
        Assertions.assertEquals(2, visibleCols.size());

        // hidden: row_id + IVM_AGG_COUNT_COL + AGG_1_COUNT + delete_sign [+ version]
        // COUNT(*) has no hidden columns; SUM has no hidden SUM (visible stores it)
        List<Column> hiddenCols = cols.stream()
                .filter(c -> !c.isVisible()).collect(Collectors.toList());
        int expectedHiddenCols = 4 + (Config.enable_hidden_version_column_by_default ? 1 : 0);
        Assertions.assertEquals(expectedHiddenCols, hiddenCols.size());
        List<String> hiddenNames = hiddenCols.stream()
                .map(Column::getName).collect(Collectors.toList());
        Assertions.assertTrue(hiddenNames.contains(Column.DELETE_SIGN));
        Assertions.assertTrue(hiddenNames.contains(Column.IVM_AGG_COUNT_COL));
        if (Config.enable_hidden_version_column_by_default) {
            Assertions.assertTrue(hiddenNames.contains(Column.VERSION_COL));
        }
    }

    @Test
    public void testCreateAggImmvWithAvg() throws Exception {
        createTable("create table test.agg_avg_base (k1 int, v1 decimal(10, 2))\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW agg_avg_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, AVG(v1) FROM agg_avg_base GROUP BY k1;");

        List<Column> cols = info.getColumns();

        // row_id hidden at index 0
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, cols.get(0).getName());
        Assertions.assertFalse(cols.get(0).isVisible());

        // 2 visible: k1, avg(v1)
        List<Column> visibleCols = cols.stream()
                .filter(Column::isVisible).collect(Collectors.toList());
        Assertions.assertEquals(2, visibleCols.size());

        // AVG decomposes to SUM + COUNT hidden states
        // hidden: row_id + IVM_AGG_COUNT_COL + AGG_0_SUM + AGG_0_COUNT = 4
        List<String> hiddenNames = cols.stream()
                .filter(c -> !c.isVisible())
                .map(Column::getName).collect(Collectors.toList());
        Assertions.assertTrue(hiddenNames.contains(Column.IVM_AGG_COUNT_COL));
        Assertions.assertTrue(hiddenNames.contains("__DORIS_IVM_AGG_0_SUM_COL__"));
        Assertions.assertTrue(hiddenNames.contains("__DORIS_IVM_AGG_0_COUNT_COL__"));
    }

    @Test
    public void testIvmMvRandomDistributionForcedToHashOnRowId() throws Exception {
        // IVM MV with RANDOM distribution should be overridden to HASH(__DORIS_IVM_ROW_ID_COL__)
        createTable("create table test.ivm_dist_random_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_random_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 3\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_random_base;");

        // Distribution should be forced to HASH
        Assertions.assertTrue(info.getDistribution().isHash(),
                "IVM MV distribution should be HASH, not RANDOM");
        // Distribution column should be __DORIS_IVM_ROW_ID_COL__
        Assertions.assertEquals(1, info.getDistribution().getCols().size());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
    }

    @Test
    public void testIvmMvHashDistributionForcedToRowId() throws Exception {
        // IVM MV with HASH(k1) distribution should also be overridden to HASH(__DORIS_IVM_ROW_ID_COL__)
        createTable("create table test.ivm_dist_hash_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_hash_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 4\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_hash_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(1, info.getDistribution().getCols().size());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
    }

    @Test
    public void testNonIvmMvRandomDistributionPreserved() throws Exception {
        // Non-IVM (AUTO refresh) MV with RANDOM distribution should remain RANDOM
        createTable("create table test.non_ivm_dist_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW non_ivm_dist_mv\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 3\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM non_ivm_dist_base;");

        // Distribution should remain RANDOM (not overridden)
        Assertions.assertFalse(info.getDistribution().isHash(),
                "Non-IVM MV distribution should remain RANDOM");
    }

    @Test
    public void testIvmMvBucketCountPreserved() throws Exception {
        // Verify that the user-specified bucket count is preserved after distribution override
        createTable("create table test.ivm_dist_bucket_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_bucket_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 7\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_bucket_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
        // Bucket count should be preserved from the user's specification
        Assertions.assertEquals(7, info.getDistribution().translateToCatalogStyle().getBuckets());
    }

    @Test
    public void testCreateAggImmvWithMinMax() throws Exception {
        createTable("create table test.agg_minmax_base (k1 int, v1 int, v2 bigint)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        // MIN/MAX are now supported for IVM; creation should succeed.
        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW agg_minmax_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, MIN(v1), MAX(v2) FROM agg_minmax_base GROUP BY k1;");
        Assertions.assertNotNull(info, "CreateMTMVInfo should not be null for MIN/MAX IVM");
        // IVM overrides distribution to HASH on row-id column
        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
    }
}
