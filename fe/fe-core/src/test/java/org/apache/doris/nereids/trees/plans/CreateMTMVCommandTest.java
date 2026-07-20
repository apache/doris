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
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mtmv.MTMVAlterOpType;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.FixedRangePartition;
import org.apache.doris.nereids.trees.plans.commands.info.InPartition;
import org.apache.doris.nereids.trees.plans.commands.info.LessThanPartition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateMTMVCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        Config.enable_table_stream = true;
    }

    @Override
    public void createTable(String sql) throws Exception {
        resetStatementContext(sql);
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        ((CreateTableCommand) plan).run(connectContext, null);
    }

    private void resetStatementContext(String sql) {
        connectContext.setStatementContext(new StatementContext(connectContext, new OriginStatement(sql, 0)));
    }

    private static List<String> keyNames(CreateMTMVInfo info) {
        return info.getColumns().stream()
                .filter(Column::isKey)
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    private static List<String> columnNames(CreateMTMVInfo info) {
        return info.getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toList());
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
        resetStatementContext(sql);
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof CreateMTMVCommand);
        CreateMTMVCommand command = (CreateMTMVCommand) logicalPlan;
        command.getCreateMTMVInfo().analyze(connectContext);

        return command.getCreateMTMVInfo();
    }

    private void createMtmv(String sql) throws Exception {
        resetStatementContext(sql);
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof CreateMTMVCommand);
        ((CreateMTMVCommand) logicalPlan).run(connectContext, null);
    }

    private void assertCreateMtmvFails(String sql, String expectedMessage) {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> createMtmv(sql));
        Assertions.assertTrue(ex.getMessage().contains(expectedMessage),
                "unexpected message: " + ex.getMessage());
    }

    private void createIvmMowTable(String tableName) throws Exception {
        createTable("create table test." + tableName + " (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
    }

    private void createIvmDupTable(String tableName) throws Exception {
        createTable("create table test." + tableName + " (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
    }

    private void alterMtmv(String sql) throws Exception {
        resetStatementContext(sql);
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof AlterMTMVCommand);
        ((AlterMTMVCommand) logicalPlan).run(connectContext, null);
    }

    private MTMV getMtmv(String mvName) throws Exception {
        Database database = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        return (MTMV) database.getTableOrMetaException(mvName, org.apache.doris.catalog.TableIf.TableType.MATERIALIZED_VIEW);
    }

    @Test
    public void testMTMVRejectVarbinary() throws Exception {
        String mv = "CREATE MATERIALIZED VIEW mv_vb\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
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
    public void testCreateMTMVWithIncrementalFallback() throws Exception {
        String mv = "CREATE MATERIALIZED VIEW mtmv_increment_fallback\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT 1 AS k1;";

        LogicalPlan plan = new NereidsParser().parseSingle(mv);
        Assertions.assertTrue(plan instanceof CreateMTMVCommand);
        CreateMTMVInfo info = ((CreateMTMVCommand) plan).getCreateMTMVInfo();

        Assertions.assertEquals(RefreshMethod.INCREMENTAL, info.getRefreshInfo().getRefreshMethod());
        Assertions.assertTrue(info.getRefreshInfo().allowFallback());
    }

    @Test
    public void testCreateMTMVWithCompleteFallbackRejected() {
        String mv = "CREATE MATERIALIZED VIEW mtmv_complete_fallback\n"
                + " BUILD DEFERRED REFRESH COMPLETE FALLBACK ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT 1 AS k1;";

        org.apache.doris.nereids.exceptions.AnalysisException ex = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> getPartitionTableInfo(mv));

        Assertions.assertTrue(ex.getMessage().contains("COMPLETE"));
        Assertions.assertTrue(ex.getMessage().contains("FALLBACK"));
    }

    @Test
    public void testCreatePartitionsRefreshRequiresPartitionBy() {
        String mv = "CREATE MATERIALIZED VIEW mtmv_partitions_without_partition_by\n"
                + " BUILD DEFERRED REFRESH PARTITIONS ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT 1 AS k1;";

        org.apache.doris.nereids.exceptions.AnalysisException ex = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> getPartitionTableInfo(mv));

        Assertions.assertTrue(ex.getMessage().contains("REFRESH PARTITIONS requires PARTITION BY"));
    }

    @Test
    public void testCreateIncrementalMTMVPersistsIvmFlag() throws Exception {
        createTable("create table test.mtmv_increment_flag_base (k1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        createMtmv("CREATE MATERIALIZED VIEW mtmv_increment_flag\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1 FROM mtmv_increment_flag_base;");

        MTMV mtmv = getMtmv("mtmv_increment_flag");
        Assertions.assertTrue(mtmv.isIvm());
        Assertions.assertTrue(mtmv.getIvmInfo().isEnableIvm());
    }

    @Test
    public void testCreateAutoMTMVPersistsIvmFlagWhenCapable() throws Exception {
        createTable("create table test.mtmv_auto_increment_flag_base (k1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        createMtmv("CREATE MATERIALIZED VIEW mtmv_auto_increment_flag\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1 FROM mtmv_auto_increment_flag_base;");

        MTMV mtmv = getMtmv("mtmv_auto_increment_flag");
        Assertions.assertTrue(mtmv.isIvm());
        Assertions.assertTrue(mtmv.getIvmInfo().isEnableIvm());
    }

    @Test
    public void testCreateAutoMTMVFallsBackToNonIvmOnIvmException() throws Exception {
        createTable("create table test.mtmv_auto_fallback_agg_base (k1 int, v1 int SUM)\n"
                + "aggregate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo info = getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_auto_fallback_agg\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1 FROM mtmv_auto_fallback_agg_base;");

        Assertions.assertFalse(info.isEnableIvm());
    }

    @Test
    public void testCreateAutoMTMVFallsBackToNonIvmOnPlainAnalysisException() throws Exception {
        createTable("create table test.mtmv_auto_fallback_plain_base (k1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        String sql = "CREATE MATERIALIZED VIEW mtmv_auto_fallback_plain\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1 FROM mtmv_auto_fallback_plain_base;";
        resetStatementContext(sql);
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateMTMVCommand);
        CreateMTMVInfo info = Mockito.spy(((CreateMTMVCommand) plan).getCreateMTMVInfo());
        List<LogicalPlan> analyzeQueries = new ArrayList<>();
        List<StatementContext> statementContexts = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            analyzeQueries.add(getLogicalQuery(info));
            statementContexts.add(connectContext.getStatementContext());
            if (info.isEnableIvm()) {
                throw new AnalysisException("mock plain analysis failure during IVM probe");
            }
            return invocation.callRealMethod();
        }).when(info).analyzeQuery(Mockito.any());

        info.analyze(connectContext);

        Assertions.assertFalse(info.isEnableIvm());
        Assertions.assertEquals(2, analyzeQueries.size());
        Assertions.assertEquals(2, statementContexts.size());
        Assertions.assertNotSame(analyzeQueries.get(0), analyzeQueries.get(1));
        Assertions.assertNotSame(statementContexts.get(0), statementContexts.get(1));
    }

    private LogicalPlan getLogicalQuery(CreateMTMVInfo info) throws Exception {
        Field logicalQueryField = CreateMTMVInfo.class.getDeclaredField("logicalQuery");
        logicalQueryField.setAccessible(true);
        return (LogicalPlan) logicalQueryField.get(info);
    }

    @Test
    public void testCreateAutoMTMVStillFailsWhenFallbackAnalyzeFails() throws Exception {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_auto_bad_sql\n"
                        + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                        + " PROPERTIES ('replication_num' = '1')\n"
                        + " AS SELECT missing_col FROM missing_table;"));

        Assertions.assertFalse(ex.getMessage().contains("fallback"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testCreateMTMVRewriteQuerySqlWithDefinedColumnsForScanPlan() throws Exception {
        createTable("create table test.mtmv_scan_base (id int, score int)\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_scan_alias"
                + " (mv_id, mv_score)\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_project_scan_alias"
                + " (mv_inc_id, mv_score)\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_no_cols"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo createMTMVInfo = getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_alias"
                + " (mv_id, mv_score)\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        // user specifies 2 column names but query only selects 1 column — should fail
        org.apache.doris.nereids.exceptions.AnalysisException ex = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> getPartitionTableInfo("CREATE MATERIALIZED VIEW mtmv_col_mismatch"
                        + " (mv_id, mv_score)\n"
                        + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW agg_multi_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        createMtmv("create materialized view mtmv_ivm_properties\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        // HAVING produces a Filter above Aggregate, which is rejected by IvmNormalizeMTMV
        org.apache.doris.nereids.exceptions.AnalysisException ex = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> getPartitionTableInfo(
                        "CREATE MATERIALIZED VIEW agg_having_mv\n"
                        + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW scalar_agg_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW agg_avg_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
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
        createTable("create table test.ivm_dist_random_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_random_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 3\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_random_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(1, info.getDistribution().getCols().size());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
        Assertions.assertEquals(3, info.getDistribution().translateToCatalogStyle().getBuckets());
        Assertions.assertFalse(info.getDistribution().isAutoBucket());
        Assertions.assertEquals(Arrays.asList(Column.IVM_ROW_ID_COL), keyNames(info));
    }

    @Test
    public void testIvmMvRandomDistributionWithoutBucketsUsesAutoBucket() throws Exception {
        createTable("create table test.ivm_dist_random_auto_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_random_auto_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_random_auto_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
        Assertions.assertTrue(info.getDistribution().isAutoBucket());
        Assertions.assertEquals("true", info.getProperties().get(PropertyAnalyzer.PROPERTIES_AUTO_BUCKET));
        Assertions.assertEquals(Arrays.asList(Column.IVM_ROW_ID_COL), keyNames(info));
    }

    @Test
    public void testIvmExplicitKeyRandomDistributionForcedToHashOnRowId() throws Exception {
        createTable("create table test.ivm_explicit_key_random_dist_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_explicit_key_random_dist_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(k1)\n"
                + " DISTRIBUTED BY RANDOM\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_explicit_key_random_dist_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
        Assertions.assertTrue(info.getDistribution().isAutoBucket());
        Assertions.assertEquals(Arrays.asList("k1", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("k1", Column.IVM_ROW_ID_COL, "v1"),
                columnNames(info).subList(0, 3));
    }

    @Test
    public void testIvmExplicitKeyDefaultDistributionForcedToHashOnRowId() throws Exception {
        createTable("create table test.ivm_explicit_key_default_dist_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_explicit_key_default_dist_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(k1)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_explicit_key_default_dist_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
        Assertions.assertTrue(info.getDistribution().isAutoBucket());
        Assertions.assertEquals(Arrays.asList("k1", Column.IVM_ROW_ID_COL), keyNames(info));
    }

    @Test
    public void testIvmMvDefaultDistributionForcedToHashOnRowId() throws Exception {
        createTable("create table test.ivm_dist_default_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_default_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_default_base;");

        // IVM MOW dedup is tablet-local, so omitted distribution becomes HASH(row-id).
        Assertions.assertTrue(info.getDistribution().isHash(),
                "IVM MV distribution should be HASH");
        Assertions.assertEquals(1, info.getDistribution().getCols().size());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
        Assertions.assertTrue(info.getDistribution().isAutoBucket());
        Assertions.assertEquals("true", info.getProperties().get(PropertyAnalyzer.PROPERTIES_AUTO_BUCKET));
        Assertions.assertEquals(Arrays.asList(Column.IVM_ROW_ID_COL), keyNames(info));
    }

    @Test
    public void testIvmMvHashDistributionPreserved() throws Exception {
        createTable("create table test.ivm_dist_hash_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_hash_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(k1)\n"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 4\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_hash_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(1, info.getDistribution().getCols().size());
        Assertions.assertEquals("k1", info.getDistribution().getCols().get(0));
        Assertions.assertEquals(Arrays.asList("k1", Column.IVM_ROW_ID_COL), keyNames(info));
    }

    @Test
    public void testIvmMvHashDistributionWithoutUserKeyGeneratesKey() throws Exception {
        createTable("create table test.ivm_dist_hash_no_key_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_hash_no_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 4\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_hash_no_key_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals("k1", info.getDistribution().getCols().get(0));
        Assertions.assertEquals(4, info.getDistribution().translateToCatalogStyle().getBuckets());
        Assertions.assertEquals(Arrays.asList("k1", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("k1", Column.IVM_ROW_ID_COL, "v1"),
                columnNames(info).subList(0, 3));
    }

    @Test
    public void testIvmRejectsExplicitKeyNotSelectPrefix() throws Exception {
        createTable("create table test.ivm_explicit_key_not_prefix_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_explicit_key_not_prefix_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(v1)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_explicit_key_not_prefix_base;"));

        Assertions.assertTrue(ex.getMessage()
                        .contains("IVM key columns [v1] must be an ordered prefix of SELECT output [k1, v1]"),
                "unexpected message: " + ex.getMessage());
        Assertions.assertFalse(ex.getMessage().contains(Column.IVM_ROW_ID_COL),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmRejectsGeneratedHashDistributionKeyNotSelectPrefix() throws Exception {
        createTable("create table test.ivm_generated_hash_not_prefix_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_generated_hash_not_prefix_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY HASH(v1) BUCKETS 4\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_generated_hash_not_prefix_base;"));

        Assertions.assertTrue(ex.getMessage()
                        .contains("IVM generated key columns [v1] "
                                + "must be an ordered prefix of SELECT output [k1, v1]"),
                "unexpected message: " + ex.getMessage());
        Assertions.assertFalse(ex.getMessage().contains(Column.IVM_ROW_ID_COL),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmMvMultiHashDistributionWithoutUserKeyGeneratesKeys() throws Exception {
        createTable("create table test.ivm_dist_multi_hash_no_key_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_multi_hash_no_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY HASH(k1, v1) BUCKETS 4\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_multi_hash_no_key_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Arrays.asList("k1", "v1"), info.getDistribution().getCols());
        Assertions.assertEquals(4, info.getDistribution().translateToCatalogStyle().getBuckets());
        Assertions.assertEquals(Arrays.asList("k1", "v1", Column.IVM_ROW_ID_COL), keyNames(info));
    }

    @Test
    public void testIvmMvRejectsInvalidHashDistributionColumnBeforeRewrite() throws Exception {
        createTable("create table test.ivm_dist_invalid_hash_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_invalid_hash_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY HASH(no_such_col) BUCKETS 8\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_invalid_hash_base;"));

        Assertions.assertTrue(ex.getMessage().contains("Distribution column(no_such_col) doesn't exist"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmMvRejectsHashDistributionNonKeyColumnBeforeRewrite() throws Exception {
        createTable("create table test.ivm_dist_non_key_hash_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_non_key_hash_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(k1)\n"
                + " DISTRIBUTED BY HASH(v1) BUCKETS 8\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_non_key_hash_base;"));

        Assertions.assertTrue(ex.getMessage().contains("Distribution column[v1] is not key column"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmRejectsExplicitKeyHashDistributionOnPartitionNonKey() throws Exception {
        createTable("create table test.ivm_partition_dist_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "partition by range(dt) (partition p202601 values [('2026-01-01'), ('2026-02-01')))\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_partition_dist_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(id)\n"
                + " PARTITION BY(dt)\n"
                + " DISTRIBUTED BY HASH(dt) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, dt, v1 FROM ivm_partition_dist_base;"));

        Assertions.assertTrue(ex.getMessage().contains("Distribution column[dt] is not key column"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmExplicitKeyPartitionAndHashDistributionValid() throws Exception {
        createTable("create table test.ivm_partition_explicit_key_hash_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "partition by range(dt) (partition p202601 values [('2026-01-01'), ('2026-02-01')))\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_partition_explicit_key_hash_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(id, dt)\n"
                + " PARTITION BY(dt)\n"
                + " DISTRIBUTED BY HASH(id) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, dt, v1 FROM ivm_partition_explicit_key_hash_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals("id", info.getDistribution().getCols().get(0));
        Assertions.assertEquals(2, info.getDistribution().translateToCatalogStyle().getBuckets());
        Assertions.assertEquals(Arrays.asList("id", "dt", Column.IVM_ROW_ID_COL), keyNames(info));
    }

    @Test
    public void testIvmAggregateExplicitKeyMayUseGroupKeySubset() throws Exception {
        createTable("create table test.ivm_agg_subset_key_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_agg_subset_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(id)\n"
                + " DISTRIBUTED BY HASH(id) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, dt, SUM(v1) AS total FROM ivm_agg_subset_key_base GROUP BY dt, id;");

        Assertions.assertEquals(Arrays.asList("id", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("id", Column.IVM_ROW_ID_COL, "dt", "total"),
                columnNames(info).subList(0, 4));
    }

    @Test
    public void testIvmAggregateExplicitKeyMayUseGroupKeyAlias() throws Exception {
        createTable("create table test.ivm_agg_alias_key_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_agg_alias_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(alias_id)\n"
                + " DISTRIBUTED BY HASH(alias_id) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id AS alias_id, dt, SUM(v1) AS total FROM ivm_agg_alias_key_base GROUP BY dt, id;");

        Assertions.assertEquals(Arrays.asList("alias_id", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("alias_id", Column.IVM_ROW_ID_COL, "dt", "total"),
                columnNames(info).subList(0, 4));
    }

    @Test
    public void testIvmAggregateGeneratedKeyMayUseGroupKeyAliasFromHashDistribution() throws Exception {
        createTable("create table test.ivm_agg_alias_hash_key_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_agg_alias_hash_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY HASH(alias_id) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id AS alias_id, dt, SUM(v1) AS total "
                + "FROM ivm_agg_alias_hash_key_base GROUP BY dt, id;");

        Assertions.assertEquals(Arrays.asList("alias_id", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("alias_id", Column.IVM_ROW_ID_COL, "dt", "total"),
                columnNames(info).subList(0, 4));
    }

    @Test
    public void testIvmAggregateExplicitKeyMayUseGroupKeyExpressionAlias() throws Exception {
        createTable("create table test.ivm_agg_expr_alias_key_base (id int not null, v1 int)\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_agg_expr_alias_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(id_plus)\n"
                + " DISTRIBUTED BY HASH(id_plus) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id + 1 AS id_plus, SUM(v1) AS total "
                + "FROM ivm_agg_expr_alias_key_base GROUP BY id + 1;");

        Assertions.assertEquals(Arrays.asList("id_plus", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("id_plus", Column.IVM_ROW_ID_COL, "total"),
                columnNames(info).subList(0, 3));
    }

    @Test
    public void testIvmAggregateExplicitKeyMayUseExpressionFromGroupKeys() throws Exception {
        createTable("create table test.ivm_agg_group_keys_expr_key_base (a int, b int, v1 int)\n"
                + "duplicate key(a, b)\n"
                + "distributed by hash(a) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_agg_group_keys_expr_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(key_ab)\n"
                + " DISTRIBUTED BY HASH(key_ab) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT a + b AS key_ab, SUM(v1) AS total "
                + "FROM ivm_agg_group_keys_expr_key_base GROUP BY a, b;");

        Assertions.assertEquals(Arrays.asList("key_ab", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("key_ab", Column.IVM_ROW_ID_COL, "total"),
                columnNames(info).subList(0, 3));
    }

    @Test
    public void testIvmAggregateExplicitKeyMayUseRenamedGroupKeyColumn() throws Exception {
        createTable("create table test.ivm_agg_renamed_key_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_agg_renamed_key_mv (`renamed_id`, `renamed_dt`, `total`)\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(renamed_id)\n"
                + " DISTRIBUTED BY HASH(renamed_id) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, dt, SUM(v1) FROM ivm_agg_renamed_key_base GROUP BY dt, id;");

        Assertions.assertEquals(Arrays.asList("renamed_id", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("renamed_id", Column.IVM_ROW_ID_COL, "renamed_dt", "total"),
                columnNames(info).subList(0, 4));
    }

    @Test
    public void testIvmRejectsGeneratedAggregateResultKeyFromHashDistribution() throws Exception {
        createTable("create table test.ivm_agg_hash_result_key_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_agg_hash_result_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY HASH(total) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT dt, id, SUM(v1) AS total FROM ivm_agg_hash_result_key_base GROUP BY dt, id;"));

        Assertions.assertTrue(ex.getMessage().contains("aggregate result column"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmRejectsDerivedAggregateResultKey() throws Exception {
        createTable("create table test.ivm_agg_derived_result_key_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_agg_derived_result_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(total)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT dt, id, SUM(v1) + 1000 AS total "
                + "FROM ivm_agg_derived_result_key_base GROUP BY dt, id;"));

        Assertions.assertTrue(ex.getMessage().contains("aggregate result column"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmPartitionColumnAddedForGeneratedKeyBeforeHashDistributionValidation() throws Exception {
        createTable("create table test.ivm_partition_auto_key_dist_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "partition by range(dt) (partition p202601 values [('2026-01-01'), ('2026-02-01')))\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_partition_auto_key_dist_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PARTITION BY(dt)\n"
                + " DISTRIBUTED BY HASH(id) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, dt, v1 FROM ivm_partition_auto_key_dist_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals("id", info.getDistribution().getCols().get(0));
        Assertions.assertEquals(Arrays.asList("id", "dt", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("id", "dt", Column.IVM_ROW_ID_COL, "v1"),
                columnNames(info).subList(0, 4));
    }

    @Test
    public void testIvmPartitionWithoutUserKeyUsesPartitionAndRowIdKeys() throws Exception {
        createTable("create table test.ivm_partition_only_auto_key_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "partition by range(dt) (partition p202601 values [('2026-01-01'), ('2026-02-01')))\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_partition_only_auto_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PARTITION BY(dt)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT dt, id, v1 FROM ivm_partition_only_auto_key_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
        Assertions.assertTrue(info.getDistribution().isAutoBucket());
        Assertions.assertEquals(Arrays.asList("dt", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("dt", Column.IVM_ROW_ID_COL, "id", "v1"),
                columnNames(info).subList(0, 4));
    }

    @Test
    public void testIvmRejectsGeneratedPartitionKeyNotSelectPrefix() throws Exception {
        createTable("create table test.ivm_partition_not_prefix_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "partition by range(dt) (partition p202601 values [('2026-01-01'), ('2026-02-01')))\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_partition_not_prefix_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PARTITION BY(dt)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, dt, v1 FROM ivm_partition_not_prefix_base;"));

        Assertions.assertTrue(ex.getMessage()
                        .contains("IVM generated key columns [dt] "
                                + "must be an ordered prefix of SELECT output [id, dt, v1]"),
                "unexpected message: " + ex.getMessage());
        Assertions.assertFalse(ex.getMessage().contains(Column.IVM_ROW_ID_COL),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmPartitionWithoutUserKeyRandomDistributionUsesPartitionAndRowIdKeys() throws Exception {
        createTable("create table test.ivm_partition_random_auto_key_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "partition by range(dt) (partition p202601 values [('2026-01-01'), ('2026-02-01')))\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_partition_random_auto_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PARTITION BY(dt)\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 3\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT dt, id, v1 FROM ivm_partition_random_auto_key_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
        Assertions.assertEquals(3, info.getDistribution().translateToCatalogStyle().getBuckets());
        Assertions.assertFalse(info.getDistribution().isAutoBucket());
        Assertions.assertEquals(Arrays.asList("dt", Column.IVM_ROW_ID_COL), keyNames(info));
        Assertions.assertEquals(Arrays.asList("dt", Column.IVM_ROW_ID_COL, "id", "v1"),
                columnNames(info).subList(0, 4));
    }

    @Test
    public void testIvmExplicitKeyPartitionRandomDistributionValid() throws Exception {
        createTable("create table test.ivm_partition_explicit_key_random_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "partition by range(dt) (partition p202601 values [('2026-01-01'), ('2026-02-01')))\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_partition_explicit_key_random_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(id, dt)\n"
                + " PARTITION BY(dt)\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 3\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, dt, v1 FROM ivm_partition_explicit_key_random_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
        Assertions.assertEquals(3, info.getDistribution().translateToCatalogStyle().getBuckets());
        Assertions.assertFalse(info.getDistribution().isAutoBucket());
        Assertions.assertEquals(Arrays.asList("id", "dt", Column.IVM_ROW_ID_COL), keyNames(info));
    }

    @Test
    public void testIvmRejectsExplicitKeyPartitionNonKey() throws Exception {
        createTable("create table test.ivm_partition_explicit_key_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "partition by range(dt) (partition p202601 values [('2026-01-01'), ('2026-02-01')))\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        Exception ex = Assertions.assertThrows(Exception.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_partition_explicit_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(id)\n"
                + " PARTITION BY(dt)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, dt, v1 FROM ivm_partition_explicit_key_base;"));

        Assertions.assertTrue(ex.getMessage().contains("Merge-on-Write table's partition column must be KEY column"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmRejectsExpressionPartition() throws Exception {
        createTable("create table test.ivm_expr_partition_base (id int, dt date, v1 int)\n"
                + "duplicate key(id, dt)\n"
                + "partition by range(dt) (partition p202601 values [('2026-01-01'), ('2026-02-01')))\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_expr_partition_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(id)\n"
                + " PARTITION BY(date_trunc(dt, 'month'))\n"
                + " DISTRIBUTED BY HASH(id) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT id, dt, v1 FROM ivm_expr_partition_base;"));

        Assertions.assertTrue(ex.getMessage().contains("only supports column partition"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testIvmRejectsUserWrittenHiddenRowId() throws Exception {
        createTable("create table test.ivm_hidden_rowid_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException keyEx = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_hidden_rowid_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(`" + Column.IVM_ROW_ID_COL + "`)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_hidden_rowid_base;"));
        Assertions.assertTrue(keyEx.getMessage().contains("does not allow specifying the hidden row-id column"),
                "unexpected message: " + keyEx.getMessage());

        AnalysisException distEx = Assertions.assertThrows(AnalysisException.class, () -> getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_hidden_rowid_dist_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY HASH(`" + Column.IVM_ROW_ID_COL + "`) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_hidden_rowid_base;"));
        Assertions.assertTrue(distEx.getMessage().contains("IVM hidden column can not be distribution column"),
                "unexpected message: " + distEx.getMessage());
    }

    @Test
    public void testNonIvmMvRandomDistributionPreserved() throws Exception {
        createTable("create table test.non_ivm_dist_base (k1 int, v1 int SUM)\n"
                + "aggregate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW non_ivm_dist_mv\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 3\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1 FROM non_ivm_dist_base;");

        Assertions.assertFalse(info.isEnableIvm());
        Assertions.assertFalse(info.getDistribution().isHash(),
                "Non-IVM MV distribution should remain RANDOM");
        Assertions.assertFalse(info.getProperties().containsKey(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE));
        Assertions.assertTrue(info.getColumns().stream()
                .noneMatch(column -> Column.IVM_ROW_ID_COL.equals(column.getName())));
    }

    @Test
    public void testCreateAutoMTMVWithUserSpecifiedKeyUsesIvm() throws Exception {
        createTable("create table test.auto_key_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW auto_key_mv\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " KEY(k1)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM auto_key_base;");

        Assertions.assertTrue(info.isEnableIvm());
        Assertions.assertEquals(Arrays.asList("k1", Column.IVM_ROW_ID_COL), keyNames(info));
    }

    @Test
    public void testIvmMvBucketCountPreserved() throws Exception {
        createTable("create table test.ivm_dist_bucket_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_bucket_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(k1)\n"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 7\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_bucket_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals("k1", info.getDistribution().getCols().get(0));
        // Bucket count should be preserved from the user's specification
        Assertions.assertEquals(7, info.getDistribution().translateToCatalogStyle().getBuckets());
    }

    @Test
    public void testIvmMvHashDistributionAutoBucketPreserved() throws Exception {
        createTable("create table test.ivm_dist_auto_bucket_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW ivm_dist_auto_bucket_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(k1)\n"
                + " DISTRIBUTED BY HASH(k1) BUCKETS AUTO\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, v1 FROM ivm_dist_auto_bucket_base;");

        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertTrue(info.getDistribution().isAutoBucket());
        Assertions.assertEquals("k1", info.getDistribution().getCols().get(0));
        Assertions.assertEquals("true", info.getProperties().get(PropertyAnalyzer.PROPERTIES_AUTO_BUCKET));
    }

    @Test
    public void testCreateAggImmvWithMinMax() throws Exception {
        createTable("create table test.agg_minmax_base (k1 int, v1 int, v2 bigint)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        // MIN/MAX are now supported for IVM; creation should succeed.
        CreateMTMVInfo info = getPartitionTableInfo(
                "CREATE MATERIALIZED VIEW agg_minmax_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS\n"
                + " SELECT k1, MIN(v1), MAX(v2) FROM agg_minmax_base GROUP BY k1;");
        Assertions.assertNotNull(info, "CreateMTMVInfo should not be null for MIN/MAX IVM");
        // IVM overrides distribution to HASH on row-id column
        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, info.getDistribution().getCols().get(0));
    }

    // --- P0-2: INCREMENTAL MV base table model validation tests ---
    // CREATE first validates base table models explicitly, then reruns analyzeQuery with IVM normalize enabled.

    @Test
    public void testCreateIncrementalMVAcceptsDupKeysBaseTable() throws Exception {
        createTable("create table test.ivm_dup_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_dup_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM ivm_dup_base;");
        MTMV mtmv = getMtmv("ivm_dup_mv");
        Assertions.assertTrue(mtmv.isIvm());
    }

    @Test
    public void testCreateIncrementalMVAnalyzeSetsNormalizeRewriteContext() throws Exception {
        createTable("create table test.ivm_context_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        CreateMTMVInfo info = getPartitionTableInfo("CREATE MATERIALIZED VIEW ivm_context_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM ivm_context_base;");

        Assertions.assertTrue(info.isEnableIvm());
        Assertions.assertEquals(IvmRewriteContext.Mode.NORMALIZE,
                connectContext.getStatementContext().getIvmRewriteContext().orElseThrow().getMode());
    }

    @Test
    public void testCreateIncrementalMVAcceptsMOWBaseTable() throws Exception {
        createTable("create table test.ivm_mow_base (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true', "
                + "'enable_unique_key_merge_on_write' = 'true');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_mow_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM ivm_mow_base;");
        MTMV mtmv = getMtmv("ivm_mow_mv");
        Assertions.assertTrue(mtmv.isIvm());
    }

    @Test
    public void testCreateIncrementalMVAcceptsUserSpecifiedKeyColumns() throws Exception {
        createTable("create table test.ivm_explicit_key_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        CreateMTMVInfo info = getPartitionTableInfo("CREATE MATERIALIZED VIEW ivm_explicit_unique_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " KEY(k1)\n"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM ivm_explicit_key_base;");

        Assertions.assertTrue(info.isEnableIvm());
        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals("k1", info.getDistribution().getCols().get(0));
    }

    @Test
    public void testCreateIncrementalMVAcceptsUserSpecifiedDuplicateKeyColumns() throws Exception {
        createTable("create table test.ivm_explicit_dup_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        CreateMTMVInfo info = getPartitionTableInfo("CREATE MATERIALIZED VIEW ivm_explicit_dup_key_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DUPLICATE KEY(k1)\n"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM ivm_explicit_dup_base;");

        Assertions.assertTrue(info.isEnableIvm());
        Assertions.assertTrue(info.getDistribution().isHash());
        Assertions.assertEquals("k1", info.getDistribution().getCols().get(0));
    }

    @Test
    public void testCreateIncrementalMVRejectsAggKeysBaseTable() throws Exception {
        createTable("create table test.ivm_agg_base (k1 int, v1 int SUM)\n"
                + "aggregate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true');");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> getPartitionTableInfo("CREATE MATERIALIZED VIEW ivm_agg_mv\n"
                        + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                        + " PROPERTIES ('replication_num' = '1')\n"
                        + " AS SELECT k1 FROM ivm_agg_base;"));
        Assertions.assertTrue(ex.getMessage().contains("requires base tables to be"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testCreateIncrementalMVAllowsAggKeysInExcludedTriggerTables() throws Exception {
        createTable("create table test.ivm_excluded_agg_base (k1 int, v1 int SUM)\n"
                + "aggregate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_excluded_agg_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1', 'excluded_trigger_tables' = 'ivm_excluded_agg_base')\n"
                + " AS SELECT k1 FROM ivm_excluded_agg_base;");
        MTMV mtmv = getMtmv("ivm_excluded_agg_mv");
        Assertions.assertTrue(mtmv.isIvm());
    }

    @Test
    public void testCreateIncrementalMVRejectsUniqueKeyWithoutMOW() throws Exception {
        createTable("create table test.ivm_nomow_base (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', "
                + "'enable_unique_key_merge_on_write' = 'false');");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> getPartitionTableInfo("CREATE MATERIALIZED VIEW ivm_nomow_mv\n"
                        + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                        + " PROPERTIES ('replication_num' = '1')\n"
                        + " AS SELECT k1, v1 FROM ivm_nomow_base;"));
        Assertions.assertTrue(ex.getMessage().contains("enable Merge-On-Write"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testCreateIncrementalMVAllowsBuildDeferred() throws Exception {
        createTable("create table test.ivm_deferred_base (k1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_deferred_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1 FROM ivm_deferred_base;");
        MTMV mtmv = getMtmv("ivm_deferred_mv");
        Assertions.assertTrue(mtmv.isIvm());
    }

    @Test
    public void testCreateIncrementalMVAllowsPartitionByWhenSupported() throws Exception {
        createTable("CREATE TABLE test.ivm_partition_base (\n"
                + " `k1` INT NOT NULL,\n"
                + " `dt` DATE NOT NULL,\n"
                + " `v1` INT\n"
                + " ) ENGINE=OLAP\n"
                + " DUPLICATE KEY(`k1`, `dt`)\n"
                + " PARTITION BY RANGE(`dt`)\n"
                + " (\n"
                + " PARTITION `p202401` VALUES [(\"2024-01-01\"), (\"2024-02-01\")),\n"
                + " PARTITION `p202402` VALUES [(\"2024-02-01\"), (\"2024-03-01\"))\n"
                + " )\n"
                + " DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo("CREATE MATERIALIZED VIEW ivm_partition_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PARTITION BY(`dt`)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT dt, k1, v1 FROM ivm_partition_base;");

        Assertions.assertNotEquals(PartitionTableInfo.EMPTY, info.getPartitionTableInfo());
        Assertions.assertEquals(2, info.getPartitionTableInfo().getPartitionDefs().size());
    }

    @Test
    public void testCreateIncrementalMVReusesMowPartitionKeyValidation() throws Exception {
        // Force the analyzed partition column into a value-column shape, then
        // invoke partition validation directly to verify IVM reuses the ordinary
        // MOW rule that partition columns must be key columns.
        createTable("CREATE TABLE test.ivm_partition_mow_validate_base (\n"
                + " `k1` INT NOT NULL,\n"
                + " `dt` DATE NOT NULL,\n"
                + " `v1` INT\n"
                + " ) ENGINE=OLAP\n"
                + " DUPLICATE KEY(`k1`, `dt`)\n"
                + " PARTITION BY RANGE(`dt`)\n"
                + " (\n"
                + " PARTITION `p202401` VALUES [(\"2024-01-01\"), (\"2024-02-01\"))\n"
                + " )\n"
                + " DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + " PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        CreateMTMVInfo info = getPartitionTableInfo("CREATE MATERIALIZED VIEW ivm_partition_mow_validate_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PARTITION BY(`dt`)\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT dt, k1, v1 FROM ivm_partition_mow_validate_base;");

        ColumnDefinition partitionColumn = info.getColumnDefinitions().stream()
                .filter(column -> column.getName().equalsIgnoreCase("dt"))
                .findFirst()
                .orElseThrow();
        partitionColumn.setIsKey(false);
        partitionColumn.setAggType(AggregateType.NONE);

        Exception ex = Assertions.assertThrows(Exception.class, () -> {
            java.lang.reflect.Method method = CreateMTMVInfo.class.getDeclaredMethod(
                    "validatePartitionInfo", org.apache.doris.qe.ConnectContext.class);
            method.setAccessible(true);
            method.invoke(info, connectContext);
        });
        Assertions.assertTrue(ex.getCause().getMessage()
                        .contains("Merge-on-Write table's partition column must be KEY column"),
                "unexpected message: " + ex.getCause().getMessage());
    }

    @Test
    public void testCreateIncrementalMVRejectsUnsupportedPartitionIncremental() throws Exception {
        createTable("create table test.ivm_partition_unsupported_base (\n"
                + " k1 int,\n"
                + " v1 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> getPartitionTableInfo("CREATE MATERIALIZED VIEW ivm_partition_unsupported_mv\n"
                        + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                        + " PARTITION BY(`k1`)\n"
                        + " PROPERTIES ('replication_num' = '1')\n"
                        + " AS SELECT k1, v1 FROM ivm_partition_unsupported_base;"));
        Assertions.assertTrue(ex.getMessage().contains("suitable"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testCreateIncrementalMVAllowsFilterAboveLeftOuterJoin() throws Exception {
        createTable("create table test.ivm_outer_filter_left (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createTable("create table test.ivm_outer_filter_right (k1 int, v2 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");

        CreateMTMVInfo info = getPartitionTableInfo("CREATE MATERIALIZED VIEW ivm_outer_filter_mv\n"
                        + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                        + " PROPERTIES ('replication_num' = '1')\n"
                        + " AS SELECT ivm_outer_filter_left.k1, ivm_outer_filter_left.v1,"
                        + " ivm_outer_filter_right.v2\n"
                        + " FROM ivm_outer_filter_left\n"
                        + " LEFT OUTER JOIN ivm_outer_filter_right"
                        + " ON ivm_outer_filter_left.k1 = ivm_outer_filter_right.k1\n"
                        + " WHERE ivm_outer_filter_left.v1 > 0;");
        Assertions.assertEquals(RefreshMethod.INCREMENTAL,
                info.getRefreshInfo().getRefreshMethod());
        Assertions.assertTrue(info.isEnableIvm());
    }

    @Test
    public void testCreateIncrementalMVRejectsLeftOuterJoinWithOuterJoinOnNullSide() throws Exception {
        createIvmMowTable("ivm_loj_null_nested_l");
        createIvmMowTable("ivm_loj_null_nested_r");
        createIvmMowTable("ivm_loj_null_nested_n");

        assertCreateMtmvFails("CREATE MATERIALIZED VIEW ivm_loj_null_nested_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_loj_null_nested_l.k1, ivm_loj_null_nested_l.v1,"
                + " ivm_loj_null_nested_r.v1 AS rv1, ivm_loj_null_nested_n.v1 AS nv1\n"
                + " FROM ivm_loj_null_nested_l\n"
                + " LEFT OUTER JOIN (ivm_loj_null_nested_r\n"
                + "     LEFT OUTER JOIN ivm_loj_null_nested_n"
                + " ON ivm_loj_null_nested_r.k1 = ivm_loj_null_nested_n.k1)\n"
                + " ON ivm_loj_null_nested_l.k1 = ivm_loj_null_nested_r.k1;",
                "null side must not contain another outer join");
    }

    @Test
    public void testCreateIncrementalMVRejectsFullOuterJoinWithOuterJoinOnEitherNullSide() throws Exception {
        createIvmMowTable("ivm_foj_left_null_nested_l");
        createIvmMowTable("ivm_foj_left_null_nested_r");
        createIvmMowTable("ivm_foj_left_null_nested_n");
        createIvmMowTable("ivm_foj_right_null_nested_l");
        createIvmMowTable("ivm_foj_right_null_nested_r");
        createIvmMowTable("ivm_foj_right_null_nested_n");

        assertCreateMtmvFails("CREATE MATERIALIZED VIEW ivm_foj_left_null_nested_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_foj_left_null_nested_l.k1, ivm_foj_left_null_nested_l.v1,"
                + " ivm_foj_left_null_nested_r.v1 AS rv1, ivm_foj_left_null_nested_n.v1 AS nv1\n"
                + " FROM (ivm_foj_left_null_nested_l\n"
                + "     LEFT OUTER JOIN ivm_foj_left_null_nested_r"
                + " ON ivm_foj_left_null_nested_l.k1 = ivm_foj_left_null_nested_r.k1)\n"
                + " FULL OUTER JOIN ivm_foj_left_null_nested_n"
                + " ON ivm_foj_left_null_nested_l.k1 = ivm_foj_left_null_nested_n.k1;",
                "null side must not contain another outer join");

        assertCreateMtmvFails("CREATE MATERIALIZED VIEW ivm_foj_right_null_nested_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_foj_right_null_nested_l.k1, ivm_foj_right_null_nested_l.v1,"
                + " ivm_foj_right_null_nested_r.v1 AS rv1, ivm_foj_right_null_nested_n.v1 AS nv1\n"
                + " FROM ivm_foj_right_null_nested_l\n"
                + " FULL OUTER JOIN (ivm_foj_right_null_nested_r\n"
                + "     LEFT OUTER JOIN ivm_foj_right_null_nested_n"
                + " ON ivm_foj_right_null_nested_r.k1 = ivm_foj_right_null_nested_n.k1)\n"
                + " ON ivm_foj_right_null_nested_l.k1 = ivm_foj_right_null_nested_r.k1;",
                "null side must not contain another outer join");
    }

    @Test
    public void testCreateIncrementalAggMVAllowsJoinWithNonDeterministicChildRowId() throws Exception {
        createIvmDupTable("ivm_agg_inner_dup_l");
        createIvmMowTable("ivm_agg_inner_mow_r");
        createMtmv("CREATE MATERIALIZED VIEW ivm_agg_inner_join_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_agg_inner_dup_l.k1, COUNT(*) AS cnt, SUM(ivm_agg_inner_dup_l.v1) AS total\n"
                + " FROM ivm_agg_inner_dup_l\n"
                + " INNER JOIN ivm_agg_inner_mow_r ON ivm_agg_inner_dup_l.k1 = ivm_agg_inner_mow_r.k1\n"
                + " GROUP BY ivm_agg_inner_dup_l.k1;");

        MTMV innerJoinMtmv = getMtmv("ivm_agg_inner_join_mv");
        Assertions.assertTrue(innerJoinMtmv.isIvm());
        Assertions.assertTrue(innerJoinMtmv.getIvmInfo().isEnableIvm());

        createIvmDupTable("ivm_agg_left_dup_l");
        createIvmMowTable("ivm_agg_left_mow_r");
        createMtmv("CREATE MATERIALIZED VIEW ivm_agg_left_join_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_agg_left_dup_l.k1, COUNT(ivm_agg_left_mow_r.v1) AS matched_cnt,"
                + " SUM(ivm_agg_left_dup_l.v1) AS total\n"
                + " FROM ivm_agg_left_dup_l\n"
                + " LEFT OUTER JOIN ivm_agg_left_mow_r ON ivm_agg_left_dup_l.k1 = ivm_agg_left_mow_r.k1\n"
                + " GROUP BY ivm_agg_left_dup_l.k1;");

        MTMV leftJoinMtmv = getMtmv("ivm_agg_left_join_mv");
        Assertions.assertTrue(leftJoinMtmv.isIvm());
        Assertions.assertTrue(leftJoinMtmv.getIvmInfo().isEnableIvm());

        createIvmDupTable("ivm_agg_full_dup_l");
        createIvmDupTable("ivm_agg_full_dup_r");
        createMtmv("CREATE MATERIALIZED VIEW ivm_agg_full_join_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_agg_full_dup_l.k1, COUNT(*) AS cnt, SUM(ivm_agg_full_dup_r.v1) AS total\n"
                + " FROM ivm_agg_full_dup_l\n"
                + " FULL OUTER JOIN ivm_agg_full_dup_r ON ivm_agg_full_dup_l.k1 = ivm_agg_full_dup_r.k1\n"
                + " GROUP BY ivm_agg_full_dup_l.k1;");

        MTMV fullJoinMtmv = getMtmv("ivm_agg_full_join_mv");
        Assertions.assertTrue(fullJoinMtmv.isIvm());
        Assertions.assertTrue(fullJoinMtmv.getIvmInfo().isEnableIvm());
    }

    @Test
    public void testCreateIncrementalMVRejectsOuterJoinWithNonDeterministicRequiredRowId() throws Exception {
        createIvmDupTable("ivm_loj_nondet_l");
        createIvmMowTable("ivm_loj_nondet_r");
        createIvmMowTable("ivm_foj_nondet_l");
        createIvmDupTable("ivm_foj_nondet_r");

        assertCreateMtmvFails("CREATE MATERIALIZED VIEW ivm_loj_nondet_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_loj_nondet_l.k1, ivm_loj_nondet_l.v1, ivm_loj_nondet_r.v1 AS rv1\n"
                + " FROM ivm_loj_nondet_l\n"
                + " LEFT OUTER JOIN ivm_loj_nondet_r ON ivm_loj_nondet_l.k1 = ivm_loj_nondet_r.k1;",
                "requires deterministic row_id on retained side (left side)");

        assertCreateMtmvFails("CREATE MATERIALIZED VIEW ivm_foj_nondet_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_foj_nondet_l.k1, ivm_foj_nondet_l.v1, ivm_foj_nondet_r.v1 AS rv1\n"
                + " FROM ivm_foj_nondet_l\n"
                + " FULL OUTER JOIN ivm_foj_nondet_r ON ivm_foj_nondet_l.k1 = ivm_foj_nondet_r.k1;",
                "requires deterministic row_id on retained side (right side)");
    }

    // TODO: Add CREATE MV coverage for null-side UNION ALL after subquery alias is supported by IVM.

    @Test
    public void testAlterExcludedTriggerTablesRejectsIncludingUnsupportedBaseTable() throws Exception {
        createTable("create table test.ivm_alter_agg_base (k1 int, v1 int SUM)\n"
                + "aggregate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_alter_excluded_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1', 'excluded_trigger_tables' = 'ivm_alter_agg_base')\n"
                + " AS SELECT k1 FROM ivm_alter_agg_base;");
        MTMV mtmv = getMtmv("ivm_alter_excluded_mv");
        Assertions.assertTrue(mtmv.isIvm());

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> alterMtmv("ALTER MATERIALIZED VIEW ivm_alter_excluded_mv "
                        + "SET ('excluded_trigger_tables' = '')"));
        Assertions.assertTrue(ex.getMessage().contains("requires base tables to be"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testAlterExcludedTriggerTablesAllowsExpandingCoverage() throws Exception {
        createTable("create table test.ivm_expand_agg_base (k1 int, v1 int SUM)\n"
                + "aggregate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_expand_excluded_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1', 'excluded_trigger_tables' = 'test.ivm_expand_agg_base')\n"
                + " AS SELECT k1 FROM ivm_expand_agg_base;");
        MTMV mtmv = getMtmv("ivm_expand_excluded_mv");
        Assertions.assertTrue(mtmv.isIvm());

        alterMtmv("ALTER MATERIALIZED VIEW ivm_expand_excluded_mv "
                + "SET ('excluded_trigger_tables' = 'ivm_expand_agg_base')");

        Assertions.assertEquals(1, mtmv.getExcludedTriggerTables().size());
        Assertions.assertTrue(mtmv.getExcludedTriggerTables().contains(new TableNameInfo("ivm_expand_agg_base")));
    }

    @Test
    public void testAlterExcludedTriggerTablesAllowsNarrowingConfiguredScope() throws Exception {
        createTable("create table test.ivm_narrow_agg_base (k1 int, v1 int SUM)\n"
                + "aggregate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_narrow_excluded_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " PROPERTIES ('replication_num' = '1', 'excluded_trigger_tables' = 'ivm_narrow_agg_base')\n"
                + " AS SELECT k1 FROM ivm_narrow_agg_base;");
        MTMV mtmv = getMtmv("ivm_narrow_excluded_mv");
        Assertions.assertTrue(mtmv.isIvm());

        alterMtmv("ALTER MATERIALIZED VIEW ivm_narrow_excluded_mv "
                + "SET ('excluded_trigger_tables' = 'test.ivm_narrow_agg_base')");

        Assertions.assertEquals(1, mtmv.getExcludedTriggerTables().size());
        TableNameInfo excludedTable = mtmv.getExcludedTriggerTables().iterator().next();
        Assertions.assertEquals("test", excludedTable.getDb());
        Assertions.assertEquals("ivm_narrow_agg_base", excludedTable.getTbl());
    }

    @Test
    public void testCreateIncrementalMVRejectsWindowFunction() throws Exception {
        createTable("create table test.ivm_win_base (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> createMtmv("CREATE MATERIALIZED VIEW ivm_win_mv\n"
                        + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                        + " PROPERTIES ('replication_num' = '1')\n"
                        + " AS SELECT k1, row_number() OVER (ORDER BY k1) rn FROM ivm_win_base;"));
        Assertions.assertTrue(ex.getMessage().contains("IVM does not support window functions"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testCreateIncrementalMVAutoCreatesStream() throws Exception {
        createTable("create table test.ivm_stream_base (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_stream_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM ivm_stream_base;");
        MTMV mtmv = getMtmv("ivm_stream_mv");
        Assertions.assertTrue(mtmv.isIvm());
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        String streamName = IvmUtil.streamName(mtmv.getId(), "ivm_stream_base");
        TableIf streamTable = db.getTableNullable(streamName);
        Assertions.assertNotNull(streamTable,
                "Stream table should be auto-created for IVM base table");
    }

    @Test
    public void testCreateIncrementalMVAutoCreatesStreamForMultipleBaseTables() throws Exception {
        createTable("create table test.ivm_multi_stream_base1 (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createTable("create table test.ivm_multi_stream_base2 (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_multi_stream_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_multi_stream_base1.k1, ivm_multi_stream_base1.v1 "
                + "FROM ivm_multi_stream_base1 "
                + "INNER JOIN ivm_multi_stream_base2 "
                + "ON ivm_multi_stream_base1.k1 = ivm_multi_stream_base2.k1;");
        MTMV mtmv = getMtmv("ivm_multi_stream_mv");
        Assertions.assertTrue(mtmv.isIvm());
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        for (String baseName : new String[]{"ivm_multi_stream_base1", "ivm_multi_stream_base2"}) {
            String streamName = IvmUtil.streamName(mtmv.getId(), baseName);
            TableIf streamTable = db.getTableNullable(streamName);
            Assertions.assertNotNull(streamTable,
                    "Stream table should be auto-created for base table " + baseName);
        }
    }

    @Test
    public void testCreateIncrementalMVSkipsStreamForExcludedTriggerTable() throws Exception {
        createTable("create table test.ivm_excl_stream_base1 (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createTable("create table test.ivm_excl_stream_base2 (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_excl_stream_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1', "
                + "'excluded_trigger_tables' = 'test.ivm_excl_stream_base2')\n"
                + " AS SELECT ivm_excl_stream_base1.k1, ivm_excl_stream_base1.v1 "
                + "FROM ivm_excl_stream_base1 "
                + "INNER JOIN ivm_excl_stream_base2 "
                + "ON ivm_excl_stream_base1.k1 = ivm_excl_stream_base2.k1;");
        MTMV mtmv = getMtmv("ivm_excl_stream_mv");
        Assertions.assertTrue(mtmv.isIvm());
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        String stream1 = IvmUtil.streamName(mtmv.getId(), "ivm_excl_stream_base1");
        Assertions.assertNotNull(db.getTableNullable(stream1),
                "Stream should be created for non-excluded table");
        String stream2 = IvmUtil.streamName(mtmv.getId(), "ivm_excl_stream_base2");
        Assertions.assertNull(db.getTableNullable(stream2),
                "Stream should NOT be created for excluded table");
    }

    @Test
    public void testAlterExcludedTriggerTablesReconcilesStreams() throws Exception {
        createTable("create table test.ivm_alter_excl_stream_base1 (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createTable("create table test.ivm_alter_excl_stream_base2 (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_alter_excl_stream_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT ivm_alter_excl_stream_base1.k1, ivm_alter_excl_stream_base1.v1 "
                + "FROM ivm_alter_excl_stream_base1 "
                + "INNER JOIN ivm_alter_excl_stream_base2 "
                + "ON ivm_alter_excl_stream_base1.k1 = ivm_alter_excl_stream_base2.k1;");
        MTMV mtmv = getMtmv("ivm_alter_excl_stream_mv");
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        String stream1 = IvmUtil.streamName(mtmv.getId(), "ivm_alter_excl_stream_base1");
        String stream2 = IvmUtil.streamName(mtmv.getId(), "ivm_alter_excl_stream_base2");
        Assertions.assertNotNull(db.getTableNullable(stream1));
        Assertions.assertNotNull(db.getTableNullable(stream2));

        alterMtmv("ALTER MATERIALIZED VIEW ivm_alter_excl_stream_mv "
                + "SET ('excluded_trigger_tables' = 'ivm_alter_excl_stream_base2')");

        Assertions.assertFalse(mtmv.getIvmInfo().isBinlogBroken(),
                "Excluding a base table should not mark its binlog as broken");
        Assertions.assertNotNull(db.getTableNullable(stream1),
                "Stream should remain for non-excluded table");
        Assertions.assertNull(db.getTableNullable(stream2),
                "Stream should be dropped for newly excluded table");

        String createStreamSql = "CREATE STREAM test." + stream2
                + " ON TABLE test.ivm_alter_excl_stream_base2 "
                + "PROPERTIES ('type' = 'min_delta', 'show_initial_rows' = 'true')";
        resetStatementContext(createStreamSql);
        super.createTable(createStreamSql);
        Assertions.assertNotNull(db.getTableNullable(stream2));

        alterMtmv("ALTER MATERIALIZED VIEW ivm_alter_excl_stream_mv "
                + "SET ('excluded_trigger_tables' = 'ivm_alter_excl_stream_base2')");

        Assertions.assertNull(db.getTableNullable(stream2),
                "Stream should be dropped when its base table is already excluded");

        alterMtmv("ALTER MATERIALIZED VIEW ivm_alter_excl_stream_mv "
                + "SET ('excluded_trigger_tables' = 'ivm_alter_excl_stream_base1')");

        Assertions.assertNull(db.getTableNullable(stream1),
                "Stream should be dropped for newly excluded table");
        Assertions.assertNotNull(db.getTableNullable(stream2),
                "Stream should be created for a table removed from excluded_trigger_tables");
        Assertions.assertTrue(mtmv.getIvmInfo().isBinlogBroken(),
                "Including a base table should require rebuilding the IVM baseline");
    }

    @Test
    public void testAlterExcludedTriggerTablesReplayDoesNotCreateStream() throws Exception {
        createTable("create table test.ivm_replay_excl_stream_base (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_replay_excl_stream_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1', "
                + "'excluded_trigger_tables' = 'ivm_replay_excl_stream_base')\n"
                + " AS SELECT k1, v1 FROM ivm_replay_excl_stream_base;");
        MTMV mtmv = getMtmv("ivm_replay_excl_stream_mv");
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        String streamName = IvmUtil.streamName(mtmv.getId(), "ivm_replay_excl_stream_base");
        Assertions.assertNull(db.getTableNullable(streamName));

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "");
        AlterMTMV replayAlter = new AlterMTMV(
                new TableNameInfo("test", "ivm_replay_excl_stream_mv"), MTMVAlterOpType.ALTER_PROPERTY);
        replayAlter.setMvProperties(properties);
        Env.getCurrentEnv().getAlterInstance().processAlterMTMV(replayAlter, true);

        Assertions.assertTrue(mtmv.getExcludedTriggerTables().isEmpty());
        Assertions.assertTrue(mtmv.getIvmInfo().isBinlogBroken(),
                "ALTER replay should restore the IVM baseline invalidation state");
        Assertions.assertNull(db.getTableNullable(streamName),
                "ALTER replay should rely on OP_CREATE_TABLE replay instead of creating a new stream");
    }

    @Test
    public void testAlterExcludedTriggerTablesStopsWhenDatabaseDropped() throws Exception {
        createIvmMowTable("ivm_dropped_db_stream_base");
        createMtmv("CREATE MATERIALIZED VIEW ivm_dropped_db_stream_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM ivm_dropped_db_stream_base;");
        MTMV mtmv = getMtmv("ivm_dropped_db_stream_mv");
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        String streamName = IvmUtil.streamName(mtmv.getId(), "ivm_dropped_db_stream_base");
        Assertions.assertNotNull(db.getTableNullable(streamName));

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "ivm_dropped_db_stream_base");
        AlterMTMV alter = new AlterMTMV(
                new TableNameInfo("test", "ivm_dropped_db_stream_mv"), MTMVAlterOpType.ALTER_PROPERTY);
        alter.setMvProperties(properties);

        db.markDropped();
        try {
            Env.getCurrentEnv().getAlterInstance().processAlterMTMV(alter, false);
        } finally {
            db.unmarkDropped();
        }

        Assertions.assertTrue(mtmv.getExcludedTriggerTables().isEmpty(),
                "ALTER should not update properties after failing to lock a dropped database");
        Assertions.assertNotNull(db.getTableNullable(streamName),
                "ALTER should not drop streams after failing to lock a dropped database");
    }

    @Test
    public void testCreateNonIncrementalMvDoesNotCreateStream() throws Exception {
        createTable("create table test.ivm_no_stream_base (k1 int, v1 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true');");
        createMtmv("CREATE MATERIALIZED VIEW ivm_no_stream_mv\n"
                + " BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM ivm_no_stream_base;");
        MTMV mtmv = getMtmv("ivm_no_stream_mv");
        Assertions.assertFalse(mtmv.isIvm());
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test");
        String streamName = IvmUtil.streamName(mtmv.getId(), "ivm_no_stream_base");
        Assertions.assertNull(db.getTableNullable(streamName),
                "Stream should NOT be auto-created for non-IVM MV");
    }
}
