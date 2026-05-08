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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVPartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.SimpleColumnDefinition;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MTMVPlanUtilTest extends SqlTestBase {

    @Test
    public void testGenerateColumnsBySql() throws Exception {
        createTables(
                "CREATE TABLE IF NOT EXISTS MTMVPlanUtilTestT1 (\n"
                        + "    id varchar(10),\n"
                        + "    score String\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "AUTO PARTITION BY LIST(`id`)\n"
                        + "(\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );

        String querySql = "select * from test.T1";
        List<ColumnDefinition> actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        List<ColumnDefinition> expect = Lists.newArrayList(new ColumnDefinition("id", BigIntType.INSTANCE, true),
                new ColumnDefinition("score", BigIntType.INSTANCE, true));
        checkRes(expect, actual);

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN, "true");
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(), properties);
        expect = Lists.newArrayList(new ColumnDefinition("id", BigIntType.INSTANCE, true),
                new ColumnDefinition("score", BigIntType.INSTANCE, true),
                new ColumnDefinition(Column.ROW_STORE_COL, StringType.INSTANCE, false));
        checkRes(expect, actual);

        querySql = "select T1.id from test.T1 inner join test.T2 on T1.id = T2.id";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        expect = Lists.newArrayList(new ColumnDefinition("id", BigIntType.INSTANCE, true));
        checkRes(expect, actual);

        querySql = "select id,sum(score) from test.T1 group by id";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        expect = Lists.newArrayList(new ColumnDefinition("id", BigIntType.INSTANCE, true),
                new ColumnDefinition("__sum_1", BigIntType.INSTANCE, true));
        checkRes(expect, actual);

        querySql = "select id,sum(score) from test.T1 group by id";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(new SimpleColumnDefinition("id", null),
                        new SimpleColumnDefinition("sum_score", null)),
                Maps.newHashMap());
        expect = Lists.newArrayList(new ColumnDefinition("id", BigIntType.INSTANCE, true),
                new ColumnDefinition("sum_score", BigIntType.INSTANCE, true));
        checkRes(expect, actual);

        querySql = "select * from test.MTMVPlanUtilTestT1";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        expect = Lists.newArrayList(new ColumnDefinition("id", new VarcharType(10), true),
                new ColumnDefinition("score", StringType.INSTANCE, true));
        checkRes(expect, actual);

        querySql = "select score from test.MTMVPlanUtilTestT1";
        actual = MTMVPlanUtil.generateColumnsBySql(querySql, connectContext, null,
                Sets.newHashSet(), Lists.newArrayList(),
                Maps.newHashMap());
        expect = Lists.newArrayList(
                new ColumnDefinition("score", VarcharType.MAX_VARCHAR_TYPE, true));
        checkRes(expect, actual);
    }

    private void checkRes(List<ColumnDefinition> expect, List<ColumnDefinition> actual) {
        Assertions.assertEquals(expect.size(), actual.size());
        for (int i = 0; i < expect.size(); i++) {
            Assertions.assertEquals(expect.get(i).getName(), actual.get(i).getName());
            Assertions.assertEquals(expect.get(i).getType(), actual.get(i).getType());
        }
    }

    @Test
    public void testGetDataType() {
        SlotReference slot = Mockito.mock(SlotReference.class);
        TableIf slotTable = Mockito.mock(TableIf.class);
        Mockito.when(slot.getDataType()).thenReturn(StringType.INSTANCE);
        Mockito.when(slot.isColumnFromTable()).thenReturn(true);
        Mockito.when(slot.getOriginalTable()).thenReturn(Optional.empty());
        Mockito.when(slot.getName()).thenReturn("slot_name");
        // test i=0
        DataType dataType = MTMVPlanUtil.getDataType(slot, 0, connectContext, "pcol", Sets.newHashSet("dcol"));
        Assertions.assertEquals(VarcharType.MAX_VARCHAR_TYPE, dataType);

        // test isColumnFromTable and is not managed table
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("dcol"));
        Assertions.assertEquals(StringType.INSTANCE, dataType);

        // test is partitionCol
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "slot_name", Sets.newHashSet("dcol"));
        Assertions.assertEquals(VarcharType.MAX_VARCHAR_TYPE, dataType);

        // test is partitdistribution Col
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assertions.assertEquals(VarcharType.MAX_VARCHAR_TYPE, dataType);
        // test managed table
        Mockito.when(slot.getOriginalTable()).thenReturn(Optional.of(slotTable));
        Mockito.when(slotTable.isManagedTable()).thenReturn(true);

        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assertions.assertEquals(StringType.INSTANCE, dataType);

        // test is not column table
        boolean originalUseMaxLengthOfVarcharInCtas = connectContext.getSessionVariable().useMaxLengthOfVarcharInCtas;
        Mockito.when(slot.getDataType()).thenReturn(new VarcharType(10));
        Mockito.when(slot.isColumnFromTable()).thenReturn(false);
        connectContext.getSessionVariable().useMaxLengthOfVarcharInCtas = true;
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assertions.assertEquals(VarcharType.MAX_VARCHAR_TYPE, dataType);

        connectContext.getSessionVariable().useMaxLengthOfVarcharInCtas = false;
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assertions.assertEquals(new VarcharType(10), dataType);

        connectContext.getSessionVariable().useMaxLengthOfVarcharInCtas = originalUseMaxLengthOfVarcharInCtas;

        // test null type
        Mockito.when(slot.getDataType()).thenReturn(NullType.INSTANCE);
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assertions.assertEquals(TinyIntType.INSTANCE, dataType);

        // test decimal type
        Mockito.when(slot.getDataType()).thenReturn(DecimalV2Type.createDecimalV2Type(1, 1));
        boolean originalEnableDecimalConversion = Config.enable_decimal_conversion;
        Config.enable_decimal_conversion = false;
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assertions.assertEquals(DecimalV2Type.SYSTEM_DEFAULT, dataType);

        Config.enable_decimal_conversion = originalEnableDecimalConversion;
    }

    @Test
    public void testGenerateColumns() {
        SlotReference slot = Mockito.mock(SlotReference.class);
        Plan plan = Mockito.mock(Plan.class);
        Mockito.when(plan.getOutput()).thenReturn(Lists.newArrayList());
        // test slots is empty
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () ->
                MTMVPlanUtil.generateColumns(plan, connectContext, null, null, null, null));

        Mockito.when(plan.getOutput()).thenReturn(Lists.newArrayList(slot));

        // test size of slots and SimpleColumnDefinitions is different
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () ->
                MTMVPlanUtil.generateColumns(plan, connectContext, null, null,
                        Lists.newArrayList(new SimpleColumnDefinition("col1", "c1"),
                                new SimpleColumnDefinition("col2", "c2")), null));

        // test name format
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () ->
                MTMVPlanUtil.generateColumns(plan, connectContext, null, null,
                        Lists.newArrayList(new SimpleColumnDefinition("", "c1")), null));

        Mockito.when(slot.getDataType()).thenReturn(BigIntType.INSTANCE);
        Mockito.when(slot.nullable()).thenReturn(true);
        Mockito.when(plan.getOutput()).thenReturn(Lists.newArrayList(slot, slot));
        // test repeat col name
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () ->
                MTMVPlanUtil.generateColumns(plan, connectContext, null, null,
                        Lists.newArrayList(new SimpleColumnDefinition("col1", "c1"),
                                new SimpleColumnDefinition("col1", "c2")), null));
    }

    @Test
    public void testAnalyzeQuerynNonDeterministic() throws Exception {
        String querySql = "select *,now() from test.T1";
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.SELF_MANAGE);
        DistributionDescriptor distributionDescriptor = new DistributionDescriptor(false, true, 10,
                Lists.newArrayList("id"));
        StatementBase parsedStmt = new NereidsParser().parseSQL(querySql).get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();

        AnalysisException exception = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class, () -> {
                    MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), mtmvPartitionDefinition,
                            distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan, false);
                });
        Assertions.assertTrue(exception.getMessage().contains("nonDeterministic"));
    }

    @Test
    public void testAnalyzeQueryFromTablet() throws Exception {
        String querySql = "select * from test.T1 tablet (20001)";
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.SELF_MANAGE);
        DistributionDescriptor distributionDescriptor = new DistributionDescriptor(false, true, 10,
                Lists.newArrayList("id"));
        StatementBase parsedStmt = new NereidsParser().parseSQL(querySql).get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();

        AnalysisException exception = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class, () -> {
                    MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), mtmvPartitionDefinition,
                            distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan, false);
                });
        Assertions.assertTrue(exception.getMessage().contains("invalid expression"));
    }

    @Test
    public void testAnalyzeQueryFromTempTable() throws Exception {
        createTables(
                "CREATE TEMPORARY TABLE IF NOT EXISTS temp_t1 (\n"
                        + "    id varchar(10),\n"
                        + "    score String\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "AUTO PARTITION BY LIST(`id`)\n"
                        + "(\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
        String querySql = "select * from test.temp_t1";
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.SELF_MANAGE);
        DistributionDescriptor distributionDescriptor = new DistributionDescriptor(false, true, 10,
                Lists.newArrayList("id"));
        StatementBase parsedStmt = new NereidsParser().parseSQL(querySql).get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();

        AnalysisException exception = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class, () -> {
                    MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), mtmvPartitionDefinition,
                            distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan, false);
                });
        Assertions.assertTrue(exception.getMessage().contains("temporary"));
    }

    @Test
    public void testAnalyzeQueryFollowBaseTableFailed() throws Exception {
        String querySql = "select * from test.T4";
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.FOLLOW_BASE_TABLE);
        mtmvPartitionDefinition.setPartitionCol("score");
        DistributionDescriptor distributionDescriptor = new DistributionDescriptor(false, true, 10,
                Lists.newArrayList("id"));
        StatementBase parsedStmt = new NereidsParser().parseSQL(querySql).get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();

        AnalysisException exception = Assertions.assertThrows(
                org.apache.doris.nereids.exceptions.AnalysisException.class, () -> {
                    MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), mtmvPartitionDefinition,
                            distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan, false);
                });
        Assertions.assertTrue(exception.getMessage().contains("suitable"));
    }

    @Test
    public void testAnalyzeQueryNormal() throws Exception {
        String querySql = "select * from test.T4";
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.FOLLOW_BASE_TABLE);
        mtmvPartitionDefinition.setPartitionCol("id");
        DistributionDescriptor distributionDescriptor = new DistributionDescriptor(false, true, 10,
                Lists.newArrayList("id"));
        StatementBase parsedStmt = new NereidsParser().parseSQL(querySql).get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        MTMVAnalyzeQueryInfo mtmvAnalyzeQueryInfo = MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(),
                mtmvPartitionDefinition,
                distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan, false);
        Assertions.assertTrue(mtmvAnalyzeQueryInfo.getRelation().getBaseTables().size() == 1);
        Assertions.assertTrue(mtmvAnalyzeQueryInfo.getMvPartitionInfo().getRelatedCol().equals("id"));
        Assertions.assertTrue(mtmvAnalyzeQueryInfo.getColumnDefinitions().size() == 2);
    }

    @Test
    public void testEnsureMTMVQueryUsable() throws Exception {
        createMvByNereids("create materialized view mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.T4;");
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv1");
        Assertions.assertDoesNotThrow(
                () -> MTMVPlanUtil.ensureMTMVQueryUsable(mtmv,
                        MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE)));
    }

    @Test
    public void testAnalyzeQueryIvmAnalyzeModeSetSessionVariables() throws Exception {
        String querySql = "select * from test.T4";
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.FOLLOW_BASE_TABLE);
        mtmvPartitionDefinition.setPartitionCol("id");
        DistributionDescriptor distributionDescriptor = new DistributionDescriptor(false, true, 10,
                Lists.newArrayList("id"));
        StatementBase parsedStmt = new NereidsParser().parseSQL(querySql).get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();

        // enableIvmNormalize=false: no IVM session variables set
        CountingSessionVariable disabledVar = new CountingSessionVariable();
        connectContext.setSessionVariable(disabledVar);
        MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), mtmvPartitionDefinition,
                distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan,
                false);
        Assertions.assertEquals(0, disabledVar.getEnableIvmRewriteSetCount());

        // enableIvmNormalize=true: ENABLE_IVM_NORMAL_REWRITE set
        CountingSessionVariable enabledVar = new CountingSessionVariable();
        connectContext.setSessionVariable(enabledVar);
        MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), mtmvPartitionDefinition,
                distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan,
                true);
        Assertions.assertEquals(1, enabledVar.getEnableIvmRewriteSetCount());
    }

    @Test
    public void testAnalyzeQueryIvmAddsMowHiddenColumns() throws Exception {
        String querySql = "select id from test.T4";
        MTMVPartitionDefinition mtmvPartitionDefinition = new MTMVPartitionDefinition();
        mtmvPartitionDefinition.setPartitionType(MTMVPartitionType.SELF_MANAGE);
        DistributionDescriptor distributionDescriptor = new DistributionDescriptor(false, true, 10,
                Lists.newArrayList("id"));
        StatementBase parsedStmt = new NereidsParser().parseSQL(querySql).get(0);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();

        MTMVAnalyzeQueryInfo queryInfo = MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(),
                mtmvPartitionDefinition, distributionDescriptor, null, Maps.newHashMap(),
                Lists.newArrayList(), logicalPlan, true);
        List<String> columnNames = queryInfo.getColumnDefinitions().stream()
                .map(ColumnDefinition::getName)
                .collect(java.util.stream.Collectors.toList());

        Assertions.assertTrue(columnNames.contains(Column.IVM_ROW_ID_COL));
        Assertions.assertTrue(columnNames.contains(Column.DELETE_SIGN));
        if (Config.enable_hidden_version_column_by_default) {
            Assertions.assertTrue(columnNames.contains(Column.VERSION_COL));
        }
        Assertions.assertEquals(Boolean.toString(Config.enable_skip_bitmap_column_by_default),
                queryInfo.getProperties().get(PropertyAnalyzer.ENABLE_UNIQUE_KEY_SKIP_BITMAP_COLUMN));
    }

    @Test
    public void testAnalyzeQueryWithSqlDoesNotMutateMtmvProperties() throws Exception {
        createMvByNereids("create materialized view mv_ivm_analyze_query_props "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1') \n"
                + "as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_ivm_analyze_query_props");
        Assertions.assertNull(
                mtmv.getTableProperty().getProperties().get(PropertyAnalyzer.ENABLE_UNIQUE_KEY_SKIP_BITMAP_COLUMN));

        MTMVPlanUtil.analyzeQueryWithSql(mtmv,
                MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE), true);

        Assertions.assertNull(
                mtmv.getTableProperty().getProperties().get(PropertyAnalyzer.ENABLE_UNIQUE_KEY_SKIP_BITMAP_COLUMN));
    }

    @Test
    public void testEnsureMTMVQueryUsableEnableIvmRewriteByRefreshMethod() throws Exception {
        createMvByNereids("create materialized view mv_auto_refresh BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.T4;");
        createMvByNereids("create materialized view mv_incremental_refresh "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV autoMtmv = (MTMV) db.getTableOrAnalysisException("mv_auto_refresh");
        MTMV incrementalMtmv = (MTMV) db.getTableOrAnalysisException("mv_incremental_refresh");

        CountingSessionVariable autoSessionVariable = new CountingSessionVariable();
        ConnectContext autoCtx = MTMVPlanUtil.createMTMVContext(autoMtmv,
                MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE);
        autoCtx.setSessionVariable(autoSessionVariable);
        Assertions.assertDoesNotThrow(() -> MTMVPlanUtil.ensureMTMVQueryUsable(autoMtmv, autoCtx));
        Assertions.assertEquals(1, autoSessionVariable.getEnableIvmRewriteSetCount());

        CountingSessionVariable incrementalSessionVariable = new CountingSessionVariable();
        ConnectContext incrementalCtx = MTMVPlanUtil.createMTMVContext(incrementalMtmv,
                MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE);
        incrementalCtx.setSessionVariable(incrementalSessionVariable);
        Assertions.assertDoesNotThrow(() -> MTMVPlanUtil.ensureMTMVQueryUsable(incrementalMtmv, incrementalCtx));
        Assertions.assertEquals(1, incrementalSessionVariable.getEnableIvmRewriteSetCount());
    }

    @Test
    public void testEnsureMTMVQueryAnalyzeFailed() throws Exception {
        createTable("CREATE TABLE IF NOT EXISTS analyze_faild_t_partition (\n"
                + "    id bigint not null,\n"
                + "    score bigint\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "AUTO PARTITION BY LIST(`score`)\n"
                + "(\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")\n");
        createTable("CREATE TABLE IF NOT EXISTS analyze_faild_t_not_partition (\n"
                + "    id bigint not null,\n"
                + "    score bigint\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")\n");
        createView("create view analyze_faild_v1 as select * from test.analyze_faild_t_partition");

        createMvByNereids("create materialized view analyze_faild_mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        PARTITION BY (score)\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.analyze_faild_v1;");
        dropView("drop view analyze_faild_v1");
        createView("create view analyze_faild_v1 as select * from test.analyze_faild_t_not_partition");
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("analyze_faild_mv1");
        JobException exception = Assertions.assertThrows(
                org.apache.doris.job.exception.JobException.class, () -> {
                    MTMVPlanUtil.ensureMTMVQueryUsable(mtmv,
                            MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE));
                });
        Assertions.assertTrue(exception.getMessage().contains("suitable"));
    }

    @Test
    public void testEnsureMTMVQueryNotEqual() throws Exception {
        createTable("CREATE TABLE IF NOT EXISTS t_partition1 (\n"
                + "    id bigint not null,\n"
                + "    score bigint\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "AUTO PARTITION BY LIST(`score`)\n"
                + "(\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")\n");
        createTable("CREATE TABLE IF NOT EXISTS t_partition2 (\n"
                + "    id bigint not null,\n"
                + "    score bigint\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "AUTO PARTITION BY LIST(`score`)\n"
                + "(\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ")\n");
        createView("create view t_partition1_v1 as select * from test.t_partition1");

        createMvByNereids("create materialized view t_partition1_mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        PARTITION BY (score)\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.t_partition1_v1;");
        dropView("drop view t_partition1_v1");
        createView("create view t_partition1_v1 as select * from test.t_partition2");
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("t_partition1_mv1");
        JobException exception = Assertions.assertThrows(
                org.apache.doris.job.exception.JobException.class, () -> {
                    MTMVPlanUtil.ensureMTMVQueryUsable(mtmv,
                            MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE));
                });
        Assertions.assertTrue(exception.getMessage().contains("changed"));
    }

    // Background:
    // IVM (INCREMENTAL) materialized views are internally stored as UNIQUE_KEYS + Merge-On-Write (MOW)
    // tables, while normal (COMPLETE) MVs use DUP_KEYS. The original validateColumns() hardcoded
    // KeysType.DUP_KEYS for all MVs, which caused ColumnDefinition.validate() to set incorrect
    // aggTypeImplicit values for IVM MV columns.
    //
    // What is aggTypeImplicit?
    //   aggTypeImplicit is a boolean field on Column that indicates whether the column's aggregation
    //   type (e.g. NONE, REPLACE) was implicitly assigned by the system rather than explicitly
    //   specified by the user. When aggTypeImplicit=true, the aggregation type is considered an
    //   internal implementation detail and is hidden from user-facing outputs.
    //
    // How aggTypeImplicit is set (in ColumnDefinition.validate()):
    //   - DUP_KEYS value columns           -> aggTypeImplicit = true  (agg type is system-assigned)
    //   - UNIQUE_KEYS + MOW value columns  -> aggTypeImplicit = false (agg type is meaningful)
    //   - UNIQUE_KEYS non-MOW value columns -> aggTypeImplicit = true
    //
    // Where aggTypeImplicit is used downstream:
    //   - Column.toSql() (SHOW CREATE TABLE): when true, the aggregation type is hidden from DDL output
    //   - Column.equals() / equalsForDistribution(): schema comparison includes this field, so a
    //     mismatch causes schema inequality even if the actual aggregation type is the same
    //
    // The fix: derive KeysType from finalEnableMergeOnWrite (true -> UNIQUE_KEYS, false -> DUP_KEYS)
    // so validate() produces the correct aggTypeImplicit for each MV type.

    // IVM MV must be created as UNIQUE_KEYS + MOW internally, regardless of user input
    @Test
    public void testIncrementalMvIsUniqueKeyWithMow() throws Exception {
        createMvByNereids("create materialized view mv_ivm_unique_key "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_ivm_unique_key");

        Assertions.assertEquals(org.apache.doris.catalog.KeysType.UNIQUE_KEYS, mtmv.getKeysType());
        Assertions.assertTrue(mtmv.getEnableUniqueKeyMergeOnWrite());
    }

    // Non-IVM (COMPLETE) MV must use DUP_KEYS — the original default behavior
    @Test
    public void testNonIncrementalMvIsDuplicateKey() throws Exception {
        createMvByNereids("create materialized view mv_non_ivm_dup_key "
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_non_ivm_dup_key");

        Assertions.assertEquals(org.apache.doris.catalog.KeysType.DUP_KEYS, mtmv.getKeysType());
    }

    // User explicitly specifying UNIQUE KEY on IVM MV must fail — the system auto-assigns UNIQUE_KEYS
    @Test
    public void testIncrementalMvWithUserSpecifiedUniqueKeyFails() {
        Assertions.assertThrows(Exception.class, () ->
                createMvByNereids("create materialized view mv_ivm_user_unique_key "
                        + "UNIQUE KEY(id) "
                        + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                        + "        DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "        PROPERTIES ('replication_num' = '1') \n"
                        + "        as select * from test.T4;"));
    }

    // User explicitly specifying DUPLICATE KEY on IVM MV must fail — same reason as above
    @Test
    public void testIncrementalMvWithUserSpecifiedDupKeyFails() {
        Assertions.assertThrows(Exception.class, () ->
                createMvByNereids("create materialized view mv_ivm_user_dup_key "
                        + "DUPLICATE KEY(id) "
                        + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                        + "        DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "        PROPERTIES ('replication_num' = '1') \n"
                        + "        as select * from test.T4;"));
    }

    // IVM value columns must have aggTypeImplicit=false (UNIQUE_KEYS+MOW behavior)
    // Before the fix, validateColumns() used DUP_KEYS which incorrectly set aggTypeImplicit=true
    @Test
    public void testIncrementalMvColumnAggTypeNotImplicit() throws Exception {
        createMvByNereids("create materialized view mv_ivm_agg_type "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_ivm_agg_type");

        // UNIQUE_KEYS + MOW columns should have aggTypeImplicit = false
        for (Column col : mtmv.getBaseSchema()) {
            if (!col.isKey() && !col.getName().startsWith("__")) {
                Assertions.assertFalse(col.isAggregationTypeImplicit(),
                        "IVM non-key column '" + col.getName() + "' should have aggTypeImplicit=false");
            }
        }
    }

    // Counterpart: non-IVM MV value columns must have aggTypeImplicit=true (DUP_KEYS behavior)
    @Test
    public void testNonIncrementalMvColumnAggTypeImplicit() throws Exception {
        createMvByNereids("create materialized view mv_non_ivm_agg_type "
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_non_ivm_agg_type");

        // DUP_KEYS columns should have aggTypeImplicit = true
        for (Column col : mtmv.getBaseSchema()) {
            if (!col.isKey()) {
                Assertions.assertTrue(col.isAggregationTypeImplicit(),
                        "Non-IVM non-key column '" + col.getName() + "' should have aggTypeImplicit=true");
            }
        }
    }

    // IVM adds a hidden __ivm_row_id column for row-level tracking. Verify it exists in full schema
    // but is invisible to users via getBaseSchema(false)
    @Test
    public void testIncrementalMvRowIdHiddenFromUserSchema() throws Exception {
        createMvByNereids("create materialized view mv_ivm_rowid_check "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_ivm_rowid_check");

        // Full schema (including hidden columns) should contain the IVM row-id column
        List<Column> fullSchema = mtmv.getBaseSchema(true);
        boolean hasRowId = fullSchema.stream()
                .anyMatch(c -> Column.IVM_ROW_ID_COL.equals(c.getName()));
        Assertions.assertTrue(hasRowId,
                "Full schema should contain hidden IVM row-id column");

        // Visible (user) schema should NOT contain the IVM row-id column
        List<Column> visibleSchema = mtmv.getBaseSchema(false);
        boolean visibleHasRowId = visibleSchema.stream()
                .anyMatch(c -> Column.IVM_ROW_ID_COL.equals(c.getName()));
        Assertions.assertFalse(visibleHasRowId,
                "Visible schema should not expose IVM row-id column to user");
    }

    private static class CountingSessionVariable extends SessionVariable {
        private int enableIvmRewriteSetCount;

        @Override
        public boolean setVarOnce(String varName, String value) {
            if (ENABLE_IVM_NORMAL_REWRITE.equals(varName) && "true".equals(value)) {
                enableIvmRewriteSetCount++;
            }
            return super.setVarOnce(varName, value);
        }

        public int getEnableIvmRewriteSetCount() {
            return enableIvmRewriteSetCount;
        }
    }
}
