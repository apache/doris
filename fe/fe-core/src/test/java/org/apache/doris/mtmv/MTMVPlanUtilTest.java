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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        Assert.assertEquals(expect.size(), actual.size());
        for (int i = 0; i < expect.size(); i++) {
            Assert.assertEquals(expect.get(i).getName(), actual.get(i).getName());
            Assert.assertEquals(expect.get(i).getType(), actual.get(i).getType());
        }
    }

    @Test
    public void testGetDataType(@Mocked SlotReference slot, @Mocked TableIf slotTable) {
        new Expectations() {
            {
                slot.getDataType();
                minTimes = 0;
                result = StringType.INSTANCE;

                slot.isColumnFromTable();
                minTimes = 0;
                result = true;

                slot.getOriginalTable();
                minTimes = 0;
                result = Optional.empty();

                slot.getName();
                minTimes = 0;
                result = "slot_name";
            }
        };
        // test i=0
        DataType dataType = MTMVPlanUtil.getDataType(slot, 0, connectContext, "pcol", Sets.newHashSet("dcol"));
        Assert.assertEquals(VarcharType.MAX_VARCHAR_TYPE, dataType);

        // test isColumnFromTable and is not managed table
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("dcol"));
        Assert.assertEquals(StringType.INSTANCE, dataType);

        // test is partitionCol
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "slot_name", Sets.newHashSet("dcol"));
        Assert.assertEquals(VarcharType.MAX_VARCHAR_TYPE, dataType);

        // test is partitdistribution Col
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assert.assertEquals(VarcharType.MAX_VARCHAR_TYPE, dataType);
        // test managed table
        new Expectations() {
            {
                slot.getOriginalTable();
                minTimes = 0;
                result = Optional.of(slotTable);

                slotTable.isManagedTable();
                minTimes = 0;
                result = true;
            }
        };

        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assert.assertEquals(StringType.INSTANCE, dataType);

        // test is not column table
        boolean originalUseMaxLengthOfVarcharInCtas = connectContext.getSessionVariable().useMaxLengthOfVarcharInCtas;
        new Expectations() {
            {
                slot.getDataType();
                minTimes = 0;
                result = new VarcharType(10);

                slot.isColumnFromTable();
                minTimes = 0;
                result = false;
            }
        };
        connectContext.getSessionVariable().useMaxLengthOfVarcharInCtas = true;
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assert.assertEquals(VarcharType.MAX_VARCHAR_TYPE, dataType);

        connectContext.getSessionVariable().useMaxLengthOfVarcharInCtas = false;
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assert.assertEquals(new VarcharType(10), dataType);

        connectContext.getSessionVariable().useMaxLengthOfVarcharInCtas = originalUseMaxLengthOfVarcharInCtas;

        // test null type
        new Expectations() {
            {
                slot.getDataType();
                minTimes = 0;
                result = NullType.INSTANCE;
            }
        };
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assert.assertEquals(TinyIntType.INSTANCE, dataType);

        // test decimal type
        new Expectations() {
            {
                slot.getDataType();
                minTimes = 0;
                result = DecimalV2Type.createDecimalV2Type(1, 1);
            }
        };
        boolean originalEnableDecimalConversion = Config.enable_decimal_conversion;
        Config.enable_decimal_conversion = false;
        dataType = MTMVPlanUtil.getDataType(slot, 1, connectContext, "pcol", Sets.newHashSet("slot_name"));
        Assert.assertEquals(DecimalV2Type.SYSTEM_DEFAULT, dataType);

        Config.enable_decimal_conversion = originalEnableDecimalConversion;
    }

    @Test
    public void testGenerateColumns(@Mocked SlotReference slot, @Mocked Plan plan) {
        new Expectations() {
            {
                plan.getOutput();
                minTimes = 0;
                result = Lists.newArrayList();
            }
        };
        // test slots is empty
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () ->
                MTMVPlanUtil.generateColumns(plan, connectContext, null, null, null, null));

        new Expectations() {
            {
                plan.getOutput();
                minTimes = 0;
                result = Lists.newArrayList(slot);
            }
        };

        // test size of slots and SimpleColumnDefinitions is different
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () ->
                MTMVPlanUtil.generateColumns(plan, connectContext, null, null,
                        Lists.newArrayList(new SimpleColumnDefinition("col1", "c1"),
                                new SimpleColumnDefinition("col2", "c2")), null));

        // test name format
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () ->
                MTMVPlanUtil.generateColumns(plan, connectContext, null, null,
                        Lists.newArrayList(new SimpleColumnDefinition("", "c1")), null));

        new Expectations() {
            {
                plan.getOutput();
                minTimes = 0;
                result = Lists.newArrayList(slot, slot);
            }
        };
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
                    MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), querySql, mtmvPartitionDefinition,
                            distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan);
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
                    MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), querySql, mtmvPartitionDefinition,
                            distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan);
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
                    MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), querySql, mtmvPartitionDefinition,
                            distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan);
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
                    MTMVPlanUtil.analyzeQuery(connectContext, Maps.newHashMap(), querySql, mtmvPartitionDefinition,
                            distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan);
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
                querySql, mtmvPartitionDefinition,
                distributionDescriptor, null, Maps.newHashMap(), Lists.newArrayList(), logicalPlan);
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
}
