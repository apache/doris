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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * UTs for table stream query plan, including
 * virtual column alias injection and partition id/version.
 */
public class ExplainTableStreamPlanTest extends TestWithFeService {

    private final NereidsParser parser = new NereidsParser();

    @Override
    public void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;

        createDatabase("test_stream");
        connectContext.setDatabase("test_stream");

        String createBaseTable = "create table test_stream.tbl_stream_base (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "partition by range(k1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")";
        createTable(createBaseTable);

        String createStream = "create stream if not exists test_stream.s1 on table test_stream.tbl_stream_base\n"
                + "properties('type' = 'default', 'show_initial_rows' = 'true')";
        createTable(createStream);

        // Make base table visible versions differ from stream offsets, so we can verify
        // scan range version uses stream partitionOffset rather than partition visibleVersion.
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        for (Partition partition : baseTable.getPartitions()) {
            partition.setVisibleVersionAndTime(partition.getVisibleVersion() + 1000,
                    System.currentTimeMillis());
        }
    }

    @Test
    public void testVirtualColumnAliasInjection1() throws Exception {
        String sql = "select __DORIS_STREAM_SEQUENCE_COL__, __DORIS_STREAM_CHANGE_TYPE_COL__ from test_stream.s1";

        Plan rewritePlan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getCascadesContext()
                .getRewritePlan();

        LogicalProject<?> project = findFirstLogicalProject(rewritePlan);
        Assertions.assertNotNull(project);
        List<NamedExpression> projects = project.getProjects();
        Assertions.assertEquals(2, projects.size());

        Alias seqAlias = (Alias) projects.get(0);
        Assertions.assertEquals(Column.STREAM_SEQ_COL, seqAlias.getName());
        Assertions.assertTrue(seqAlias.child() instanceof BigIntLiteral);
        Assertions.assertEquals(Long.valueOf(-1L), ((BigIntLiteral) seqAlias.child()).getValue());

        Alias changeTypeAlias = (Alias) projects.get(1);
        Assertions.assertEquals(Column.STREAM_CHANGE_TYPE_COL, changeTypeAlias.getName());
        Assertions.assertTrue(changeTypeAlias.child() instanceof VarcharLiteral);
        Assertions.assertEquals("APPEND", ((VarcharLiteral) changeTypeAlias.child()).getValue());
    }

    @Test
    public void testVirtualColumnAliasInjection2() throws Exception {
        // show hidden columns
        String sql = "select * from test_stream.s1";
        ConnectContext ctx = createDefaultCtx();
        connectContext.getSessionVariable().feDebug = true;
        ctx.getSessionVariable().showHiddenColumns = true;
        Plan rewritePlan = PlanChecker.from(ctx)
                .analyze(sql)
                .rewrite()
                .getCascadesContext()
                .getRewritePlan();

        LogicalProject<?> project = findFirstLogicalProject(rewritePlan);
        Assertions.assertNotNull(project);
        List<NamedExpression> projects = project.getProjects();
        Assertions.assertEquals(4, projects.size());

        Assertions.assertEquals("k1", projects.get(0).getName());
        Assertions.assertEquals("k2", projects.get(1).getName());
        Alias seqAlias = (Alias) projects.get(2);
        Assertions.assertEquals(Column.STREAM_SEQ_COL, seqAlias.getName());
        Assertions.assertTrue(seqAlias.child() instanceof BigIntLiteral);
        Assertions.assertEquals(Long.valueOf(-1L), ((BigIntLiteral) seqAlias.child()).getValue());

        Alias changeTypeAlias = (Alias) projects.get(3);
        Assertions.assertEquals(Column.STREAM_CHANGE_TYPE_COL, changeTypeAlias.getName());
        Assertions.assertTrue(changeTypeAlias.child() instanceof VarcharLiteral);
        Assertions.assertEquals("APPEND", ((VarcharLiteral) changeTypeAlias.child()).getValue());
    }

    @Test
    public void testVirtualColumnAliasFilterInjection() throws Exception {
        // test filter
        PlanChecker.from(connectContext)
                .checkPlannerResult("select * from test_stream.s1 where __DORIS_STREAM_SEQUENCE_COL__ = 1");

        PlanChecker.from(connectContext)
                .checkPlannerResult("select * from test_stream.s1 where __DORIS_STREAM_CHANGE_TYPE_COL__ = \"APPEND\"");
    }

    @Test
    public void testVirtualColumnAliasAggregateInjection() throws Exception {
        // test group by
        PlanChecker.from(connectContext)
                .checkPlannerResult("select __DORIS_STREAM_SEQUENCE_COL__, count(*) from test_stream.s1 "
                        + "group by __DORIS_STREAM_SEQUENCE_COL__");
        PlanChecker.from(connectContext)
                .checkPlannerResult("select  __DORIS_STREAM_CHANGE_TYPE_COL__, count(*) from test_stream.s1 "
                        + "group by __DORIS_STREAM_CHANGE_TYPE_COL__");
    }

    @Test
    public void testPartitionSpecify() throws Exception {
        String sql = "explain select * from test_stream.s1 partition p1";
        PlanFragment fragment = getFragment(sql);
        PlanNode root = fragment.getPlanRoot();

        List<OlapScanNode> scanNodes = new ArrayList<>();
        collectOlapScanNodes(root, scanNodes);
        Assertions.assertFalse(scanNodes.isEmpty());
        OlapScanNode scanNode = scanNodes.get(0);
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        Assertions.assertEquals(baseTable.getPartition("p1").getId(),
                new ArrayList<>(scanNode.getSelectedPartitionIds()).get(0));
    }

    @Test
    public void testPartitionOffsetAndScanRangeVersion() throws Exception {
        String sql = "explain select * from test_stream.s1";
        PlanFragment fragment = getFragment(sql);
        PlanNode root = fragment.getPlanRoot();

        List<OlapScanNode> scanNodes = new ArrayList<>();
        collectOlapScanNodes(root, scanNodes);
        Assertions.assertFalse(scanNodes.isEmpty());
        OlapScanNode scanNode = scanNodes.get(0);

        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTableStream stream =
                (OlapTableStream) db.getTableOrMetaException("s1");

        List<Long> expectedPartitionIds = new ArrayList<>(baseTable.getPartitionIds());
        List<Long> selectedPartitionIds = new ArrayList<>(scanNode.getSelectedPartitionIds());
        Assertions.assertEquals(expectedPartitionIds.size(), selectedPartitionIds.size());
        Assertions.assertTrue(selectedPartitionIds.containsAll(expectedPartitionIds));

        Map<Long, Long> actualOffset = scanNode.getStreamUpdate().getNext();
        Assertions.assertNotNull(actualOffset);
        for (Long pid : expectedPartitionIds) {
            Assertions.assertEquals(stream.getStreamUpdate(pid).second, actualOffset.get(pid));
        }

        Map<Long, Long> tabletIdToPartitionId = new java.util.HashMap<>();
        for (Partition partition : baseTable.getPartitions()) {
            MaterializedIndex baseIndex = partition.getIndex(baseTable.getBaseIndexId());
            for (Tablet tablet : baseIndex.getTablets()) {
                tabletIdToPartitionId.put(tablet.getId(), partition.getId());
            }
        }

        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(Long.MAX_VALUE);
        Assertions.assertFalse(locations.isEmpty());
        for (TScanRangeLocations loc : locations) {
            long tabletId = loc.getScanRange().getPaloScanRange().getTabletId();
            long partitionId = tabletIdToPartitionId.get(tabletId);
            String expectedVersion = String.valueOf(stream.getStreamUpdate(partitionId).second);
            String version = loc.getScanRange().getPaloScanRange().getVersion();
            Assertions.assertEquals(expectedVersion, version);
        }
    }

    private void collectOlapScanNodes(PlanNode node, List<OlapScanNode> result) {
        if (node instanceof OlapScanNode) {
            result.add((OlapScanNode) node);
        }
        for (PlanNode child : node.getChildren()) {
            collectOlapScanNodes(child, result);
        }
    }

    private LogicalProject<?> findFirstLogicalProject(Plan plan) {
        if (plan instanceof LogicalProject) {
            return (LogicalProject<?>) plan;
        }
        for (Plan child : plan.children()) {
            LogicalProject<?> found = findFirstLogicalProject(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private PlanFragment getFragment(String sql) throws Exception {
        StatementScopeIdGenerator.clear();
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        LogicalPlan logicalPlan = (LogicalPlan) ((Explainable) (((ExplainCommand) parser.parseSingle(sql))
                .getLogicalPlan())).getExplainPlan(connectContext);
        PhysicalPlan plan = planner.planWithLock(logicalPlan, PhysicalProperties.ANY);
        return new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext()))
                .translatePlan(plan);
    }
}
