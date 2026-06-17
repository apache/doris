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

import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTableWrapper;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.RowBinlogTableWrapper;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
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
import org.apache.doris.thrift.TBinlogScanType;
import org.apache.doris.thrift.TPaloScanRange;
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
        Config.enable_feature_binlog = true;

        createDatabase("test_stream");
        connectContext.setDatabase("test_stream");

        String createBaseTable = "create table test_stream.tbl_stream_base (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "unique key(k1)\n"
                + "partition by range(k1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\","
                + "\"enable_unique_key_merge_on_write\"=\"true\","
                + "\"binlog.enable\"=\"true\",\"binlog.format\"=\"ROW\","
                + "\"binlog.need_historical_value\"=\"true\")";
        createTable(createBaseTable);

        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        // Bump base table partition + replica versions to a value larger than the initial version
        // before creating s1 so that s1 populates historicalPartitionTSO (history path).
        bumpPartitionsAndReplicas(baseTable, 1001L);

        String createStream = "create stream if not exists test_stream.s1 on table test_stream.tbl_stream_base\n"
                + "properties('show_initial_rows' = 'true')";
        createTable(createStream);

        // Create another stream s2 without showing initial rows, then bump versions again so
        // s2 has incremental data (no historicalPartitionTSO).
        String createIncrementalStream =
                "create stream if not exists test_stream.s2 on table test_stream.tbl_stream_base\n"
                        + "properties('show_initial_rows' = 'false')";
        createTable(createIncrementalStream);
        bumpPartitionsAndReplicas(baseTable, 2002L);

        String createDuplicateBaseTable = "create table test_stream.tbl_dup_stream_base (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\","
                + "\"binlog.enable\"=\"true\",\"binlog.format\"=\"ROW\")";
        createTable(createDuplicateBaseTable);
        OlapTable duplicateBaseTable = (OlapTable) db.getTableOrMetaException("tbl_dup_stream_base");
        bumpPartitionsAndReplicas(duplicateBaseTable, 1001L);
        String createDuplicateInitialStream =
                "create stream if not exists test_stream.s_dup_initial on table test_stream.tbl_dup_stream_base\n"
                        + "properties('show_initial_rows' = 'true')";
        createTable(createDuplicateInitialStream);
        bumpPartitionsAndReplicas(duplicateBaseTable, 2002L);
        String createDuplicateStream =
                "create stream if not exists test_stream.s_dup on table test_stream.tbl_dup_stream_base\n"
                        + "properties('show_initial_rows' = 'false')";
        createTable(createDuplicateStream);
    }

    private static void bumpPartitionsAndReplicas(OlapTable table, long newVersion) {
        for (Partition partition : table.getPartitions()) {
            // Use the same timestamp value as both visibleVersionTime and tso to simulate a
            // transactional commit advancing partition.tso (the dedicated commit-tso field).
            // Without populating tso explicitly, non-transactional setters leave it as -1
            // which would make the stream consider the partition fully consumed.
            long ts = System.currentTimeMillis();
            partition.setVisibleVersionAndTime(newVersion, ts, ts);
            partition.setNextVersion(newVersion + 1);
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersion(newVersion);
                    }
                }
            }
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
        // history partition now projects STREAM_SEQ_COL from the base table commit-tso column
        Assertions.assertTrue(seqAlias.child() instanceof SlotReference);
        Assertions.assertEquals(Column.COMMIT_TSO_COL, ((SlotReference) seqAlias.child()).getName());

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
        // history partition now projects STREAM_SEQ_COL from the base table commit-tso column
        Assertions.assertTrue(seqAlias.child() instanceof SlotReference);
        Assertions.assertEquals(Column.COMMIT_TSO_COL, ((SlotReference) seqAlias.child()).getName());

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

        List<Long> expectedPartitionIds = new ArrayList<>(baseTable.getPartitionIds());
        List<Long> selectedPartitionIds = new ArrayList<>(scanNode.getSelectedPartitionIds());
        Assertions.assertEquals(expectedPartitionIds.size(), selectedPartitionIds.size());
        Assertions.assertTrue(selectedPartitionIds.containsAll(expectedPartitionIds));

        // The history-partition scan now reads the base table directly; scan range version
        // is the partition visible version (the legacy stream-tso override was removed).
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
            String expectedVersion = String.valueOf(baseTable.getPartition(partitionId).getVisibleVersion());
            String version = loc.getScanRange().getPaloScanRange().getVersion();
            Assertions.assertEquals(expectedVersion, version);
        }
    }

    @Test
    public void testIncrementalScanChangeTypeProjectionIsCaseExpr() throws Exception {
        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase("test_stream");
        ctx.getSessionVariable().showHiddenColumns = true;

        StatementScopeIdGenerator.clear();
        PlanFragment fragment = getFragment(ctx, "explain select * from test_stream.s2");
        PlanNode root = fragment.getPlanRoot();

        List<OlapScanNode> scanNodes = new ArrayList<>();
        collectOlapScanNodes(root, scanNodes);
        Assertions.assertFalse(scanNodes.isEmpty());

        boolean foundIncrementalCaseExpr = false;
        for (OlapScanNode scanNode : scanNodes) {
            List<Expr> projectList = scanNode.getProjectList();
            if (projectList == null || projectList.size() < 4) {
                continue;
            }
            Expr changeTypeExpr = projectList.get(3);
            if (changeTypeExpr instanceof CaseExpr) {
                foundIncrementalCaseExpr = true;
                break;
            }
        }
        Assertions.assertTrue(foundIncrementalCaseExpr,
                "incremental stream scan should keep change type projection as CaseExpr");
    }

    @Test
    public void testIncrementalScanRangeUsesPartitionOffsetAsStartTSO() throws Exception {
        // s2 was created with show_initial_rows=false, so partitionOffset is seeded to the
        // visibleVersionTime captured at creation time. After we bumped versions in
        // runBeforeAll, the current partition.visibleVersionTime is strictly greater. The
        // scan range should advertise a STREAM read source bounded by [partitionOffset,
        // partition.visibleVersionTime).
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s2");

        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase("test_stream");
        ctx.getSessionVariable().showHiddenColumns = true;
        StatementScopeIdGenerator.clear();
        PlanFragment fragment = getFragment(ctx, "explain select * from test_stream.s2");

        List<OlapScanNode> scanNodes = new ArrayList<>();
        collectOlapScanNodes(fragment.getPlanRoot(), scanNodes);
        Assertions.assertFalse(scanNodes.isEmpty());

        TBinlogScanType expectedScanType = BaseTableStream.StreamScanType.toThrift(stream.getStreamScanType());
        Map<Long, Long> tabletIdToPartitionId = new java.util.HashMap<>();
        for (Partition partition : baseTable.getPartitions()) {
            MaterializedIndex baseIndex = partition.getIndex(baseTable.getBaseIndexId());
            for (Tablet tablet : baseIndex.getTablets()) {
                tabletIdToPartitionId.put(tablet.getId(), partition.getId());
            }
        }

        boolean assertedAtLeastOne = false;
        for (OlapScanNode scanNode : scanNodes) {
            if (!(scanNode.getOlapTable() instanceof RowBinlogTableWrapper)) {
                continue;
            }
            List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(Long.MAX_VALUE);
            Assertions.assertFalse(locations.isEmpty());
            for (TScanRangeLocations loc : locations) {
                TPaloScanRange range = loc.getScanRange().getPaloScanRange();
                long tabletId = range.getTabletId();
                long pid = tabletIdToPartitionId.get(tabletId);
                long expectedStart = stream.getStreamUpdate(pid).first;
                Assertions.assertEquals(expectedScanType, range.getBinlogScanType(),
                        "binlog scan type should match stream consume type");
                Assertions.assertEquals(expectedStart, range.getStartTso(),
                        "startTSO should equal stream partitionOffset (last committed binlog TSO)");
                assertedAtLeastOne = true;
            }
        }
        Assertions.assertTrue(assertedAtLeastOne,
                "expected at least one incremental scan range to assert binlog TSO bounds against");
    }

    @Test
    public void testIncrementalScanStartTsoAdvancesAfterOffsetCommit() throws Exception {
        // Closed-loop validation: after the offset is advanced via unprotectedUpdateStreamUpdate,
        // a subsequent explain plan must use the new offset as the startTSO.
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTable baseTable = (OlapTable) db.getTableOrMetaException("tbl_stream_base");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s2");

        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase("test_stream");
        ctx.getSessionVariable().showHiddenColumns = true;

        // 1st explain: capture current scan node's stream update (prev/next per partition).
        StatementScopeIdGenerator.clear();
        PlanFragment fragment1 = getFragment(ctx, "explain select * from test_stream.s2");
        List<OlapScanNode> scanNodes1 = new ArrayList<>();
        collectOlapScanNodes(fragment1.getPlanRoot(), scanNodes1);
        OlapScanNode incrementalScan1 = null;
        for (OlapScanNode scanNode : scanNodes1) {
            if (scanNode.getOlapTable() instanceof RowBinlogTableWrapper) {
                incrementalScan1 = scanNode;
                break;
            }
        }
        Assertions.assertNotNull(incrementalScan1);
        OlapTableWrapper wrapper = (OlapTableWrapper) incrementalScan1.getOlapTable();
        Map<Long, Long> prevOffsets = new java.util.HashMap<>();
        Map<Long, Long> nextOffsets = new java.util.HashMap<>();
        for (Long pid : incrementalScan1.getSelectedPartitionIds()) {
            Pair<Long, Long> off = wrapper.getPartitionOffset(pid);
            if (off.first != null) {
                prevOffsets.put(pid, off.first);
            }
            nextOffsets.put(pid, off.second != null ? off.second : baseTable.getPartition(pid).getTso());
        }
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(prevOffsets, nextOffsets);
        Assertions.assertFalse(nextOffsets.isEmpty());

        // Commit the offsets.
        baseTable.writeLock();
        try {
            stream.unprotectedUpdateStreamUpdate(update, System.currentTimeMillis());
        } finally {
            baseTable.writeUnlock();
        }
        for (Map.Entry<Long, Long> entry : nextOffsets.entrySet()) {
            Assertions.assertEquals(entry.getValue(), stream.getStreamUpdate(entry.getKey()).first,
                    "partitionOffset should be advanced to committed next TSO");
        }

        // Simulate another BE-side commit by bumping partition versions further.
        bumpPartitionsAndReplicas(baseTable, 3003L);

        // 2nd explain: new startTSO should equal the previous explain's endTSO.
        StatementScopeIdGenerator.clear();
        PlanFragment fragment2 = getFragment(ctx, "explain select * from test_stream.s2");
        List<OlapScanNode> scanNodes2 = new ArrayList<>();
        collectOlapScanNodes(fragment2.getPlanRoot(), scanNodes2);

        Map<Long, Long> tabletIdToPartitionId = new java.util.HashMap<>();
        for (Partition partition : baseTable.getPartitions()) {
            MaterializedIndex baseIndex = partition.getIndex(baseTable.getBaseIndexId());
            for (Tablet tablet : baseIndex.getTablets()) {
                tabletIdToPartitionId.put(tablet.getId(), partition.getId());
            }
        }
        boolean assertedAtLeastOne = false;
        for (OlapScanNode scanNode : scanNodes2) {
            if (!(scanNode.getOlapTable() instanceof RowBinlogTableWrapper)) {
                continue;
            }
            List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(Long.MAX_VALUE);
            for (TScanRangeLocations loc : locations) {
                TPaloScanRange range = loc.getScanRange().getPaloScanRange();
                long pid = tabletIdToPartitionId.get(range.getTabletId());
                Assertions.assertEquals(nextOffsets.get(pid), range.getStartTso(),
                        "after offset commit, new startTSO must equal the previously committed next TSO");
                assertedAtLeastOne = true;
            }
        }
        Assertions.assertTrue(assertedAtLeastOne);
    }

    @Test
    public void testAppendOnlyStreamSnapshotCanBePlanned() throws Exception {
        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase("test_stream");
        ctx.getSessionVariable().showHiddenColumns = true;

        assertStreamScanCanBePlanned(ctx, "explain select * from test_stream.s_dup@snapshot()");
    }

    @Test
    public void testAppendOnlyStreamResetCanBePlanned() throws Exception {
        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase("test_stream");
        ctx.getSessionVariable().showHiddenColumns = true;

        assertStreamScanCanBePlanned(ctx, "explain select * from test_stream.s_dup@reset()");
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

    private void assertStreamScanCanBePlanned(ConnectContext ctx, String sql) {
        PlanFragment fragment = Assertions.assertDoesNotThrow(() -> getFragment(ctx, sql));

        List<OlapScanNode> scanNodes = new ArrayList<>();
        collectOlapScanNodes(fragment.getPlanRoot(), scanNodes);
        Assertions.assertFalse(scanNodes.isEmpty());
    }

    private PlanFragment getFragment(String sql) throws Exception {
        return getFragment(connectContext, sql);
    }

    private PlanFragment getFragment(ConnectContext ctx, String sql) throws Exception {
        StatementScopeIdGenerator.clear();
        StatementContext statementContext = MemoTestUtils.createStatementContext(ctx, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        LogicalPlan logicalPlan = (LogicalPlan) ((Explainable) (((ExplainCommand) parser.parseSingle(sql))
                .getLogicalPlan())).getExplainPlan(ctx);
        PhysicalPlan plan = planner.planWithLock(logicalPlan, PhysicalProperties.ANY);
        return new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext()))
                .translatePlan(plan);
    }
}
