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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests the delta plan of an append-only detail IVM MV: the rows its refresh appends to the MV.
 *
 * <p>The normalized query of that model is the query itself -- the normalize for it only checks that no
 * operator of the query would have to correct a row the MV holds -- so these tests build the delta over
 * the query directly, which is exactly the plan the refresh reaches the rewriter with.
 */
class IvmDupDeltaRewriterTest extends IvmDeltaTestBase {
    /** The hidden slots a unique-key MV's delta carries. A detail MV's delta has no place for any. */
    private static final Set<String> DELTA_METADATA_SLOTS = ImmutableSet.of(
            Column.IVM_DML_FACTOR_COL, Column.SEQUENCE_COL, Column.DELETE_SIGN, Column.IVM_ROW_ID_COL);

    @Test
    void testProjectOverFilterReadsTheAppendedRows() {
        LogicalOlapScan scan = buildScanForTable(701, "dup_t1");
        Plan query = buildScanPlan(scan).child();
        MTMV mtmv = dupMtmvWithPendingRows(query, 701_001L, ImmutableList.of(scan.getTable()));

        Plan delta = generateDelta(query, mtmv);

        Assertions.assertEquals(1, collect(delta, LogicalOlapTableStreamScan.class).stream()
                .filter(LogicalOlapTableStreamScan::isIncremental).count());
        assertNoDeltaMetadataSlots(delta);
        Assertions.assertEquals(mtmv.getInsertedColumnNames(), outputNames(delta));
    }

    @Test
    void testInnerJoinContributesOneArmPerSideThatAppended() {
        LogicalOlapScan leftScan = buildScanForTable(702, "dup_left");
        LogicalOlapScan rightScan = buildScanForTable(703, "dup_right");
        Plan query = buildInnerJoin(leftScan, rightScan);
        MTMV mtmv = dupMtmvWithPendingRows(query, 702_001L,
                ImmutableList.of(leftScan.getTable(), rightScan.getTable()));

        Plan delta = generateDelta(query, mtmv);

        // Both sides appended, so the delta is the two arms an inner join expands into: the left delta
        // joined against the right's pre-refresh snapshot, and the right delta against the left's
        // post-refresh snapshot. Their union also carries the rows both sides appended in the window.
        Assertions.assertEquals(2, incrementalScans(delta).size());
        List<LogicalUnion> unions = collect(delta, LogicalUnion.class);
        Assertions.assertEquals(1, unions.size());
        Assertions.assertEquals(2, unions.get(0).children().size());
        assertNoDeltaMetadataSlots(delta);
        Assertions.assertEquals(mtmv.getInsertedColumnNames(), outputNames(delta));
    }

    @Test
    void testInnerJoinWithOneSidedAppendContributesOneArm() {
        LogicalOlapScan leftScan = buildScanForTable(704, "dup_left");
        LogicalOlapScan rightScan = buildScanForTable(705, "dup_right");
        Plan query = buildInnerJoin(leftScan, rightScan);
        MTMV mtmv = dupMtmvWithPendingRows(query, 704_001L,
                ImmutableList.of(leftScan.getTable(), rightScan.getTable()));
        // The right table's stream is up to date, so nothing it holds reaches the delta.
        advanceStreamToTable(rightScan.getTable(), mtmv);

        Plan delta = generateDelta(query, mtmv);

        Assertions.assertEquals(1, incrementalScans(delta).size());
        Assertions.assertTrue(collect(delta, LogicalUnion.class).isEmpty());
        assertNoDeltaMetadataSlots(delta);
        Assertions.assertEquals(mtmv.getInsertedColumnNames(), outputNames(delta));
    }

    @Test
    void testUnionAllCombinesTheArmsThatAppended() {
        LogicalOlapScan firstScan = buildScanForTable(706, "dup_u1");
        LogicalOlapScan secondScan = buildScanForTable(707, "dup_u2");
        Plan query = buildUnionAll(firstScan, secondScan);
        MTMV mtmv = dupMtmvWithPendingRows(query, 706_001L,
                ImmutableList.of(firstScan.getTable(), secondScan.getTable()));

        Plan delta = generateDelta(query, mtmv);

        Assertions.assertEquals(2, incrementalScans(delta).size());
        List<LogicalUnion> unions = collect(delta, LogicalUnion.class);
        Assertions.assertEquals(1, unions.size());
        Assertions.assertEquals(2, unions.get(0).children().size());
        assertNoDeltaMetadataSlots(delta);
        Assertions.assertEquals(mtmv.getInsertedColumnNames(), outputNames(delta));
    }

    @Test
    void testExcludedTriggerTableContributesNoDeltaOfItsOwn() {
        LogicalOlapScan leftScan = buildScanForTable(708, "dup_ex_left");
        LogicalOlapScan rightScan = buildScanForTable(709, "dup_ex_right");
        Plan query = buildInnerJoin(leftScan, rightScan);
        MTMV mtmv = dupMtmvWithPendingRows(query, 708_001L,
                ImmutableList.of(leftScan.getTable(), rightScan.getTable()));
        mtmv.setMvProperties(ImmutableMap.of(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES,
                "test_db." + rightScan.getTable().getName()));

        Plan delta = generateDelta(query, mtmv);

        // The excluded table still supplies the join's other side as a snapshot, but no delta of its own.
        Assertions.assertEquals(1, incrementalScans(delta).size());
        assertNoDeltaMetadataSlots(delta);
        Assertions.assertEquals(mtmv.getInsertedColumnNames(), outputNames(delta));
    }

    @Test
    void testNoPendingRowsProducesAnEmptyDelta() {
        LogicalOlapScan scan = buildScanForTable(710, "dup_empty");
        Plan query = buildScanPlan(scan).child();
        MTMV mtmv = dupMtmvWithPendingRows(query, 710_001L, ImmutableList.of(scan.getTable()));
        advanceStreamToTable(scan.getTable(), mtmv);

        Assertions.assertInstanceOf(LogicalEmptyRelation.class, generateDelta(query, mtmv));
    }

    @Test
    void testAggregateAndOuterJoinAreRejected() {
        LogicalOlapScan scan = buildScanForTable(711, "dup_reject");
        Plan query = buildScanPlan(scan).child();
        MTMV mtmv = dupMtmvWithPendingRows(query, 711_001L, ImmutableList.of(scan.getTable()));

        // Both shapes would have to correct a row the MV already holds, which is why the normalize of
        // this model rejects them before any refresh runs.
        Assertions.assertThrows(IvmException.class, () -> generateDelta(buildGroupedAgg(scan), mtmv));
        Assertions.assertThrows(IvmException.class, () -> generateDelta(buildOuterJoin(scan, scan), mtmv));
    }

    @Test
    void testMarkJoinIsRejected() {
        LogicalOlapScan scan = buildScanForTable(712, "dup_mark_reject");
        Plan query = buildScanPlan(scan).child();
        MTMV mtmv = dupMtmvWithPendingRows(query, 712_001L, ImmutableList.of(scan.getTable()));

        // A mark join's mark can drop a row the MV already holds, so a refresh cannot express it by
        // appending, and the shared join gate the detail normalize uses rejects it here as well.
        Assertions.assertThrows(IvmException.class, () -> generateDelta(buildMarkJoin(scan, scan), mtmv));
    }

    private Plan generateDelta(Plan query, MTMV mtmv) {
        ConnectContext connectContext = newConnectContext();
        IvmRewriteResult rewriteResult = new IvmRewriteResult();
        rewriteResult.setKeysType(KeysType.DUP_KEYS);
        return new IvmDeltaRewriter().generateIncrRefreshPlan(query, rewriteResult,
                IvmRewriteContext.incremental(mtmv), connectContext);
    }

    /**
     * Builds the MV over {@code query}'s output and leaves every base table with rows its stream has not
     * read yet, which is the state a refresh runs in when it has something to append. A stream is named
     * after the MV and its base table, so each test takes an id of its own: base table names repeat.
     */
    private MTMV dupMtmvWithPendingRows(Plan query, long mvId, List<OlapTable> baseTables) {
        MTMV mtmv = buildDupMtmvFromPlan(query.getOutput());
        mtmv.setId(mvId);
        for (OlapTable table : baseTables) {
            registerTestStream(table, mtmv.getId());
            bumpBaseTableTso(table, 20);
            setStreamOffset(table, getRegisteredStream(table, mtmv.getId()), 10);
        }
        return mtmv;
    }

    /** Leaves the table's stream up to date, so that table contributes no delta of its own. */
    private void advanceStreamToTable(OlapTable table, MTMV mtmv) {
        advanceStreamToBaseTable(table, getRegisteredStream(table, mtmv.getId()));
    }

    private Plan buildInnerJoin(LogicalOlapScan leftScan, LogicalOlapScan rightScan) {
        LogicalProject<LogicalOlapScan> leftArm = new LogicalProject<>(ImmutableList.of(
                new Alias(leftScan.getOutput().get(0), "l_id"),
                new Alias(leftScan.getOutput().get(1), "l_name")), leftScan);
        LogicalProject<LogicalOlapScan> rightArm = new LogicalProject<>(ImmutableList.of(
                new Alias(rightScan.getOutput().get(0), "r_id"),
                new Alias(rightScan.getOutput().get(1), "r_name")), rightScan);
        return new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(leftArm.getOutput().get(0), rightArm.getOutput().get(0))),
                leftArm, rightArm, JoinReorderContext.EMPTY);
    }

    private Plan buildOuterJoin(LogicalOlapScan leftScan, LogicalOlapScan rightScan) {
        LogicalProject<LogicalOlapScan> leftArm = new LogicalProject<>(
                ImmutableList.copyOf(leftScan.getOutput()), leftScan);
        LogicalProject<LogicalOlapScan> rightArm = new LogicalProject<>(ImmutableList.of(
                new Alias(rightScan.getOutput().get(0), "r_id")), rightScan);
        return new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(leftArm.getOutput().get(0), rightArm.getOutput().get(0))),
                leftArm, rightArm, JoinReorderContext.EMPTY);
    }

    private Plan buildMarkJoin(LogicalOlapScan leftScan, LogicalOlapScan rightScan) {
        LogicalProject<LogicalOlapScan> leftArm = new LogicalProject<>(
                ImmutableList.copyOf(leftScan.getOutput()), leftScan);
        LogicalProject<LogicalOlapScan> rightArm = new LogicalProject<>(
                ImmutableList.copyOf(rightScan.getOutput()), rightScan);
        return new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE),
                Optional.of(new MarkJoinSlotReference("$mark")),
                leftArm, rightArm, JoinReorderContext.EMPTY);
    }

    private Plan buildUnionAll(LogicalOlapScan firstScan, LogicalOlapScan secondScan) {
        Plan firstArm = buildScanPlan(firstScan).child();
        Plan secondArm = new LogicalProject<>(ImmutableList.of(
                new Alias(secondScan.getOutput().get(0), firstScan.getOutput().get(0).getName()),
                new Alias(secondScan.getOutput().get(1), firstScan.getOutput().get(1).getName())), secondScan);
        return IvmDeltaRewriteHelper.INSTANCE.buildUnionAll(ImmutableList.of(firstArm, secondArm));
    }

    private static List<LogicalOlapTableStreamScan> incrementalScans(Plan plan) {
        return collect(plan, LogicalOlapTableStreamScan.class).stream()
                .filter(LogicalOlapTableStreamScan::isIncremental)
                .collect(Collectors.toList());
    }

    private static <T extends Plan> List<T> collect(Plan plan, Class<T> nodeClass) {
        List<T> nodes = new ArrayList<>();
        plan.foreach(node -> {
            if (nodeClass.isInstance(node)) {
                nodes.add(nodeClass.cast(node));
            }
        });
        return nodes;
    }

    private static List<String> outputNames(Plan plan) {
        return plan.getOutput().stream().map(Slot::getName).collect(Collectors.toList());
    }

    /**
     * No node of the delta may carry a slot of the unique-key delta's layout: a detail MV's delta has no
     * sign to encode, no sequence to allocate and no row of the MV to match.
     */
    private static void assertNoDeltaMetadataSlots(Plan plan) {
        for (Plan node : collect(plan, Plan.class)) {
            for (Slot slot : node.getOutput()) {
                Assertions.assertFalse(DELTA_METADATA_SLOTS.contains(slot.getName())
                                || IvmUtil.isIvmHiddenColumn(slot.getName()),
                        "delta plan carries the hidden slot " + slot.getName()
                                + " of " + node.getClass().getSimpleName());
            }
        }
    }
}
