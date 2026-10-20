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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Optional;

/**
 * Tests the join gate of {@link IvmNormalizeCommonChecker}, which is the one gate the unique-key and
 * append-only detail models share with a per-model accepted set, and that the models reach it.
 */
class IvmNormalizeCommonCheckerTest extends IvmDeltaTestBase {

    private LogicalOlapScan buildDupScan(long tableId, String name) {
        OlapTable table = PlanConstructor.newOlapTable(tableId, name, 0, KeysType.DUP_KEYS);
        enableRowBinlog(table);
        table.setQualifiedDbName("test_db");
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, ImmutableList.of("test_db"));
    }

    private LogicalJoin<?, ?> crossJoin(LogicalOlapScan left, LogicalOlapScan right) {
        return new LogicalJoin<>(JoinType.CROSS_JOIN, left, right, JoinReorderContext.EMPTY);
    }

    private LogicalJoin<?, ?> semiJoin(LogicalOlapScan left, LogicalOlapScan right) {
        return new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN,
                ImmutableList.of(new EqualTo(left.getOutput().get(0), right.getOutput().get(0))),
                left, right, JoinReorderContext.EMPTY);
    }

    private LogicalJoin<?, ?> markJoin(JoinType joinType, LogicalOlapScan left, LogicalOlapScan right) {
        return new LogicalJoin<>(joinType,
                ImmutableList.of(), ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE),
                Optional.of(new MarkJoinSlotReference("$mark")),
                left, right, JoinReorderContext.EMPTY);
    }

    @Test
    void testAcceptedJoinTypePasses() {
        LogicalOlapScan left = buildDupScan(1, "a");
        LogicalOlapScan right = buildDupScan(2, "b");

        Assertions.assertDoesNotThrow(() -> IvmNormalizeCommonChecker.checkJoin(crossJoin(left, right),
                IvmNormalizeCommonChecker.DETAIL_JOIN_TYPES));
    }

    @Test
    void testUnacceptedJoinTypeIsRejected() {
        LogicalOlapScan left = buildDupScan(1, "a");
        LogicalOlapScan right = buildDupScan(2, "b");

        assertPlanPatternUnsupported(() -> IvmNormalizeCommonChecker.checkJoin(semiJoin(left, right),
                IvmNormalizeCommonChecker.DETAIL_JOIN_TYPES));
    }

    @Test
    void testMarkJoinOfAcceptedTypeIsRejected() {
        LogicalOlapScan left = buildDupScan(1, "a");
        LogicalOlapScan right = buildDupScan(2, "b");

        // The mark is orthogonal to the join type: a CROSS join can carry one, so a type test alone
        // accepts it, and the mark would let the join reach rows the MV already holds.
        assertPlanPatternUnsupported(() -> IvmNormalizeCommonChecker.checkJoin(
                markJoin(JoinType.CROSS_JOIN, left, right),
                IvmNormalizeCommonChecker.DETAIL_JOIN_TYPES));
        assertPlanPatternUnsupported(() -> IvmNormalizeCommonChecker.checkJoin(
                markJoin(JoinType.INNER_JOIN, left, right),
                IvmNormalizeCommonChecker.UNIQ_JOIN_TYPES));
    }

    @Test
    void testDetailNormalizeRejectsMarkJoin() {
        LogicalOlapScan left = buildDupScan(1, "a");
        LogicalOlapScan right = buildDupScan(2, "b");

        assertPlanPatternUnsupported(() -> new IvmNormalizeDupKeysMTMV().normalize(
                wrapForDupNormalize(markJoin(JoinType.CROSS_JOIN, left, right)), ImmutableSet.of()));
    }

    private Plan wrapForDupNormalize(Plan plan) {
        ImmutableList<NamedExpression> exprs = plan.getOutput().stream()
                .filter(slot -> !Column.ROW_LSN_COL.equals(slot.getName()))
                .filter(slot -> !(slot instanceof MarkJoinSlotReference))
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
        LogicalProject<?> project = new LogicalProject<>(exprs, plan);
        return new LogicalResultSink<>(exprs, project);
    }

    private void assertPlanPatternUnsupported(Executable executable) {
        IvmException exception = Assertions.assertThrows(IvmException.class, executable);
        Assertions.assertEquals(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, exception.getFailureReason());
    }
}
