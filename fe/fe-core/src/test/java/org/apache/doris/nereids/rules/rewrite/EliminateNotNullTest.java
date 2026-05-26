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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for EliminateNotNull rule.
 * Verifies that generated IS NOT NULL predicates are:
 * - Removed for fixed-length types (INT, BIGINT, etc.) when implied by other predicates
 * - Kept for variable-length types (STRING, VARCHAR, JSON, etc.) even when implied,
 *   because null bitmap check is cheaper than reading variable-length data
 */
class EliminateNotNullTest implements MemoPatternMatchSupported {
    // Table with nullable columns: id(INT, nullable), name(STRING, nullable)
    private static final LogicalOlapScan scan1 = newNullableScan(0, "t1");

    @BeforeAll
    static void makeSureSlotIdStable() {
        scan1.getOutput();
    }

    private static LogicalOlapScan newNullableScan(long tableId, String tableName) {
        List<Column> columns = ImmutableList.of(
                new Column("id", Type.INT, false, AggregateType.NONE, true, "0", ""),
                new Column("name", Type.STRING, false, AggregateType.NONE, true, "", ""));
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(3,
                ImmutableList.of(columns.get(0)));
        OlapTable table = new OlapTable(tableId, tableName, columns,
                KeysType.DUP_KEYS, new PartitionInfo(), hashDistributionInfo);
        table.setIndexMeta(-1, tableName, table.getFullSchema(), 0, 0, (short) 0,
                org.apache.doris.thrift.TStorageType.COLUMN, KeysType.DUP_KEYS);
        return new LogicalOlapScan(RelationId.createGenerator().getNextId(), table,
                ImmutableList.of("db"));
    }

    @Test
    void testRemoveGeneratedIsNotNullForFixedLengthType() {
        // id IS NOT NULL(generated) AND id > 1 → remove IS NOT NULL (INT is fixed-length)
        Slot idSlot = scan1.getOutput().get(0);
        Expression generatedIsNotNull = new Not(new IsNull(idSlot), true);
        Expression greaterThan = new GreaterThan(idSlot, new IntegerLiteral(1));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(ImmutableSet.of(greaterThan, generatedIsNotNull))
                .build();

        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext(), plan);
        checker.getCascadesContext().bottomUpRewrite(new EliminateNotNull());
        checker.matches(
                logicalFilter(logicalOlapScan())
                        .when(f -> f.getConjuncts().size() == 1)
                        .when(f -> f.getConjuncts().stream()
                                .allMatch(e -> e instanceof GreaterThan))
        );
    }

    @Test
    void testKeepGeneratedIsNotNullForVariableLengthType() {
        // name IS NOT NULL(generated) AND name > 'abc' → keep IS NOT NULL (STRING is variable-length)
        Slot nameSlot = scan1.getOutput().get(1);
        Expression generatedIsNotNull = new Not(new IsNull(nameSlot), true);
        Expression greaterThan = new GreaterThan(nameSlot, new StringLiteral("abc"));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(ImmutableSet.of(greaterThan, generatedIsNotNull))
                .build();

        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext(), plan);
        checker.getCascadesContext().bottomUpRewrite(new EliminateNotNull());
        checker.matches(
                logicalFilter(logicalOlapScan())
                        .when(f -> f.getConjuncts().size() == 2)
        );
    }

    @Test
    void testRemoveStandaloneGeneratedIsNotNullForFixedLengthType() {
        // id IS NOT NULL(generated) alone → removed (INT, no pre-filter value)
        Slot idSlot = scan1.getOutput().get(0);
        Expression generatedIsNotNull = new Not(new IsNull(idSlot), true);

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(ImmutableSet.of(generatedIsNotNull))
                .build();

        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext(), plan);
        checker.getCascadesContext().bottomUpRewrite(new EliminateNotNull());
        checker.matches(logicalOlapScan());
    }

    @Test
    void testKeepStandaloneGeneratedIsNotNullForVariableLengthType() {
        // name IS NOT NULL(generated) alone → kept (STRING, cheap null bitmap pre-filter)
        Slot nameSlot = scan1.getOutput().get(1);
        Expression generatedIsNotNull = new Not(new IsNull(nameSlot), true);

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(ImmutableSet.of(generatedIsNotNull))
                .build();

        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext(), plan);
        checker.getCascadesContext().bottomUpRewrite(new EliminateNotNull());
        checker.matches(
                logicalFilter(logicalOlapScan())
                        .when(f -> f.getConjuncts().size() == 1)
        );
    }

    @Test
    void testEndToEndInferAndEliminateForVariableLengthType() {
        // Full pipeline: name > 'abc' → InferFilterNotNull adds IS NOT NULL → EliminateNotNull keeps it
        Slot nameSlot = scan1.getOutput().get(1);
        Expression greaterThan = new GreaterThan(nameSlot, new StringLiteral("abc"));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(ImmutableSet.of(greaterThan))
                .build();

        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext(), plan);
        CascadesContext ctx = checker.getCascadesContext();
        ctx.topDownRewrite(new InferFilterNotNull());
        ctx.bottomUpRewrite(new EliminateNotNull());
        checker.matches(
                logicalFilter(logicalOlapScan())
                        .when(f -> f.getConjuncts().size() == 2)
        );
    }

    @Test
    void testEndToEndInferAndEliminateForFixedLengthType() {
        // Full pipeline: id > 1 → InferFilterNotNull adds IS NOT NULL → EliminateNotNull removes it
        Slot idSlot = scan1.getOutput().get(0);
        Expression greaterThan = new GreaterThan(idSlot, new IntegerLiteral(1));

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .filter(ImmutableSet.of(greaterThan))
                .build();

        PlanChecker checker = PlanChecker.from(MemoTestUtils.createConnectContext(), plan);
        CascadesContext ctx = checker.getCascadesContext();
        ctx.topDownRewrite(new InferFilterNotNull());
        ctx.bottomUpRewrite(new EliminateNotNull());
        checker.matches(
                logicalFilter(logicalOlapScan())
                        .when(f -> f.getConjuncts().size() == 1)
                        .when(f -> f.getConjuncts().stream()
                                .allMatch(e -> e instanceof GreaterThan))
        );
    }
}
