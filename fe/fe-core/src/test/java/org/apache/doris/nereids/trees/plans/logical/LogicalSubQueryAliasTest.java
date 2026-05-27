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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

/**
 * Regression tests for Hive view column resolution fix.
 * Port of 2.1 commit 5bcb446510b.
 *
 * Bug: when a Hive view wraps a subquery whose slots carry a short
 * qualifier (e.g. ["b"] from a table alias), and the view itself has
 * a longer qualifier (e.g. ["hive","app","v_view"]), the original
 * computeOutputInternal() only handled two cases:
 *   1) originQualifier.size() >= qualifier.size()  → suffix replacement
 *   2) originQualifier.isEmpty()                    → add all
 * It fell through silently when originQualifier was non-empty but shorter,
 * leaving the wrong qualifier on the output slot. Downstream column
 * resolution then failed because it looked for the column under the
 * inner table alias instead of the view's fully-qualified name.
 *
 * Fix: add an else branch that clears and fully replaces the qualifier.
 * Also call .withIndexInSql(null) so synthetic subquery-alias outputs
 * don't carry stale SQL indices from the child plan.
 */
public class LogicalSubQueryAliasTest {

    // -----------------------------------------------------------------------
    // Branch 1: originQualifier.size() >= qualifier.size() (suffix replace)
    // -----------------------------------------------------------------------

    @Test
    public void testSuffixReplacementWhenOriginLonger() {
        SlotReference slot = new SlotReference(
                new ExprId(1), "col", StringType.INSTANCE, true,
                ImmutableList.of("catalog", "db", "tbl", "extra"));

        LogicalEmptyRelation child = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slot));

        LogicalSubQueryAlias<LogicalPlan> alias = new LogicalSubQueryAlias<>(
                ImmutableList.of("new_db", "new_tbl"), child);

        List<String> q = alias.computeOutput().get(0).getQualifier();
        Assertions.assertEquals(ImmutableList.of("catalog", "db", "new_db", "new_tbl"), q);
    }

    @Test
    public void testSuffixReplacementWhenSameLength() {
        SlotReference slot = new SlotReference(
                new ExprId(1), "col", StringType.INSTANCE, true,
                ImmutableList.of("db", "tbl"));

        LogicalEmptyRelation child = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slot));

        LogicalSubQueryAlias<LogicalPlan> alias = new LogicalSubQueryAlias<>(
                ImmutableList.of("new_db", "new_tbl"), child);

        List<String> q = alias.computeOutput().get(0).getQualifier();
        Assertions.assertEquals(ImmutableList.of("new_db", "new_tbl"), q);
    }

    // -----------------------------------------------------------------------
    // Branch 2: originQualifier.isEmpty() (add all)
    // -----------------------------------------------------------------------

    @Test
    public void testAddAllWhenOriginEmpty() {
        SlotReference slot = new SlotReference(
                new ExprId(1), "col", StringType.INSTANCE, true,
                ImmutableList.of());

        LogicalEmptyRelation child = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slot));

        LogicalSubQueryAlias<LogicalPlan> alias = new LogicalSubQueryAlias<>(
                ImmutableList.of("db", "tbl"), child);

        List<String> q = alias.computeOutput().get(0).getQualifier();
        Assertions.assertEquals(ImmutableList.of("db", "tbl"), q);
    }

    // -----------------------------------------------------------------------
    // Branch 3 (THE BUG): non-empty originQualifier shorter than qualifier
    // The else branch that was missing before the fix.
    // -----------------------------------------------------------------------

    @Test
    public void testFullReplacementWhenOriginShorter_HiveViewComputedColumn() {
        // Exact scenario from the bug: a Hive view has a computed column
        // like SELECT a+1 AS b FROM some_table t. The table alias "t"
        // produces a slot with qualifier ["t"]. The view wrapper adds
        // qualifier ["hive","app","v_view"]. Without the else branch,
        // the qualifier stays ["t"] and column resolution breaks.
        SlotReference slot = new SlotReference(
                new ExprId(1), "b", IntegerType.INSTANCE, true,
                ImmutableList.of("t"));

        LogicalEmptyRelation child = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slot));

        LogicalSubQueryAlias<LogicalPlan> viewAlias = new LogicalSubQueryAlias<>(
                ImmutableList.of("hive", "app", "v_view"), child);

        List<String> q = viewAlias.computeOutput().get(0).getQualifier();
        Assertions.assertEquals(ImmutableList.of("hive", "app", "v_view"), q,
                "Short qualifier must be fully replaced by the view qualifier");
    }

    @Test
    public void testFullReplacementWhenOriginShorter_TwoPartVsThreePart() {
        SlotReference slot = new SlotReference(
                new ExprId(1), "col", IntegerType.INSTANCE, true,
                ImmutableList.of("db", "tbl"));

        LogicalEmptyRelation child = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slot));

        LogicalSubQueryAlias<LogicalPlan> alias = new LogicalSubQueryAlias<>(
                ImmutableList.of("catalog", "db", "view_tbl"), child);

        List<String> q = alias.computeOutput().get(0).getQualifier();
        Assertions.assertEquals(ImmutableList.of("catalog", "db", "view_tbl"), q);
    }

    // -----------------------------------------------------------------------
    // withIndexInSql(null): the other half of the fix
    // -----------------------------------------------------------------------

    @Test
    public void testIndexInSqlClearedAfterAlias() {
        // Build a slot that has an indexInSql (simulating one from parsed SQL),
        // wrap it in LogicalSubQueryAlias, and verify the output has null indexInSql.
        SlotReference slot = new SlotReference(
                new ExprId(1), "col", StringType.INSTANCE, true,
                ImmutableList.of("db", "tbl"));
        SlotReference slotWithIndex = (SlotReference) slot.withIndexInSql(Pair.of(10, 20));

        Assertions.assertTrue(slotWithIndex.getIndexInSqlString().isPresent(),
                "Sanity: slot should have indexInSql before wrapping");

        LogicalEmptyRelation child = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slotWithIndex));

        LogicalSubQueryAlias<LogicalPlan> alias = new LogicalSubQueryAlias<>(
                ImmutableList.of("new_db", "new_tbl"), child);

        Slot output = alias.computeOutput().get(0);
        Assertions.assertFalse(output.getIndexInSqlString().isPresent(),
                "Subquery alias output must not inherit child's indexInSql");
    }

    // -----------------------------------------------------------------------
    // Nested LogicalSubQueryAlias: the real Hive view nesting scenario
    // -----------------------------------------------------------------------

    @Test
    public void testNestedViewQualifiers() {
        // Simulates:
        //   CREATE VIEW inner_v AS SELECT * FROM t;    -- inner alias: ["t"] → ["db","inner_v"]
        //   CREATE VIEW outer_v AS SELECT * FROM inner_v; -- outer alias: ["db","inner_v"] → ["hive","app","outer_v"]
        // The outer alias triggers the else branch because ["db","inner_v"] (2 parts)
        // is shorter than ["hive","app","outer_v"] (3 parts).
        SlotReference slot = new SlotReference(
                new ExprId(1), "b", IntegerType.INSTANCE, true,
                ImmutableList.of("t"));

        LogicalEmptyRelation leaf = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slot));

        LogicalSubQueryAlias<LogicalPlan> innerView = new LogicalSubQueryAlias<>(
                ImmutableList.of("db", "inner_v"), leaf);

        Assertions.assertEquals(ImmutableList.of("db", "inner_v"),
                innerView.computeOutput().get(0).getQualifier());

        LogicalSubQueryAlias<LogicalPlan> outerView = new LogicalSubQueryAlias<>(
                ImmutableList.of("hive", "app", "outer_v"), innerView);

        List<String> q = outerView.computeOutput().get(0).getQualifier();
        Assertions.assertEquals(ImmutableList.of("hive", "app", "outer_v"), q,
                "Nested view must fully replace inner view's qualifier");
    }

    @Test
    public void testTripleNestedViewQualifiers() {
        SlotReference slot = new SlotReference(
                new ExprId(1), "x", IntegerType.INSTANCE, true,
                ImmutableList.of());

        LogicalEmptyRelation leaf = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slot));

        LogicalSubQueryAlias<LogicalPlan> alias1 = new LogicalSubQueryAlias<>(
                ImmutableList.of("a"), leaf);

        LogicalSubQueryAlias<LogicalPlan> alias2 = new LogicalSubQueryAlias<>(
                ImmutableList.of("b", "c"), alias1);

        LogicalSubQueryAlias<LogicalPlan> alias3 = new LogicalSubQueryAlias<>(
                ImmutableList.of("d", "e", "f"), alias2);

        List<String> q = alias3.computeOutput().get(0).getQualifier();
        Assertions.assertEquals(ImmutableList.of("d", "e", "f"), q);
    }

    // -----------------------------------------------------------------------
    // Multiple slots with mixed qualifier lengths (computed + regular cols)
    // -----------------------------------------------------------------------

    @Test
    public void testMixedSlotQualifiersInSingleAlias() {
        // A Hive view with both computed columns (empty qualifier) and
        // direct column references (table alias qualifier):
        //   CREATE VIEW v AS SELECT a+1 AS computed, t.plain FROM t;
        SlotReference computedSlot = new SlotReference(
                new ExprId(1), "computed", IntegerType.INSTANCE, true,
                ImmutableList.of());

        SlotReference plainSlot = new SlotReference(
                new ExprId(2), "plain", StringType.INSTANCE, true,
                ImmutableList.of("t"));

        LogicalEmptyRelation child = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(),
                ImmutableList.of(computedSlot, plainSlot));

        LogicalSubQueryAlias<LogicalPlan> alias = new LogicalSubQueryAlias<>(
                ImmutableList.of("hive", "app", "v"), child);

        List<Slot> output = alias.computeOutput();
        Assertions.assertEquals(2, output.size());

        List<String> q1 = output.get(0).getQualifier();
        Assertions.assertEquals(ImmutableList.of("hive", "app", "v"), q1,
                "Empty qualifier → add all");

        List<String> q2 = output.get(1).getQualifier();
        Assertions.assertEquals(ImmutableList.of("hive", "app", "v"), q2,
                "Short qualifier → full replacement (else branch)");

        Assertions.assertEquals("computed", output.get(0).getName());
        Assertions.assertEquals("plain", output.get(1).getName());
    }

    // -----------------------------------------------------------------------
    // Column alias interaction with qualifier replacement
    // -----------------------------------------------------------------------

    @Test
    public void testColumnAliasWithShortQualifier() {
        SlotReference slot = new SlotReference(
                new ExprId(1), "original_name", StringType.INSTANCE, true,
                ImmutableList.of("t"));

        LogicalEmptyRelation child = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slot));

        LogicalSubQueryAlias<LogicalPlan> alias = new LogicalSubQueryAlias<>(
                ImmutableList.of("hive", "app", "v_view"),
                Optional.of(ImmutableList.of("renamed_col")),
                Optional.empty(), Optional.empty(), child);

        Slot output = alias.computeOutput().get(0);
        Assertions.assertEquals("renamed_col", output.getName());
        Assertions.assertEquals(ImmutableList.of("hive", "app", "v_view"),
                output.getQualifier());
    }

    // -----------------------------------------------------------------------
    // Bug reproduction: the exact error that would occur without the fix
    // -----------------------------------------------------------------------

    @Test
    public void testBugReproduction_WrongQualifierBreaksColumnResolution() {
        // Without the else branch, a slot with qualifier ["t"] wrapped by
        // alias ["hive","app","v"] would keep qualifier ["t"]. Any
        // downstream code trying to resolve "hive.app.v.b" would fail
        // because the slot carries qualifier ["t"] instead.
        SlotReference slot = new SlotReference(
                new ExprId(42), "b", IntegerType.INSTANCE, true,
                ImmutableList.of("t"));

        LogicalEmptyRelation child = new LogicalEmptyRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(slot));

        LogicalSubQueryAlias<LogicalPlan> viewAlias = new LogicalSubQueryAlias<>(
                ImmutableList.of("hive", "app", "v"), child);

        Slot result = viewAlias.computeOutput().get(0);

        // Without the fix, this would be ["t"] — the bug
        Assertions.assertNotEquals(ImmutableList.of("t"), result.getQualifier(),
                "Qualifier must NOT remain as the inner table alias");

        // With the fix, it's the full view qualifier
        Assertions.assertEquals(ImmutableList.of("hive", "app", "v"), result.getQualifier());

        // Name is preserved
        Assertions.assertEquals("b", result.getName());

        // indexInSql must be null
        Assertions.assertFalse(result.getIndexInSqlString().isPresent());
    }
}
