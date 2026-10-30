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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SplitByString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SplitPart;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RewriteElementAtSplitToSplitPartTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.t_split(k int, s varchar(64), sep varchar(4), s2 varchar(64))"
                + " distributed by hash(k) properties('replication_num'='1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    private static boolean subtreeContains(Plan plan, Class<?> clazz) {
        for (Expression e : plan.getExpressions()) {
            if (containsExprOfType(e, clazz)) {
                return true;
            }
        }
        for (Plan child : plan.children()) {
            if (subtreeContains(child, clazz)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsExprOfType(Expression e, Class<?> clazz) {
        if (clazz.isInstance(e)) {
            return true;
        }
        for (Expression c : e.children()) {
            if (containsExprOfType(c, clazz)) {
                return true;
            }
        }
        return false;
    }

    private Plan rewrite(String sql) {
        return PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
    }

    @Test
    void testEqWithNonEmptyStringRewrites() {
        // Non-empty string RHS: rewrite is safe (NULL vs false both fail the predicate).
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, ','), 2) = 'a'");
        Assertions.assertTrue(subtreeContains(plan, SplitPart.class),
                "split_part must appear after rewrite");
        Assertions.assertFalse(subtreeContains(plan, ElementAt.class),
                "element_at must be gone after rewrite");
        Assertions.assertFalse(subtreeContains(plan, SplitByString.class),
                "split_by_string must be gone after rewrite");
    }

    @Test
    void testEqWithEmptyStringNotRewritten() {
        // Empty-string RHS: `NULL = ''` vs `'' = ''` differ (NULL vs true), so do NOT rewrite.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, ','), 2) = ''");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class),
                "empty-string RHS must be preserved");
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class),
                "split_part must not appear when RHS is empty string");
    }

    @Test
    void testEqWithColumnNotRewritten() {
        // Column RHS (may be NULL / empty): NULL vs '' semantics differ, do NOT rewrite.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, ','), 2) = s2");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class),
                "column RHS must be preserved");
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testInWithNonEmptyLiteralsRewrites() {
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, '/'), 3) in ('a', 'b', 'c')");
        Assertions.assertTrue(subtreeContains(plan, SplitPart.class));
        Assertions.assertFalse(subtreeContains(plan, ElementAt.class));
    }

    @Test
    void testInContainingEmptyStringNotRewritten() {
        // Presence of empty string in IN list changes NULL vs true semantics; keep as-is.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, ','), 2) in ('a', '', 'b')");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class));
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testNonLiteralIndexNotRewritten() {
        // split_part's third argument must bind to a literal int without extra cast; skip
        // when the index is a column reference.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, ','), k) = 'x'");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class));
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testProjectionNotRewritten() {
        // Rewrite is scoped to LogicalFilter; a bare projection with no WHERE keeps
        // element_at(split_by_string(...)) unchanged.
        Plan plan = rewrite("select element_at(split_by_string(s, ','), 2) from test.t_split");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class));
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testUnrelatedFilterUnchanged() {
        // Rule must not touch predicates that don't match its pattern.
        Plan plan = rewrite("select k from test.t_split where k = 1");
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
        Assertions.assertTrue(plan.anyMatch(p -> p instanceof LogicalFilter));
    }

    @Test
    void testTopLevelAndConjunctRewritten() {
        // A top-level AND is split into separate conjuncts by the filter, so the
        // matching conjunct's root '=' is rewritten while the other is left alone.
        Plan plan = rewrite("select k from test.t_split "
                + "where k > 0 and element_at(split_by_string(s, '.'), 1) = 'x'");
        Assertions.assertTrue(subtreeContains(plan, SplitPart.class));
        Assertions.assertFalse(subtreeContains(plan, ElementAt.class));
    }

    @Test
    void testNotNotRewritten() {
        // NOT is NULL-vs-false sensitive: for an input with no n-th part, the original
        // NOT(NULL = 'a') is NULL (drops the row) while NOT(split_part = 'a') would be
        // true (keeps it). The rule only touches a conjunct root, never inside NOT.
        Plan plan = rewrite("select k from test.t_split "
                + "where not (element_at(split_by_string(s, ','), 1) = 'a')");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class),
                "pattern under NOT must be preserved");
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testOrNotRewritten() {
        // An OR branch is not a top-level conjunct root; keep it unchanged to avoid
        // reasoning about three-valued logic inside compound predicates.
        Plan plan = rewrite("select k from test.t_split "
                + "where k = 1 or element_at(split_by_string(s, ','), 1) = 'a'");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class),
                "pattern under OR must be preserved");
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testEmptySeparatorNotRewritten() {
        // Empty separator: split_by_string splits into characters (element_at picks the
        // n-th char) while split_part('', ...) returns ''. Not equivalent; do NOT rewrite.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, ''), 2) = 'a'");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class),
                "empty separator must be preserved");
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testColumnSeparatorNotRewritten() {
        // A column separator could be empty at runtime; only a non-empty string literal
        // separator is safe to rewrite.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, sep), 2) = 'a'");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class),
                "column separator must be preserved");
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testNegativeIndexNotRewritten() {
        // split_part's negative index back-counts with rfind, which can diverge from
        // split_by_string's forward split; keep the original form to stay correct.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, ','), -1) = 'a'");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class),
                "negative index must be preserved");
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testNegativeIndexMultiCharSepNotRewritten() {
        // Concrete divergence: split_by_string('aaa','aa') -> ['', 'a'] so
        // element_at(-1) = 'a', but split_part('aaa','aa',-1) rfinds the overlapping
        // 'aa' at offset 1 and returns ''. Must NOT rewrite.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, 'aa'), -1) = 'a'");
        Assertions.assertTrue(subtreeContains(plan, ElementAt.class),
                "negative index with multi-char separator must be preserved");
        Assertions.assertFalse(subtreeContains(plan, SplitPart.class));
    }

    @Test
    void testZeroIndexRewritten() {
        // element_at(arr, 0) and split_part(s, sep, 0) are both unconditionally NULL, so
        // the rewrite is exact and additionally skips building the array.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, ','), 0) = 'a'");
        Assertions.assertTrue(subtreeContains(plan, SplitPart.class),
                "zero index must still be rewritten");
        Assertions.assertFalse(subtreeContains(plan, ElementAt.class));
    }

    @Test
    void testMultiCharSepPositiveIndexRewritten() {
        // Positive index with a multi-char separator: split_part counts parts forward
        // exactly like split_by_string, so the rewrite is valid.
        Plan plan = rewrite("select k from test.t_split "
                + "where element_at(split_by_string(s, '::'), 2) = 'a'");
        Assertions.assertTrue(subtreeContains(plan, SplitPart.class),
                "positive index with multi-char separator must be rewritten");
        Assertions.assertFalse(subtreeContains(plan, ElementAt.class));
    }
}
