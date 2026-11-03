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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.matcher.SPMAstCheckVisitor;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Round-58 regression tests for the reviewer's findings.
 *
 * #1 (SPMPlaceholderBuilder#254): two equal literals in DIFFERENT ON clauses of ONE query
 * block must get DISTINCT placeholder ids. The dedup key used to include the literal, its
 * direct parent, the child position, the block and the projection item - but no join or
 * expression occurrence - so two structurally identical ON predicates shared ONE id. A
 * caller that later varies those independent predicates separately (1/1 -> 2/3) still
 * passes the value-free digest pre-filter, but recordPlaceholderValue rejects the second
 * value for the shared id, so the saved baseline can never be used. The key now carries a
 * stable OCCURRENCE path (the expression root's ordinal in the block's walk order plus the
 * child-position path from that root down to the literal), which stays reproducible across
 * the separately parsed bind / plan trees.
 */
public class SPMRound58SafetyTest {

    /**
     * Two joins of ONE query block with structurally IDENTICAL ON predicates are independent
     * slots: distinct placeholder ids, and a user query that varies them separately matches
     * with one value per predicate.
     */
    @Test
    public void testSeparateJoinPredicatesGetDistinctPlaceholders() {
        String bindSql = "SELECT a.k FROM a JOIN b ON b.k = 1 JOIN c ON b.k = 1";
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(parse(bindSql), builder);

        List<LogicalJoin<?, ?>> bindJoins = joins(parameterized);
        Assertions.assertEquals(2, bindJoins.size(), "one JOIN per ON clause: " + bindJoins);
        // depth-first order: the OUTER join ((a JOIN b) JOIN c) first, then (a JOIN b)
        long outerId = singlePlaceholderId(predicates(bindJoins.get(0)).get(0));
        long innerId = singlePlaceholderId(predicates(bindJoins.get(1)).get(0));
        Assertions.assertNotEquals(innerId, outerId,
                "two structurally identical ON clauses of one block are independent slots:"
                        + " sharing one id made a query varying them separately unmatchable");
        Assertions.assertEquals(2, builder.getPlaceholderExprs().size(),
                "one placeholder record per occurrence: " + builder.getPlaceholderExprs());

        // the MATCHER: the user varies the two predicates separately (1/1 -> 2/3)
        LogicalPlan user = parse("SELECT a.k FROM a JOIN b ON b.k = 2 JOIN c ON b.k = 3");
        List<LogicalJoin<?, ?>> userJoins = joins(user);
        Assertions.assertEquals(2, userJoins.size(), "same shape on the user side");
        Map<Long, Expression> values = new HashMap<>();
        SPMAstCheckVisitor matcher = new SPMAstCheckVisitor();
        Assertions.assertTrue(matcher.checkExpression(predicates(bindJoins.get(1)).get(0),
                predicates(userJoins.get(1)).get(0), values), "the inner ON clause matches");
        Assertions.assertTrue(matcher.checkExpression(predicates(bindJoins.get(0)).get(0),
                predicates(userJoins.get(0)).get(0), values), "the outer ON clause matches");
        // the extracted value is coerced to the placeholder's (bind-side) type, so compare
        // its rendering rather than the literal class
        Assertions.assertEquals("2", values.get(innerId).toSql(),
                "the inner predicate extracts its OWN value");
        Assertions.assertEquals("3", values.get(outerId).toSql(),
                "the outer predicate extracts its OWN value");
    }

    /**
     * The occurrence identity must be REPRODUCIBLE: the bind tree and the (separately parsed)
     * plan tree of one baseline are parameterized by two walk runs, and the ids of
     * corresponding slots have to agree or the plan side introduces placeholders the bind
     * side never supplies (the CREATE alignment check).
     */
    @Test
    public void testJoinPredicateOccurrenceIdsAreReproducible() {
        String sql = "SELECT a.k FROM a JOIN b ON b.k = 1 JOIN c ON b.k = 3";
        List<Long> first = joinPredicateIds(sql);
        List<Long> second = joinPredicateIds(sql);
        Assertions.assertEquals(2, first.size(), "one placeholder per ON clause: " + first);
        Assertions.assertEquals(first, second,
                "the occurrence identity is deterministic: the same text must assign the"
                        + " same ids, or the plan tree could not reuse the bind ids");
        Assertions.assertEquals(2, new HashSet<>(first).size(),
                "the two occurrences keep their own id: " + first);
    }

    /** The ids of all join predicates of one parameterization, in join walk order. */
    private static List<Long> joinPredicateIds(String sql) {
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(parse(sql),
                new SPMPlaceholderBuilder());
        List<Long> ids = new ArrayList<>();
        for (LogicalJoin<?, ?> join : joins(parameterized)) {
            for (Expression predicate : predicates(join)) {
                ids.add(singlePlaceholderId(predicate));
            }
        }
        return ids;
    }

    private static LogicalPlan parse(String sql) {
        return new NereidsParser().parseSingle(sql);
    }

    /** All joins of the tree, in the depth-first order the parameterization walks. */
    private static List<LogicalJoin<?, ?>> joins(LogicalPlan plan) {
        List<LogicalJoin<?, ?>> joins = new ArrayList<>();
        collectJoins(plan, joins);
        return joins;
    }

    private static void collectJoins(Plan plan, List<LogicalJoin<?, ?>> joins) {
        if (plan instanceof LogicalJoin) {
            joins.add((LogicalJoin<?, ?>) plan);
        }
        for (Plan child : plan.children()) {
            collectJoins(child, joins);
        }
    }

    /** The ON predicates of one join (all conjunct kinds), in their own order. */
    private static List<Expression> predicates(LogicalJoin<?, ?> join) {
        List<Expression> predicates = new ArrayList<>(join.getHashJoinConjuncts());
        predicates.addAll(join.getMarkJoinConjuncts());
        predicates.addAll(join.getOtherJoinConjuncts());
        return predicates;
    }

    /** The ONE placeholder id of a predicate that carries exactly one literal. */
    private static long singlePlaceholderId(Expression predicate) {
        List<Long> ids = new ArrayList<>();
        collectPlaceholderIds(predicate, ids);
        Assertions.assertEquals(1, ids.size(), "one scalar per ON predicate: " + predicate);
        return ids.get(0);
    }

    private static void collectPlaceholderIds(Expression expression, List<Long> ids) {
        if (expression instanceof SpmConstVar) {
            ids.add(((SpmConstVar) expression).getId());
        }
        for (Expression child : expression.children()) {
            collectPlaceholderIds(child, ids);
        }
    }
}
