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

package org.apache.doris.nereids.ssb;

import org.apache.doris.nereids.analyzer.NereidsAnalyzer;
import org.apache.doris.nereids.rules.rewrite.logical.ReorderJoin;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.PlanRewriter;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SSBJoinReorderTest extends SSBTestBase {
    @Test
    public void q4_1() {
        test(
                SSBUtils.Q4_1,
                ImmutableList.of(
                        "(lo_orderdate = d_datekey)",
                        "((lo_custkey = c_custkey) AND (c_region = 'AMERICA'))",
                        "((lo_suppkey = s_suppkey) AND (s_region = 'AMERICA'))",
                        "(lo_partkey = p_partkey)"
                ),
                ImmutableList.of("((p_mfgr = 'MFGR#1') OR (p_mfgr = 'MFGR#2'))")
        );
    }

    @Test
    public void q4_2() {
        test(
                SSBUtils.Q4_2,
                ImmutableList.of(
                        "(lo_orderdate = d_datekey)",
                        "((lo_custkey = c_custkey) AND (c_region = 'AMERICA'))",
                        "((lo_suppkey = s_suppkey) AND (s_region = 'AMERICA'))",
                        "(lo_partkey = p_partkey)"
                ),
                ImmutableList.of(
                        "(((d_year = 1997) OR (d_year = 1998)) AND ((p_mfgr = 'MFGR#1') OR (p_mfgr = 'MFGR#2')))")
        );
    }

    @Test
    public void q4_3() {
        test(
                SSBUtils.Q4_3,
                ImmutableList.of(
                        "(lo_orderdate = d_datekey)",
                        "(lo_custkey = c_custkey)",
                        "((lo_suppkey = s_suppkey) AND (s_nation = 'UNITED STATES'))",
                        "((lo_partkey = p_partkey) AND (p_category = 'MFGR#14'))"
                ),
                ImmutableList.of("((d_year = 1997) OR (d_year = 1998))")
        );
    }

    private void test(String sql, List<String> expectJoinConditions, List<String> expectFilterPredicates) {
        LogicalPlan analyzed = new NereidsAnalyzer(connectContext).analyze(sql);
        LogicalPlan plan = testJoinReorder(analyzed);
        new PlanChecker(expectJoinConditions, expectFilterPredicates).check(plan);
    }

    private LogicalPlan testJoinReorder(LogicalPlan plan) {
        return (LogicalPlan) PlanRewriter.topDownRewrite(plan, connectContext, new ReorderJoin());
    }

    private static class PlanChecker extends PlanVisitor<Void, Context> {
        private final List<LogicalRelation> joinInputs = new ArrayList<>();
        private final List<LogicalJoin> joins = new ArrayList<>();
        private final List<LogicalFilter> filters = new ArrayList<>();
        // TODO: it's tricky to compare expression by string, use a graceful manner to do this in the future.
        private final List<String> expectJoinConditions;
        private final List<String> expectFilterPredicates;

        public PlanChecker(List<String> expectJoinConditions, List<String> expectFilterPredicates) {
            this.expectJoinConditions = expectJoinConditions;
            this.expectFilterPredicates = expectFilterPredicates;
        }

        public void check(Plan plan) {
            plan.accept(this, new Context(null));

            // check join table orders
            Assertions.assertEquals(
                    ImmutableList.of("dates", "lineorder", "customer", "supplier", "part"),
                    joinInputs.stream().map(p -> p.getTable().getName()).collect(Collectors.toList()));

            // check join conditions
            List<String> actualJoinConditions = joins.stream().map(j -> {
                Optional<Expression> condition = j.getCondition();
                return condition.map(Expression::toSql).orElse("");
            }).collect(Collectors.toList());
            Assertions.assertEquals(expectJoinConditions, actualJoinConditions);

            // check filter predicates
            List<String> actualFilterPredicates = filters.stream()
                    .map(f -> f.getPredicates().toSql()).collect(Collectors.toList());
            Assertions.assertEquals(expectFilterPredicates, actualFilterPredicates);
        }

        @Override
        public Void visit(Plan plan, Context context) {
            for (Plan child : plan.children()) {
                child.accept(this, new Context(plan));
            }
            return null;
        }

        @Override
        public Void visitLogicalRelation(LogicalRelation relation, Context context) {
            if (context.parent instanceof LogicalJoin) {
                joinInputs.add(relation);
            }
            return null;
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<Plan> filter, Context context) {
            filters.add(filter);
            filter.child().accept(this, new Context(filter));
            return null;
        }

        @Override
        public Void visitLogicalJoin(LogicalJoin<Plan, Plan> join, Context context) {
            join.left().accept(this, new Context(join));
            join.right().accept(this, new Context(join));
            joins.add(join);
            return null;
        }
    }

    private static class Context {
        public final Plan parent;

        public Context(Plan parent) {
            this.parent = parent;
        }
    }
}
