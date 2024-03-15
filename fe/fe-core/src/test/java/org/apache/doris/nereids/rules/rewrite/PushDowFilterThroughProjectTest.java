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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class PushDowFilterThroughProjectTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
            PlanConstructor.student,
            ImmutableList.of(""));

    @Test
    void pushDownFilterThroughProject() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(new Alias(new ExprId(1), scan.getOutput().get(0), "a")))
                .projectExprs(ImmutableList.of(new Alias(
                        new ExprId(1), new Alias(new ExprId(1), scan.getOutput().get(0), "a").toSlot(),
                        "b")))
                .filter(new IsNull((new Alias(
                        new ExprId(1), new Alias(new ExprId(1), scan.getOutput().get(0), "a").toSlot(),
                        "b")).toSlot()))
                .build();

        PlanChecker.from(context, plan)
                .applyTopDown(new PushDownFilterThroughProject())
                .matches(logicalFilter().when(f ->
                    f.getPredicate().toSql().equals("id IS NULL")
                ));
    }

    @Test
    void notPushDownFilterThroughWindow() {
        Expression window = new WindowExpression(new Rank(), ImmutableList.of(scan.getOutput().get(0)),
                ImmutableList.of(new OrderExpression(new OrderKey(scan.getOutput().get(0), true, true))));
        Alias alias = new Alias(new ExprId(1), window, "window");
        ConnectContext context = MemoTestUtils.createConnectContext();

        // filter -> limit -> project(windows)
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(alias))
                .filter(new IsNull(alias.toSlot()))
                .build();

        PlanChecker.from(context, plan)
                .applyTopDown(new PushDownFilterThroughProject())
                .matches(logicalFilter(logicalProject()));

        // filter -> limit -> project(windows)
        plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(alias))
                .limit(1)
                .filter(new IsNull(alias.toSlot()))
                .build();

        PlanChecker.from(context, plan)
                .applyTopDown(new PushDownFilterThroughProject())
                .matches(logicalFilter(logicalLimit(logicalProject())));
    }

    @Test
    void pushDownFilterThroughLimit() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        // filter -> limit -> project
        Alias alias = new Alias(new ExprId(1), scan.getOutput().get(0), "a");
        LogicalPlan plan = new LogicalPlanBuilder(scan)
                .projectExprs(ImmutableList.of(alias))
                .limit(1)
                .filter(new IsNull(alias.toSlot()))
                .build();

        PlanChecker.from(context, plan)
                .applyTopDown(new PushDownFilterThroughProject())
                .matches(logicalProject(logicalFilter(logicalLimit()).when(f ->
                        f.getPredicate().toSql().equals("id IS NULL"))));
    }
}
