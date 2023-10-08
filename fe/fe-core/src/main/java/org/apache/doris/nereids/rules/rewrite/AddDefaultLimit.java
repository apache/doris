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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;

/**
 * add limit node to the top of the plan tree if sql_select_limit or default_order_by_limit is set.
 */
public class AddDefaultLimit extends DefaultPlanRewriter<StatementContext> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, jobContext.getCascadesContext().getStatementContext());
    }

    @Override
    public Plan visit(Plan plan, StatementContext context) {
        // check if children contain logical sort and add limit.
        ConnectContext ctx = context.getConnectContext();
        if (ctx != null) {
            long defaultLimit = ctx.getSessionVariable().getSqlSelectLimit();
            if (defaultLimit >= 0) {
                return new LogicalLimit<>(defaultLimit, 0, LimitPhase.ORIGIN, plan);
            }
        }
        return plan;
    }

    // should add limit under anchor to keep optimize opportunity
    @Override
    public Plan visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            StatementContext context) {
        return cteAnchor.withChildren(cteAnchor.child(0), cteAnchor.child(1));
    }

    // we should keep that sink node is the top node of the plan tree.
    // currently, it's one of the olap table sink and file sink.
    @Override
    public Plan visitLogicalSink(LogicalSink<? extends Plan> logicalSink, StatementContext context) {
        return super.visit(logicalSink, context);
    }

    @Override
    public Plan visitLogicalLimit(LogicalLimit<? extends Plan> limit, StatementContext context) {
        return limit;
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, StatementContext context) {
        ConnectContext ctx = context.getConnectContext();
        if (ctx != null) {
            long defaultLimit = ctx.getSessionVariable().getDefaultOrderByLimit();
            long sqlLimit = ctx.getSessionVariable().getSqlSelectLimit();
            if (defaultLimit >= 0 || sqlLimit >= 0) {
                if (defaultLimit < 0) {
                    defaultLimit = Long.MAX_VALUE;
                }
                if (sqlLimit < 0) {
                    sqlLimit = Long.MAX_VALUE;
                }
                defaultLimit = Math.min(sqlLimit, defaultLimit);
                if (defaultLimit < Long.MAX_VALUE) {
                    return new LogicalLimit<>(defaultLimit, 0, LimitPhase.ORIGIN, sort);
                }
            }
        }
        return sort;
    }
}
