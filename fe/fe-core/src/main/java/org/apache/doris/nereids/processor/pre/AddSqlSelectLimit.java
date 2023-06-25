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

package org.apache.doris.nereids.processor.pre;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.ConnectContext;

/**
 * handle sql_select_limit and default_order_by_limit
 */
public class AddSqlSelectLimit extends PlanPreprocessor implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext context) {
        return null;
    }

    @Override
    public Plan visit(Plan plan, StatementContext context) {
        // check if children contain logical sort and add limit.
        ConnectContext ctx = context.getConnectContext();
        if (ctx != null) {
            long defaultLimit = ctx.getSessionVariable().sqlSelectLimit;
            if (defaultLimit >= 0) {
                return new LogicalLimit<>(defaultLimit, 0, LimitPhase.GLOBAL, plan);
            }
        }
        return plan;
    }

    @Override
    public LogicalPlan visitLogicalLimit(LogicalLimit<? extends Plan> limit, StatementContext context) {
        return limit;
    }

    @Override
    public LogicalPlan visitLogicalSort(LogicalSort<? extends Plan> sort, StatementContext context) {
        ConnectContext ctx = context.getConnectContext();
        if (ctx != null) {
            long defaultLimit = ctx.getSessionVariable().defaultOrderByLimit;
            long sqlLimit = ctx.getSessionVariable().sqlSelectLimit;
            if (defaultLimit >= 0 || sqlLimit >= 0) {
                if (defaultLimit < 0) {
                    defaultLimit = Long.MAX_VALUE;
                }
                if (sqlLimit < 0) {
                    sqlLimit = Long.MAX_VALUE;
                }
                defaultLimit = Math.min(sqlLimit, defaultLimit);
                return new LogicalLimit<>(defaultLimit, 0, LimitPhase.GLOBAL, sort);
            }
        }
        return sort;
    }
}
