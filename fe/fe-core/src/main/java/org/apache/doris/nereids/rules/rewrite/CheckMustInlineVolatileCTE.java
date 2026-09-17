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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.Set;

/**
 * Report recursive ctes which still reference a cte that must be inlined but contains a volatile
 * expression, after the dead branches in the recursive child have been eliminated.
 *
 * <p>Every cte referenced by the recursive child of a recursive cte has to be inlined, because the
 * recursive child is reset and re-executed on every iteration and therefore can not read a
 * materialized cte. A cte containing a volatile expression (rand(), uuid(), a volatile udf, ...)
 * can not be inlined either: the volatile expression would be evaluated once per iteration and once
 * per reference instead of once for the whole statement. {@link CTEInline} keeps such ctes
 * materialized and defers the decision to this rule, which is applied after the dead branches are
 * removed, so references which are eliminated as dead code (for example below a false filter) stay
 * valid.
 */
public class CheckMustInlineVolatileCTE extends DefaultPlanRewriter<Void> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        Set<CTEId> deferredCTEs = jobContext.getCascadesContext().getStatementContext()
                .getDeferredInlineVolatileCTEs();
        if (deferredCTEs.isEmpty()) {
            return plan;
        }
        plan.foreach(node -> {
            if (node instanceof LogicalRecursiveUnion) {
                checkRecursiveChild(((LogicalRecursiveUnion<?, ?>) node).child(1), deferredCTEs);
            }
        });
        return plan;
    }

    private void checkRecursiveChild(Plan recursiveChild, Set<CTEId> deferredCTEs) {
        recursiveChild.foreach(node -> {
            if (node instanceof LogicalCTEConsumer
                    && deferredCTEs.contains(((LogicalCTEConsumer) node).getCteId())) {
                throw new AnalysisException("recursive cte must inline all used ctes,"
                        + " but inline is blocked by volatile function");
            }
        });
    }
}
