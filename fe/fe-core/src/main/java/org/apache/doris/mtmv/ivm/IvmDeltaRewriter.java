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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

/**
 * Single recursive rewriter that transforms a normalized MV plan into delta commands.
 * Dispatches per-node-type via {@link DefaultPlanRewriter} visitor methods.
 *
 * Skeleton only — all visit methods throw AnalysisException.
 * Real per-node logic will be added in future PRs.
 */
public class IvmDeltaRewriter extends DefaultPlanRewriter<IvmDeltaRewriteContext> {

    /**
     * Entry point: rewrites the normalized plan into a delta plan for one driving base table.
     */
    public Plan rewrite(Plan normalizedPlan, IvmDeltaRewriteContext ctx) {
        return normalizedPlan.accept(this, ctx);
    }

    @Override
    public Plan visit(Plan plan, IvmDeltaRewriteContext ctx) {
        throw new AnalysisException(
                "IVM delta rewrite does not yet support: " + plan.getClass().getSimpleName());
    }
}
