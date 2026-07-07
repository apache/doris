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
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

/**
 * Internal visitor that dispatches each plan node to the linear, join, or aggregate handler.
 */
class IvmDeltaRewriteVisitor extends PlanVisitor<IvmDeltaRewriteResult, IvmRefreshContext> {
    private final IvmLinearDeltaHandler linearHandler;
    private final IvmJoinDeltaHandler joinHandler;
    private final IvmAggDeltaHandler aggHandler;

    IvmDeltaRewriteVisitor() {
        this(new IvmLinearDeltaHandler(), new IvmJoinDeltaHandler(), new IvmAggDeltaHandler());
    }

    IvmDeltaRewriteVisitor(IvmLinearDeltaHandler linearHandler,
            IvmJoinDeltaHandler joinHandler, IvmAggDeltaHandler aggHandler) {
        this.linearHandler = linearHandler;
        this.joinHandler = joinHandler;
        this.aggHandler = aggHandler;
    }

    IvmDeltaRewriteResult rewritePlan(Plan normalizedPlan, IvmRefreshContext ctx) {
        return normalizedPlan.accept(this, ctx);
    }

    @Override
    public IvmDeltaRewriteResult visit(Plan plan, IvmRefreshContext ctx) {
        throw new AnalysisException(
                "IVM delta rewrite does not support: " + plan.getClass().getSimpleName());
    }

    /** Regular scan is a snapshot — no delta to process. */
    @Override
    public IvmDeltaRewriteResult visitLogicalOlapScan(LogicalOlapScan scan, IvmRefreshContext ctx) {
        return linearHandler.rewriteOlapScan(scan);
    }

    /** Stream scan is a delta source — build dml_factor if isIncrementalScan. */
    @Override
    public IvmDeltaRewriteResult visitLogicalOlapTableStreamScan(
            LogicalOlapTableStreamScan scan, IvmRefreshContext ctx) {
        return linearHandler.rewriteOlapTableStreamScan(scan);
    }

    @Override
    public IvmDeltaRewriteResult visitLogicalProject(LogicalProject<? extends Plan> project,
            IvmRefreshContext ctx) {
        return linearHandler.rewriteProject(project, this, ctx);
    }

    @Override
    public IvmDeltaRewriteResult visitLogicalFilter(LogicalFilter<? extends Plan> filter,
            IvmRefreshContext ctx) {
        return linearHandler.rewriteFilter(filter, this, ctx);
    }

    @Override
    public IvmDeltaRewriteResult visitLogicalUnion(LogicalUnion union, IvmRefreshContext ctx) {
        return linearHandler.rewriteUnion(union, this, ctx);
    }

    @Override
    public IvmDeltaRewriteResult visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat,
            IvmRefreshContext ctx) {
        return linearHandler.rewriteRepeat(repeat, this, ctx);
    }

    @Override
    public IvmDeltaRewriteResult visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
            IvmRefreshContext ctx) {
        return joinHandler.rewriteJoin(join, this, ctx);
    }

    @Override
    public IvmDeltaRewriteResult visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
            IvmRefreshContext ctx) {
        throw new AnalysisException(
                "IVM: AGG node must be detached and handled by IvmDeltaRewriter, not the visitor");
    }
}
