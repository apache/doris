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
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import java.util.Optional;

/**
 * Internal visitor that dispatches each plan node to the linear, join, or aggregate handler.
 */
class IvmDeltaRewriteVisitor extends PlanVisitor<Optional<IvmDeltaRewriteResult>, IvmRefreshContext> {
    private final IvmLinearDeltaHandler linearHandler;
    private final IvmJoinDeltaHandler joinHandler;
    private final IvmAggDeltaHandler aggHandler;
    private final IvmDeltaRewriteState rewriteState;

    IvmDeltaRewriteVisitor() {
        this(new IvmLinearDeltaHandler(), new IvmJoinDeltaHandler(), new IvmAggDeltaHandler(), null);
    }

    IvmDeltaRewriteVisitor(IvmLinearDeltaHandler linearHandler,
            IvmJoinDeltaHandler joinHandler, IvmAggDeltaHandler aggHandler,
            IvmDeltaRewriteState rewriteState) {
        this.linearHandler = linearHandler;
        this.joinHandler = joinHandler;
        this.aggHandler = aggHandler;
        this.rewriteState = rewriteState;
    }

    Optional<IvmDeltaRewriteResult> rewritePlan(Plan normalizedPlan, IvmRefreshContext ctx) {
        return normalizedPlan.accept(this, ctx);
    }

    IvmDeltaRewriteState getRewriteState() {
        return rewriteState;
    }

    @Override
    public Optional<IvmDeltaRewriteResult> visit(Plan plan, IvmRefreshContext ctx) {
        throw new AnalysisException(
                "IVM delta rewrite does not support: " + plan.getClass().getSimpleName());
    }

    @Override
    public Optional<IvmDeltaRewriteResult> visitLogicalOlapScan(LogicalOlapScan scan, IvmRefreshContext ctx) {
        return linearHandler.rewriteOlapScan(scan, rewriteState);
    }

    @Override
    public Optional<IvmDeltaRewriteResult> visitLogicalProject(LogicalProject<? extends Plan> project,
            IvmRefreshContext ctx) {
        return linearHandler.rewriteProject(project, this, ctx);
    }

    @Override
    public Optional<IvmDeltaRewriteResult> visitLogicalFilter(LogicalFilter<? extends Plan> filter,
            IvmRefreshContext ctx) {
        return linearHandler.rewriteFilter(filter, this, ctx);
    }

    @Override
    public Optional<IvmDeltaRewriteResult> visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias,
            IvmRefreshContext ctx) {
        return linearHandler.rewriteSubQueryAlias(alias, this, ctx);
    }

    @Override
    public Optional<IvmDeltaRewriteResult> visitLogicalUnion(LogicalUnion union, IvmRefreshContext ctx) {
        return linearHandler.rewriteUnion(union, this, ctx);
    }

    @Override
    public Optional<IvmDeltaRewriteResult> visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat,
            IvmRefreshContext ctx) {
        return linearHandler.rewriteRepeat(repeat, this, ctx);
    }

    @Override
    public Optional<IvmDeltaRewriteResult> visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
            IvmRefreshContext ctx) {
        return joinHandler.rewriteJoin(join, this, ctx);
    }

    @Override
    public Optional<IvmDeltaRewriteResult> visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
            IvmRefreshContext ctx) {
        return aggHandler.rewriteAggregate(aggregate, this, ctx);
    }
}
