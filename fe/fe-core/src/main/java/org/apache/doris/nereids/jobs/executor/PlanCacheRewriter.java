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

package org.apache.doris.nereids.jobs.executor;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.PlanCachePhase;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.rewrite.PruneEmptyPartition;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanPartition;
import org.apache.doris.nereids.rules.rewrite.PruneOlapScanTablet;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.PlaceholderLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.PlanCacheKey;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

import java.util.List;

/** PlanCacheRewriter */
public class PlanCacheRewriter {
    private final CascadesContext cascadesContext;
    private final StatementContext statementContext;

    public PlanCacheRewriter(CascadesContext cascadesContext) {
        this.cascadesContext = cascadesContext;
        this.statementContext = cascadesContext.getStatementContext();
    }

    /** rewrite */
    public void rewrite() {
        boolean isPhaseOne = statementContext.planCachePhase == PlanCachePhase.ONE;
        NereidsSqlCacheManager sqlCacheManager = Env.getCurrentEnv().getSqlCacheManager();

        LogicalPlan planWithPlaceholderLiteral = (LogicalPlan) cascadesContext.getRewritePlan();
        if (isPhaseOne) {
            sqlCacheManager.planCache.put(
                    new PlanCacheKey(statementContext.initPlaceholderPlan),
                    planWithPlaceholderLiteral
            );
        }

        statementContext.planCachePhase = PlanCachePhase.TWO;
        analyzePlaceholderLiteralAndFunctions(planWithPlaceholderLiteral);
        new PhaseTwoRewriter(cascadesContext).execute();
    }

    private LogicalPlan analyzePlaceholderLiteralAndFunctions(LogicalPlan planWithPlaceholderLiteral) {
        DefaultPlanRewriter<Void> planRewriter = new DefaultPlanRewriter<Void>() {
            @Override
            public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
                PlaceholderAnalyzer placeholderAnalyzer = new PlaceholderAnalyzer(filter);
                ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(cascadesContext);
                Builder<Expression> newConjuncts
                        = ImmutableSet.builderWithExpectedSize(filter.getConjuncts().size());
                for (Expression conjunct : filter.getConjuncts()) {
                    Expression newConjunct = placeholderAnalyzer.analyze(conjunct, rewriteContext);
                    newConjuncts.add(newConjunct);
                }
                return new LogicalFilter<>(newConjuncts.build(), filter.child());
            }
        };
        LogicalPlan newPlan = (LogicalPlan) planWithPlaceholderLiteral.accept(planRewriter, null);
        cascadesContext.setRewritePlan(newPlan);
        return newPlan;
    }

    private class PlaceholderAnalyzer extends ExpressionAnalyzer {
        public PlaceholderAnalyzer(Plan currentPlan) {
            super(currentPlan, new Scope(ImmutableList.of()), cascadesContext, false, false);
        }

        @Override
        public Expression visitPlaceholderLiteral(PlaceholderLiteral placeholderLiteral,
                ExpressionRewriteContext context) {
            Literal literal = statementContext.placeholderLiteralToLiteral.get(placeholderLiteral);
            if (literal == null) {
                throw new IllegalStateException(
                        "placeholder literal not exists: ?" + placeholderLiteral.id);
            }
            return literal;
        }

        @Override
        public Expression visitPlaceholder(Placeholder placeholder, ExpressionRewriteContext context) {
            return super.visitPlaceholder(placeholder, context);
        }
    }

    private static class PhaseTwoRewriter extends AbstractBatchJobExecutor {
        private static final List<RewriteJob> REWRITE_JOBS = jobs(
                topDown(
                        new PruneOlapScanPartition(),
                        new PruneEmptyPartition(),
                        new PruneOlapScanTablet()
                )
        );

        public PhaseTwoRewriter(CascadesContext cascadesContext) {
            super(cascadesContext);
        }

        @Override
        public List<RewriteJob> getJobs() {
            return REWRITE_JOBS;
        }
    }
}
