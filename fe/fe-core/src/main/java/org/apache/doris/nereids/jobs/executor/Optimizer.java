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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.jobs.joinorder.JoinOrderJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.CTEInliner;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning;
import org.apache.doris.nereids.rules.rewrite.EliminateEmptyRelation;
import org.apache.doris.nereids.rules.rewrite.EliminateUnnecessaryProject;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * Cascades style optimize:
 * Perform equivalent logical plan exploration and physical implementation enumeration,
 * try to find best plan under the guidance of statistic information and cost model.
 */
public class Optimizer {
    private static final Logger LOG = LogManager.getLogger(Optimizer.class);

    private final CascadesContext cascadesContext;

    public Optimizer(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext cannot be null");
    }

    /**
     * execute optimize, use dphyp or cascades according to join number and session variables.
     */
    public void execute() {
        MoreFieldsThread.keepFunctionSignature(() -> {
            // generate inlined CTE alternative for CBO comparison
            Plan cboInlinedPlan = generateCTEInlineAlternative();
            // init memo
            cascadesContext.toMemo();
            if (cboInlinedPlan != null) {
                cascadesContext.getMemo().copyIn(cboInlinedPlan, cascadesContext.getMemo().getRoot(), false);
            }
            // stats derive
            cascadesContext.getMemo().getRoot().getLogicalExpressions()
                    .forEach(groupExpression -> cascadesContext.pushJob(
                            new DeriveStatsJob(groupExpression, cascadesContext.getCurrentJobContext())));
            cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
            if (cascadesContext.getStatementContext().isDpHyp() || isDpHyp(cascadesContext)) {
                // RightNow, dp hyper can only order 64 join operators
                dpHypOptimize();
            }
            // Cascades optimize
            cascadesContext.pushJob(
                    new OptimizeGroupJob(cascadesContext.getMemo().getRoot(), cascadesContext.getCurrentJobContext()));
            cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
            return null;
        });
    }

    /**
     * This method calc the result that if use dp hyper or not
     */
    public static boolean isDpHyp(CascadesContext cascadesContext) {
        boolean optimizeWithUnknownColStats = false;
        if (ConnectContext.get() != null && ConnectContext.get().getStatementContext() != null) {
            if (ConnectContext.get().getStatementContext().isHasUnknownColStats()) {
                optimizeWithUnknownColStats = true;
            }
        }
        // DPHyp optimize
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        int maxTableCount = sessionVariable.getMaxTableCountUseCascadesJoinReorder();
        if (optimizeWithUnknownColStats) {
            // if column stats are unknown, 10~20 table-join is optimized by cascading framework
            maxTableCount = 2 * maxTableCount;
        }
        int continuousJoinNum = Memo.countMaxContinuousJoin(cascadesContext.getRewritePlan());
        cascadesContext.getStatementContext().setMaxContinuousJoin(continuousJoinNum);
        boolean isDpHyp = sessionVariable.enableDPHypOptimizer || continuousJoinNum > maxTableCount;
        boolean finalEnableDpHyp = !sessionVariable.isDisableJoinReorder()
                && !cascadesContext.isLeadingDisableJoinReorder()
                && continuousJoinNum <= sessionVariable.getMaxJoinNumberOfReorder()
                && isDpHyp;
        cascadesContext.getStatementContext().setDpHyp(finalEnableDpHyp);
        return finalEnableDpHyp;
    }

    private void dpHypOptimize() {
        Group root = cascadesContext.getMemo().getRoot();
        // Due to EnsureProjectOnTopJoin, root group can't be Join Group, so DPHyp doesn't change the root group
        cascadesContext.pushJob(new JoinOrderJob(root, cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }

    /**
     * Generate a fully inlined CTE alternative plan and add it to the Memo root group.
     * This gives the CBO the ability to compare costs of materialized vs inlined CTE approaches.
     *
     * After inlining, runs filter pushdown and column pruning on the inlined plan so that
     * each inlined CTE body gets consumer-specific filters pushed down into it, producing
     * different optimized sub-trees per consumer position (e.g., different date/type filters
     * can eliminate branches in UNION queries inside the CTE body).
     */
    private Plan generateCTEInlineAlternative() {
        int mode = getSessionVariable().cteInlineMode;
        if (mode < 0) {
            return null;
        }
        try {
            if (mode == 0) {
                return generateSelectiveCTEInline();
            } else {
                return generateFullCTEInline();
            }
        } catch (Exception e) {
            LOG.warn("Failed to generate CTE inline alternative for CBO, fall back to default behavior", e);
            return null;
        }
    }

    private Plan generateFullCTEInline() {
        Plan rewritePlan = cascadesContext.getRewritePlan();
        CTEInliner cteInliner = new CTEInliner(cascadesContext.getStatementContext());
        Plan inlinedPlan = cteInliner.generateInlinedPlan(rewritePlan);
        if (inlinedPlan != null) {
            return rewriteInlinedPlan(inlinedPlan);
        }
        return null;
    }

    // Returns null because mode=0 directly replaces rewritePlan via
    // setRewritePlan(),
    // so toMemo() will use the inlined plan. No need to copyIn as an alternative.
    private Plan generateSelectiveCTEInline() {
        Plan rewritePlan = cascadesContext.getRewritePlan();
        CTEInliner cteInliner = new CTEInliner(cascadesContext.getStatementContext(), true);
        Plan inlinedPlan = cteInliner.generateInlinedPlan(rewritePlan);
        if (inlinedPlan != null) {
            inlinedPlan = rewriteInlinedPlan(inlinedPlan);
            if (inlinedPlan.anyMatch(p -> p instanceof LogicalEmptyRelation)) {
                inlinedPlan = eliminateEmptyRelation(inlinedPlan);
                cascadesContext.setRewritePlan(inlinedPlan);
                return null;
            }
        }
        return null;
    }

    private Plan eliminateEmptyRelation(Plan plan) {
        CascadesContext ctx = CascadesContext.initContext(
                cascadesContext.getStatementContext(), plan, PhysicalProperties.ANY);
        // Use getCteChildrenRewriter for the same reason as rewriteInlinedPlan:
        // getWholeTreeRewriterWithCustomJobs would invoke RewriteCteChildren which
        // reads stale rewrittenCteConsumer cache from the main Rewriter phase,
        // reverting the inlined CTE subtrees back to the original structure.
        Rewriter.getCteChildrenRewriter(ctx, ImmutableList.of(
                Rewriter.bottomUp(new EliminateEmptyRelation()),
                Rewriter.custom(RuleType.COLUMN_PRUNING, ColumnPruning::new),
                Rewriter.custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new))).execute();
        return ctx.getRewritePlan();
    }

    /**
     * Run filter pushdown and column pruning on the inlined plan using a temporary
     * CascadesContext.
     *
     * We deliberately use getCteChildrenRewriter (no notTraverseChildrenOf wrapper) so that
     * PUSH_DOWN_FILTERS traverses the ENTIRE inlined plan tree, including inside any remaining
     * LogicalCTEAnchor subtrees (e.g. for CTEs that were NOT inlined). Using
     * getWholeTreeRewriterWithCustomJobs would invoke RewriteCteChildren, which reads from the
     * shared StatementContext cache (rewrittenCteConsumer) populated during the main Rewriter
     * phase. That cached outer query still contains LogicalCTEConsumer nodes for the inlined CTE,
     * preventing the filter from ever reaching the inlined union body.
     */
    private Plan rewriteInlinedPlan(Plan inlinedPlan) {
        CascadesContext inlinedContext = CascadesContext.initContext(
                cascadesContext.getStatementContext(), inlinedPlan, PhysicalProperties.ANY);
        Rewriter.getCteChildrenRewriter(inlinedContext, ImmutableList.of(
                Rewriter.bottomUp(RuleSet.PUSH_DOWN_FILTERS),
                Rewriter.custom(RuleType.COLUMN_PRUNING, ColumnPruning::new),
                Rewriter.bottomUp(RuleSet.PUSH_DOWN_FILTERS),
                Rewriter.custom(RuleType.ELIMINATE_UNNECESSARY_PROJECT, EliminateUnnecessaryProject::new))).execute();
        return inlinedContext.getRewritePlan();
    }

    private SessionVariable getSessionVariable() {
        return cascadesContext.getConnectContext().getSessionVariable();
    }
}
