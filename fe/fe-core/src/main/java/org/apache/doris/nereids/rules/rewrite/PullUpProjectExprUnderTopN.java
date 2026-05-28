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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pull up non-trivial expressions from Projects below TopN to above TopN,
 * exposing their input base columns as lazy materialization candidates.
 *
 * <p>Two-pass CustomRewriter:
 * <ol>
 * <li><b>Collector (top-down)</b>: walk the plan tree, find qualifying TopNs,
 *     walk into their descendants to find Projects with pull-able expressions.</li>
 * <li><b>Replacer (bottom-up)</b>: simplify found Projects and add upper
 *     Projects above TopN to restore pulled-up expressions.</li>
 * </ol>
 */
public class PullUpProjectExprUnderTopN implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        ConnectContext ctx = jobContext.getCascadesContext()
                .getStatementContext().getConnectContext();
        if (ctx != null && !ctx.getSessionVariable().enableTopnExprPullup) {
            return plan;
        }

        // Pass 1: Collect pull-up info
        CollectorContext collectorCtx = new CollectorContext();
        plan.accept(new Collector(), collectorCtx);

        if (collectorCtx.topNToPullUpInfo.isEmpty()) {
            return plan;
        }

        // Pass 2: Replace/restructure
        return plan.accept(new Replacer(), collectorCtx);
    }

    // =========================================================================
    // Data structures
    // =========================================================================

    /** Info collected per TopN about which expressions to pull up from which Projects. */
    static class PullUpInfo {
        final LogicalTopN topN;
        final List<Slot> originalTopNOutput;
        final List<NamedExpression> allPulledUpExprs = new ArrayList<>();
        final Map<LogicalProject<? extends Plan>, List<NamedExpression>> projectToPulledUpExprs
                = new LinkedHashMap<>();
        final Map<ExprId, List<Slot>> baseSlotsByExpr = new HashMap<>();

        PullUpInfo(LogicalTopN topN) {
            this.topN = topN;
            this.originalTopNOutput = ImmutableList.copyOf(topN.getOutput());
        }

        void addPulledUpExpr(LogicalProject<? extends Plan> project, NamedExpression expr) {
            allPulledUpExprs.add(expr);
            projectToPulledUpExprs.computeIfAbsent(project, k -> new ArrayList<>()).add(expr);
            baseSlotsByExpr.put(expr.getExprId(), ImmutableList.copyOf(expr.getInputSlots()));
        }
    }

    /** Context shared between collector and replacer passes. */
    static class CollectorContext {
        final Map<LogicalTopN, PullUpInfo> topNToPullUpInfo = new LinkedHashMap<>();
        boolean insideQualifyingTopN = false;
        boolean insideCTEProducer = false;

        boolean hasPullUpInfo(LogicalTopN topN) {
            return topNToPullUpInfo.containsKey(topN);
        }

        PullUpInfo getPullUpInfo(LogicalTopN topN) {
            return topNToPullUpInfo.get(topN);
        }

        PullUpInfo getPullUpInfoForProject(LogicalProject<? extends Plan> project) {
            for (PullUpInfo info : topNToPullUpInfo.values()) {
                if (info.projectToPulledUpExprs.containsKey(project)) {
                    return info;
                }
            }
            return null;
        }
    }

    // =========================================================================
    // Pass 1: Collector (top-down)
    // =========================================================================

    static class Collector extends DefaultPlanRewriter<CollectorContext> {

        private static boolean qualifiesForLazyMat(LogicalTopN topN) {
            long limit = topN.getLimit();
            if (limit <= 0) {
                return false;
            }
            long threshold = SessionVariable.getTopNLazyMaterializationThreshold();
            return threshold >= limit;
        }

        @Override
        public Plan visitLogicalCTEProducer(
                LogicalCTEProducer<? extends Plan> cteProducer, CollectorContext context) {
            context.insideCTEProducer = true;
            try {
                return visit(cteProducer, context);
            } finally {
                context.insideCTEProducer = false;
            }
        }

        @Override
        public Plan visitLogicalTopN(LogicalTopN topN, CollectorContext context) {
            if (context.insideCTEProducer
                    || !qualifiesForLazyMat(topN)
                    || context.insideQualifyingTopN) {
                return visit(topN, context);
            }
            PullUpInfo info = new PullUpInfo(topN);
            Set<ExprId> collectedOutputExprIds = new HashSet<>();
            boolean blocked = walkAndCollect((Plan) topN.child(0), topN, info, collectedOutputExprIds);
            if (!blocked && !info.allPulledUpExprs.isEmpty()) {
                context.topNToPullUpInfo.put(topN, info);
            }
            context.insideQualifyingTopN = true;
            try {
                return visit(topN, context);
            } finally {
                context.insideQualifyingTopN = false;
            }
        }
    }

    /**
     * Walk down from a qualifying TopN's child to find Projects with pull-able expressions.
     *
     * @return true if walk was blocked (expression outputs used by intermediate operator)
     */
    private static boolean walkAndCollect(Plan node, LogicalTopN topN, PullUpInfo info,
            Set<ExprId> collectedOutputExprIds) {
        if (node instanceof LogicalProject) {
            LogicalProject<? extends Plan> project = (LogicalProject<? extends Plan>) node;
            for (NamedExpression ne : project.getProjects()) {
                if (canPullUp(ne, topN)) {
                    info.addPulledUpExpr(project, ne);
                    collectedOutputExprIds.add(ne.getExprId());
                }
            }
            Plan child = (Plan) project.child(0);
            if (child instanceof LogicalProject) {
                return false;
            }
            if (!isScanNode(child)) {
                return walkAndCollect(child, topN, info, collectedOutputExprIds);
            }
            return false;
        }

        if (node instanceof LogicalJoin) {
            LogicalJoin<? extends Plan, ? extends Plan> join = (LogicalJoin<? extends Plan, ? extends Plan>) node;
            // Walk children first, then check if join conditions reference ANY
            // collected expressions (both from children and from ancestors).
            int oldSize = collectedOutputExprIds.size();
            boolean leftBlocked = walkAndCollect((Plan) join.left(), topN, info, collectedOutputExprIds);
            boolean rightBlocked = walkAndCollect((Plan) join.right(), topN, info, collectedOutputExprIds);
            if (leftBlocked || rightBlocked) {
                return true;
            }
            // Check join conditions against ALL collected outputs (not just new ones),
            // because expressions may have been collected from a Project above this Join.
            if (operatorUsesPulledUpOutputs(node, collectedOutputExprIds)) {
                // Remove only what's referenced by the join condition
                Set<ExprId> referenced = collectReferencedExprIds(join);
                removePulledUpExprsByOutput(info, referenced, collectedOutputExprIds);
            }
            return false;
        }

        if (node instanceof LogicalFilter) {
            if (operatorUsesPulledUpOutputs(node, collectedOutputExprIds)) {
                info.allPulledUpExprs.clear();
                info.projectToPulledUpExprs.clear();
                info.baseSlotsByExpr.clear();
                collectedOutputExprIds.clear();
                return true;
            }
            return walkAndCollect((Plan) ((LogicalFilter<? extends Plan>) node).child(0), topN,
                    info, collectedOutputExprIds);
        }

        // Scan, Agg, Sort, Window, Union, CTE Producer: stop walking
        if (node instanceof LogicalCTEProducer) {
            return false;
        }
        return false;
    }

    private static boolean isScanNode(Plan node) {
        return node instanceof LogicalRelation;
    }

    /**
     * Check whether any expression of an intermediate operator references
     * the output slots of already-collected pull-up expressions.
     */
    private static boolean operatorUsesPulledUpOutputs(Plan node, Set<ExprId> pulledUpOutputExprIds) {
        if (pulledUpOutputExprIds.isEmpty()) {
            return false;
        }
        List<Expression> expressions = new ArrayList<>();
        if (node instanceof LogicalFilter) {
            expressions.addAll(((LogicalFilter<? extends Plan>) node).getConjuncts());
        } else if (node instanceof LogicalJoin) {
            LogicalJoin<? extends Plan, ? extends Plan> join
                    = (LogicalJoin<? extends Plan, ? extends Plan>) node;
            expressions.addAll(join.getHashJoinConjuncts());
            expressions.addAll(join.getOtherJoinConjuncts());
            expressions.addAll(join.getMarkJoinConjuncts());
        }
        for (Expression expr : expressions) {
            for (Slot slot : expr.getInputSlots()) {
                if (pulledUpOutputExprIds.contains(slot.getExprId())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Set<ExprId> collectReferencedExprIds(LogicalJoin<?, ?> join) {
        Set<ExprId> result = new HashSet<>();
        List<Expression> allConjuncts = new ArrayList<>();
        allConjuncts.addAll(join.getHashJoinConjuncts());
        allConjuncts.addAll(join.getOtherJoinConjuncts());
        allConjuncts.addAll(join.getMarkJoinConjuncts());
        for (Expression expr : allConjuncts) {
            // Collect the expression's own ExprId if it's a NamedExpression
            // (e.g., extracted Alias like expr_cast(d_week_seq1 as BIGINT)#389)
            if (expr instanceof NamedExpression) {
                result.add(((NamedExpression) expr).getExprId());
            }
            // Also collect all input slot ExprIds
            for (Slot slot : expr.getInputSlots()) {
                result.add(slot.getExprId());
            }
        }
        return result;
    }

    private static void removePulledUpExprsByOutput(PullUpInfo info, Set<ExprId> toRemove,
            Set<ExprId> collectedOutputExprIds) {
        Set<ExprId> removed = new HashSet<>();
        List<NamedExpression> remaining = new ArrayList<>();
        for (NamedExpression ne : info.allPulledUpExprs) {
            if (toRemove.contains(ne.getExprId())) {
                removed.add(ne.getExprId());
            } else {
                remaining.add(ne);
            }
        }
        info.allPulledUpExprs.clear();
        info.allPulledUpExprs.addAll(remaining);
        info.projectToPulledUpExprs.values().forEach(
                list -> list.removeIf(e -> toRemove.contains(e.getExprId())));
        info.projectToPulledUpExprs.entrySet().removeIf(
                e -> e.getValue().isEmpty());
        removed.forEach(info.baseSlotsByExpr::remove);
        collectedOutputExprIds.removeAll(toRemove);
    }

    // =========================================================================
    // Pull-up eligibility
    // =========================================================================

    /**
     * Check if a named expression can be pulled up above TopN.
     * Eligible: Alias with non-trivial child, not in order keys, no NoneMovableFunction.
     */
    static boolean canPullUp(NamedExpression ne, LogicalTopN topN) {
        if (!(ne instanceof Alias)) {
            return false;
        }
        Expression child = ((Alias) ne).child();
        if (child instanceof Slot || child instanceof Literal) {
            return false;
        }

        // Not referenced by order keys (including via input slots)
        Set<ExprId> orderKeyExprIds = new HashSet<>();
        for (Object obj : topN.getOrderKeys()) {
            OrderKey ok = (OrderKey) obj;
            Expression keyExpr = ok.getExpr();
            if (keyExpr instanceof NamedExpression) {
                orderKeyExprIds.add(((NamedExpression) keyExpr).getExprId());
            }
            for (Slot slot : keyExpr.getInputSlots()) {
                orderKeyExprIds.add(slot.getExprId());
            }
        }
        if (orderKeyExprIds.contains(ne.getExprId())) {
            return false;
        }

        // No NoneMovableFunction
        if (ne.anyMatch(e -> e instanceof NoneMovableFunction)) {
            return false;
        }

        return true;
    }

    // =========================================================================
    // Pass 2: Replacer (bottom-up)
    // =========================================================================

    static class Replacer extends DefaultPlanRewriter<CollectorContext> {

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, CollectorContext context) {
            LogicalProject<? extends Plan> rewritten = (LogicalProject<? extends Plan>) visit(project, context);
            PullUpInfo info = context.getPullUpInfoForProject(rewritten);
            if (info == null && rewritten != project
                    && rewritten.getProjects().equals(project.getProjects())) {
                info = context.getPullUpInfoForProject(project);
            }
            if (info == null) {
                return rewritten;
            }
            return simplifyProject(rewritten, info, project);
        }

        @Override
        public Plan visitLogicalTopN(LogicalTopN topN, CollectorContext context) {
            LogicalTopN rewritten = (LogicalTopN) visit(topN, context);
            if (!context.hasPullUpInfo(rewritten)) {
                return rewritten;
            }
            PullUpInfo info = context.getPullUpInfo(rewritten);
            if (info.allPulledUpExprs.isEmpty()) {
                return rewritten;
            }
            return addUpperProject(rewritten, info);
        }
    }

    /** Remove pulled-up expressions from project and add their base input slots. */
    private static LogicalProject<? extends Plan> simplifyProject(
            LogicalProject<? extends Plan> project, PullUpInfo info, LogicalProject<? extends Plan> original) {
        List<NamedExpression> pulledUpExprs = info.projectToPulledUpExprs.get(original != null ? original : project);
        if (pulledUpExprs == null) {
            pulledUpExprs = info.projectToPulledUpExprs.get(project);
        }
        if (pulledUpExprs == null || pulledUpExprs.isEmpty()) {
            return project;
        }

        Set<ExprId> pulledUpExprIds = new HashSet<>();
        for (NamedExpression ne : pulledUpExprs) {
            pulledUpExprIds.add(ne.getExprId());
        }

        List<NamedExpression> simplified = new ArrayList<>();
        Set<ExprId> existingExprIds = new HashSet<>();
        for (NamedExpression ne : project.getProjects()) {
            if (!pulledUpExprIds.contains(ne.getExprId())) {
                simplified.add(ne);
                existingExprIds.add(ne.getExprId());
            }
        }

        for (NamedExpression pulledUpExpr : pulledUpExprs) {
            List<Slot> baseSlots = info.baseSlotsByExpr.get(pulledUpExpr.getExprId());
            if (baseSlots != null) {
                for (Slot baseSlot : baseSlots) {
                    if (!existingExprIds.contains(baseSlot.getExprId())) {
                        simplified.add(baseSlot);
                        existingExprIds.add(baseSlot.getExprId());
                    }
                }
            }
        }

        if (simplified.equals(project.getProjects())) {
            return project;
        }
        return (LogicalProject<? extends Plan>) project.withProjects(simplified);
    }

    /** Create a new Project above the TopN that restores pulled-up expressions. */
    private static LogicalProject<Plan> addUpperProject(LogicalTopN topN, PullUpInfo info) {
        Map<ExprId, NamedExpression> pulledUpBySlotExprId = new HashMap<>();
        for (NamedExpression e : info.allPulledUpExprs) {
            pulledUpBySlotExprId.put(e.toSlot().getExprId(), e);
        }

        List<NamedExpression> upperOutput = new ArrayList<>();
        for (Slot slot : info.originalTopNOutput) {
            NamedExpression pulledUpExpr = pulledUpBySlotExprId.get(slot.getExprId());
            if (pulledUpExpr != null) {
                upperOutput.add(pulledUpExpr);
            } else {
                upperOutput.add(slot);
            }
        }

        return new LogicalProject<>(ImmutableList.copyOf(upperOutput), topN);
    }
}
