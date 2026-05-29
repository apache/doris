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
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
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
 *     walk into their descendants to find Projects with pull-able expressions.
 *     Any operator that references a slot blocks pulling up expressions that
 *     output that slot past it. Boundary nodes (Aggregate, Window, Repeat,
 *     Relation, CTEProducer) stop the walk.
 *     Set operators are treated as blockers for the current TopN but their
 *     children are still traversed so nested TopNs inside them are visited.</li>
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
        int cteProducerDepth = 0;

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

    private static boolean qualifiesForLazyMatThreshold(LogicalTopN topN) {
        long limit = topN.getLimit();
        if (limit <= 0) {
            return false;
        }
        long threshold = SessionVariable.getTopNLazyMaterializationThreshold();
        return threshold >= limit;
    }

    static class Collector extends DefaultPlanRewriter<CollectorContext> {

        @Override
        public Plan visitLogicalCTEProducer(
                LogicalCTEProducer<? extends Plan> cteProducer, CollectorContext context) {
            context.cteProducerDepth++;
            try {
                return visit(cteProducer, context);
            } finally {
                context.cteProducerDepth--;
            }
        }

        @Override
        public Plan visitLogicalTopN(LogicalTopN topN, CollectorContext context) {
            if (context.cteProducerDepth > 0
                    || !qualifiesForLazyMatThreshold(topN)) {
                return visit(topN, context);
            }
            PullUpInfo info = new PullUpInfo(topN);
            // Seed blockedExprIds with this TopN's order key ExprIds so that
            // expressions used by order keys are not pulled up past this TopN.
            Set<ExprId> blockedExprIds = buildOrderKeyExprIds(topN);
            collectFromNode((Plan) topN.child(0), info, blockedExprIds);
            if (!info.allPulledUpExprs.isEmpty()) {
                context.topNToPullUpInfo.put(topN, info);
            }
            return visit(topN, context);
        }
    }

    /**
     * Recursively walk down from a TopN's child to find Projects with pull-able expressions.
     *
     * <p>{@code blockedExprIds} contains ExprIds of slots that are referenced by operators
     * along the path from the TopN to the current node. An expression whose output ExprId
     * is in this set cannot be pulled up past the operators that reference it.
     */
    private static void collectFromNode(Plan node, PullUpInfo info, Set<ExprId> blockedExprIds) {
        if (node instanceof LogicalProject) {
            LogicalProject<? extends Plan> project = (LogicalProject<? extends Plan>) node;
            for (NamedExpression ne : project.getProjects()) {
                if (canPullUp(ne) && !blockedExprIds.contains(ne.getExprId())) {
                    info.addPulledUpExpr(project, ne);
                }
            }
            // Continue into the project's child. Chained projects are all visited.
            collectFromNode((Plan) project.child(0), info, blockedExprIds);
            return;
        }

        if (node instanceof LogicalTopN) {
            LogicalTopN inner = (LogicalTopN) node;
            // TopN preserves all input columns, so it doesn't block by itself.
            // However, its order keys consume slots, so add them to blocked set.
            // Do NOT reset blockedExprIds — intermediate operators between the
            // outer and inner TopN must still block expressions.
            Set<ExprId> newBlocked = new HashSet<>(blockedExprIds);
            newBlocked.addAll(buildOrderKeyExprIds(inner));
            collectFromNode((Plan) inner.child(0), info, newBlocked);
            return;
        }

        // Stop at boundary nodes that transform the schema or are data sources.
        if (node instanceof LogicalRelation || node instanceof LogicalCTEProducer
                || isBlockingNode(node)) {
            return;
        }

        // For set operators, walk into each child.
        // blockedExprIds must be mapped from the set operator's output ExprIds
        // to each child's output ExprIds via regularChildrenOutputs.
        //
        // Union ALL is transparent: it only concatenates rows and does not
        // access individual columns, so its outputs are not blocked.
        // Union DISTINCT, Except and Intersect need to compare/deduplicate rows,
        // so they consume all output columns and their outputs are blocked.
        if (node instanceof LogicalSetOperation) {
            LogicalSetOperation setOp = (LogicalSetOperation) node;
            Set<ExprId> newBlocked = new HashSet<>(blockedExprIds);
            if (shouldBlockSetOpOutputs(setOp)) {
                for (NamedExpression output : setOp.getOutputs()) {
                    newBlocked.add(output.getExprId());
                }
            }
            for (int childIdx = 0; childIdx < node.children().size(); childIdx++) {
                Set<ExprId> childBlocked = mapBlockedExprIdsForSetOpChild(setOp, childIdx, newBlocked);
                collectFromNode(node.child(childIdx), info, childBlocked);
            }
            return;
        }

        // For all other nodes, add their input slot ExprIds to the blocked set.
        // Any operator that references a slot in its expressions prevents
        // expressions that output that slot from being pulled up past it.
        Set<ExprId> newBlocked = new HashSet<>(blockedExprIds);
        for (Expression expr : node.getExpressions()) {
            newBlocked.addAll(expr.getInputSlotExprIds());
            if (expr instanceof NamedExpression) {
                newBlocked.add(((NamedExpression) expr).getExprId());
            }
        }

        for (Plan child : node.children()) {
            collectFromNode(child, info, newBlocked);
        }
    }

    // =========================================================================
    // Pull-up eligibility
    // =========================================================================

    /**
     * Check if a named expression can be pulled up above TopN.
     * Eligible: Alias with non-trivial child, not blocked, no NoneMovableFunction.
     */
    static boolean canPullUp(NamedExpression ne) {
        if (!(ne instanceof Alias)) {
            return false;
        }
        Expression child = ((Alias) ne).child();
        if (child instanceof Slot || child instanceof Literal) {
            return false;
        }
        if (ne.anyMatch(e -> e instanceof NoneMovableFunction)) {
            return false;
        }
        return true;
    }

    private static boolean isBlockingNode(Plan node) {
        return node instanceof LogicalAggregate
                || node instanceof LogicalWindow
                || node instanceof LogicalRepeat;
    }

    /**
     * Determine whether a set operator's outputs should be treated as blocked.
     *
     * <p>Union ALL is transparent (just concatenates rows).
     * Union DISTINCT, Except and Intersect consume all output columns
     * for deduplication/comparison, so their outputs are blocked.
     */
    private static boolean shouldBlockSetOpOutputs(LogicalSetOperation setOp) {
        if (setOp instanceof LogicalUnion) {
            return setOp.getQualifier() == org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier.DISTINCT;
        }
        // Except and Intersect always block.
        return setOp instanceof LogicalExcept || setOp instanceof LogicalIntersect;
    }

    private static Set<ExprId> buildOrderKeyExprIds(LogicalTopN<?> topN) {
        Set<ExprId> orderKeyExprIds = new HashSet<>();
        for (OrderKey orderKey : topN.getOrderKeys()) {
            Expression keyExpr = orderKey.getExpr();
            orderKeyExprIds.addAll(keyExpr.getInputSlotExprIds());
            if (keyExpr instanceof NamedExpression) {
                orderKeyExprIds.add(((NamedExpression) keyExpr).getExprId());
            }
        }
        return orderKeyExprIds;
    }

    /**
     * Map blocked ExprIds from a set operator's output scope to a specific child's output scope.
     *
     * <p>Set operators project each child's outputs (regularChildrenOutputs[i]) into the set
     * operator's outputs by position. An ExprId that is blocked at the set operator level
     * corresponds to the same-position child output ExprId when we walk into that child.
     */
    private static Set<ExprId> mapBlockedExprIdsForSetOpChild(
            LogicalSetOperation setOp, int childIdx, Set<ExprId> blockedExprIds) {
        Set<ExprId> childBlocked = new HashSet<>();
        List<org.apache.doris.nereids.trees.expressions.SlotReference> childOutputs
                = setOp.getRegularChildOutput(childIdx);
        List<NamedExpression> setOutputs = setOp.getOutputs();

        // Map by position: set operator output j corresponds to child output j.
        for (int i = 0; i < setOutputs.size() && i < childOutputs.size(); i++) {
            if (blockedExprIds.contains(setOutputs.get(i).getExprId())) {
                childBlocked.add(childOutputs.get(i).getExprId());
            }
        }

        // Also preserve any child output ExprIds that happen to already be in the blocked set.
        for (org.apache.doris.nereids.trees.expressions.SlotReference childOutput : childOutputs) {
            if (blockedExprIds.contains(childOutput.getExprId())) {
                childBlocked.add(childOutput.getExprId());
            }
        }

        return childBlocked;
    }

    /**
     * Deduplicate pull-up expressions so that each expression in a Project is only
     * pulled up to the outermost TopN that collects it.
     *
     * <p>Since {@link CollectorContext#topNToPullUpInfo} is a {@link LinkedHashMap}
     * and the Collector visits the plan top-down, iteration order is outer-to-inner.
     * We keep the first occurrence of each (project-reference, exprId) pair and
     * remove duplicates from inner TopNs.
     */
    private static void deduplicatePullUps(CollectorContext context) {
        // Use IdentityHashMap because we need to distinguish Project nodes by object
        // reference, not by content equality.
        Map<LogicalProject<? extends Plan>, Set<ExprId>> handled = new IdentityHashMap<>();

        for (PullUpInfo info : context.topNToPullUpInfo.values()) {
            List<NamedExpression> toRemove = new ArrayList<>();
            for (Map.Entry<LogicalProject<? extends Plan>, List<NamedExpression>> entry
                    : info.projectToPulledUpExprs.entrySet()) {
                LogicalProject<? extends Plan> project = entry.getKey();
                Set<ExprId> projectHandled = handled.computeIfAbsent(project, k -> new HashSet<>());
                for (NamedExpression expr : entry.getValue()) {
                    if (projectHandled.contains(expr.getExprId())) {
                        toRemove.add(expr);
                    } else {
                        projectHandled.add(expr.getExprId());
                    }
                }
            }
            for (NamedExpression expr : toRemove) {
                info.allPulledUpExprs.remove(expr);
                for (List<NamedExpression> list : info.projectToPulledUpExprs.values()) {
                    list.removeIf(e -> e == expr);
                }
                info.projectToPulledUpExprs.entrySet().removeIf(e -> e.getValue().isEmpty());
                info.baseSlotsByExpr.remove(expr.getExprId());
            }
        }
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
            PullUpInfo info = context.getPullUpInfo(rewritten);
            if (info == null && rewritten != topN) {
                info = context.getPullUpInfo(topN);
            }
            if (info == null || info.allPulledUpExprs.isEmpty()) {
                return rewritten;
            }
            return addUpperProject(rewritten, info);
        }
    }

    /** Remove pulled-up expressions from project and add their base input slots. */
    private static LogicalProject<? extends Plan> simplifyProject(
            LogicalProject<? extends Plan> project, PullUpInfo info, LogicalProject<? extends Plan> original) {
        List<NamedExpression> pulledUpExprs = info.projectToPulledUpExprs.get(original);
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
