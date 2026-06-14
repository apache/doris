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
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
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
 * <p>The rewriter runs bottom-up. Each LogicalTopN is treated as the current
 * target TopN after its child has already been rewritten. The target TopN then
 * collects only Projects in its own child subtree, stops at nested TopNs, and
 * adds one upper Project to restore the original TopN output. This lets an
 * upper TopN pull expressions that were just restored above a lower TopN.
 */
public class PullUpProjectExprUnderTopN implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        ConnectContext ctx = jobContext.getCascadesContext()
                .getStatementContext().getConnectContext();
        if (ctx != null && !ctx.getSessionVariable().enableTopnExprPullup) {
            return plan;
        }

        return plan.accept(new Rewriter(), new RewriteContext());
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
        final Map<Slot, Expression> pullUpExprReplaceMap = new LinkedHashMap<>();

        PullUpInfo(LogicalTopN topN) {
            this.topN = topN;
            this.originalTopNOutput = ImmutableList.copyOf(topN.getOutput());
        }

        void addPulledUpExpr(LogicalProject<? extends Plan> project, NamedExpression expr) {
            allPulledUpExprs.add(expr);
            projectToPulledUpExprs.computeIfAbsent(project, k -> new ArrayList<>()).add(expr);
            addPullUpExprReplace(expr);
        }

        void addPullUpExprReplace(NamedExpression expr) {
            if (expr instanceof Alias) {
                pullUpExprReplaceMap.putIfAbsent(expr.toSlot(), expr.child(0));
            }
        }
    }

    /** Context for the bottom-up traversal. */
    static class RewriteContext {
        int cteProducerDepth = 0;
    }

    // =========================================================================
    // Bottom-up TopN rewriter
    // =========================================================================

    private static boolean qualifiesForLazyMatThreshold(LogicalTopN topN) {
        long limit = topN.getLimit();
        if (limit <= 0) {
            return false;
        }
        long threshold = SessionVariable.getTopNLazyMaterializationThreshold();
        return threshold >= limit;
    }

    static class Rewriter extends DefaultPlanRewriter<RewriteContext> {

        @Override
        public Plan visitLogicalCTEProducer(
                LogicalCTEProducer<? extends Plan> cteProducer, RewriteContext context) {
            context.cteProducerDepth++;
            try {
                return visit(cteProducer, context);
            } finally {
                context.cteProducerDepth--;
            }
        }

        @Override
        public Plan visitLogicalTopN(LogicalTopN topN, RewriteContext context) {
            LogicalTopN rewritten = (LogicalTopN) visit(topN, context);
            if (context.cteProducerDepth > 0 || !qualifiesForLazyMatThreshold(rewritten)) {
                return rewritten;
            }
            PullUpInfo info = new PullUpInfo(rewritten);
            // Seed blockedExprIds with this TopN's order key ExprIds so that
            // expressions used by order keys are not pulled up past this TopN.
            collectFromNode((Plan) rewritten.child(0), info, buildOrderKeyExprIds(rewritten));
            if (info.allPulledUpExprs.isEmpty()) {
                return rewritten;
            }

            Plan simplifiedChild = ((Plan) rewritten.child(0)).accept(new ProjectSimplifier(), info);
            if (simplifiedChild == rewritten.child(0)) {
                return rewritten;
            }
            LogicalTopN topNWithSimplifiedChild = rewritten.withChildren(ImmutableList.of(simplifiedChild));
            return addUpperProject(topNWithSimplifiedChild, info);
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
            Set<ExprId> childBlockedExprIds = new HashSet<>(blockedExprIds);
            for (NamedExpression ne : project.getProjects()) {
                info.addPullUpExprReplace(ne);
                if (canPullUp(ne) && !blockedExprIds.contains(ne.getExprId())) {
                    info.addPulledUpExpr(project, ne);
                }
                if (blockedExprIds.contains(ne.getExprId())) {
                    childBlockedExprIds.addAll(ne.getInputSlotExprIds());
                }
            }
            // Continue into the project's child. Chained projects are all visited.
            collectFromNode((Plan) project.child(0), info, childBlockedExprIds);
            return;
        }

        if (node instanceof LogicalTopN) {
            // The bottom-up rewriter has already handled this nested TopN.
            // The current target TopN only collects Projects above it.
            return;
        }

        // Stop at boundary nodes that transform the schema or are data sources.
        if (node instanceof LogicalRelation || node instanceof LogicalCTEProducer
                || isBlockingNode(node)) {
            return;
        }

        // Set operations are a boundary for the current TopN: do NOT collect
        // expressions from below them. UNION ALL children may compute the same
        // output column with different expressions (e.g. a+1 vs a+2), and a
        // single pull-up Project above the TopN cannot represent branch-specific
        // semantics. The normal visitor will still traverse into the children,
        // so nested TopNs inside set operations are handled independently.
        if (node instanceof LogicalSetOperation) {
            return;
        }

        // For null-generating outer joins, block all output slots from the
        // nullable side(s). Expressions inside a nullable side are protected
        // by join null-extension: when there is no match, the entire nullable
        // tuple is set to NULL. Pulling such an expression above the join
        // would break this, e.g. ifnull(r.b, 0) inside the right side of a
        // LEFT JOIN would see individual column NULLs and convert them to 0,
        // changing the NULL that null-extension produced.
        // Example: SELECT l.id, sub.x FROM l LEFT JOIN (
        //            SELECT id, ifnull(b, 0) AS x FROM r) sub ON l.id = sub.id
        //          ORDER BY l.id LIMIT 3;
        // Here x=ifnull(b,0) is in a Project on the nullable (right) side.
        // Pulling it above the join turns unmatched-row x from NULL to 0.
        if (node instanceof LogicalJoin) {
            LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) node;
            JoinType joinType = join.getJoinType();
            Set<ExprId> newBlocked = new HashSet<>(blockedExprIds);
            // add join expression slots (same as default branch)
            for (Expression expr : node.getExpressions()) {
                newBlocked.addAll(expr.getInputSlotExprIds());
                if (expr instanceof NamedExpression) {
                    newBlocked.add(((NamedExpression) expr).getExprId());
                }
            }
            // block all output slots from the nullable side(s)
            if (joinType.isLeftOuterJoin() || joinType.isAsofLeftOuterJoin()
                    || joinType.isFullOuterJoin()) {
                for (Slot s : join.right().getOutput()) {
                    newBlocked.add(s.getExprId());
                }
            }
            if (joinType.isRightOuterJoin() || joinType.isAsofRightOuterJoin()
                    || joinType.isFullOuterJoin()) {
                for (Slot s : join.left().getOutput()) {
                    newBlocked.add(s.getExprId());
                }
            }
            for (Plan child : node.children()) {
                collectFromNode(child, info, newBlocked);
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
        if (ne.containsVolatileExpression()) {
            return false;
        }
        if (ne.anyMatch(e -> e instanceof Score)) {
            return false;
        }
        if (ne.anyMatch(e -> e instanceof L2DistanceApproximate)) {
            return false;
        }
        return true;
    }

    private static boolean isBlockingNode(Plan node) {
        return node instanceof LogicalAggregate
                || node instanceof LogicalWindow
                || node instanceof LogicalRepeat;
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

    static class ProjectSimplifier extends DefaultPlanRewriter<PullUpInfo> {
        @Override
        public Plan visitLogicalTopN(LogicalTopN topN, PullUpInfo info) {
            return topN;
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, PullUpInfo info) {
            LogicalProject<? extends Plan> rewritten = (LogicalProject<? extends Plan>) visit(project, info);
            List<NamedExpression> exprs = info.projectToPulledUpExprs.get(rewritten);
            if (exprs != null) {
                return simplifyProject(rewritten, exprs, info);
            }
            if (rewritten != project && rewritten.getProjects().equals(project.getProjects())) {
                exprs = info.projectToPulledUpExprs.get(project);
                if (exprs != null) {
                    return simplifyProject(rewritten, exprs, info);
                }
            }
            return simplifyProject(rewritten, ImmutableList.of(), info);
        }
    }

    /**
     * Remove pulled-up expressions from this Project and add the input slots that still need to pass through TopN.
     *
     * <p>For example, after pulling up {@code x = a + 1}:
     *
     * <pre>
     * TopN
     *   Project(id, x)                  -- forwards x from its child
     *     Project(id, a + 1 as x)
     *       Scan(id, a)
     * </pre>
     *
     * <p>The lower Project should become {@code Project(id, a)}, because {@code x} is restored above TopN.
     * The upper Project must also become {@code Project(id, a)} instead of keeping {@code Project(id, x)},
     * since its child no longer outputs {@code x}.
     */
    private static LogicalProject<? extends Plan> simplifyProject(
            LogicalProject<? extends Plan> project,
            List<NamedExpression> pulledUpExprs,
            PullUpInfo info) {
        Set<ExprId> childOutputExprIds = ((Plan) project.child(0)).getOutputExprIdSet();
        List<Expression> passThroughExprs = collectUnavailablePullUpExprs(project, info, childOutputExprIds);
        if (pulledUpExprs.isEmpty() && passThroughExprs.isEmpty()) {
            return project;
        }

        Set<ExprId> pulledUpExprIds = new HashSet<>();
        for (NamedExpression ne : pulledUpExprs) {
            pulledUpExprIds.add(ne.getExprId());
        }

        List<NamedExpression> simplified = new ArrayList<>();
        Set<ExprId> existingExprIds = new HashSet<>();
        for (NamedExpression ne : project.getProjects()) {
            if (!pulledUpExprIds.contains(ne.getExprId())
                    && !isUnavailablePullUpExpression(ne, info, childOutputExprIds)) {
                NamedExpression resolved = resolveAliasChildIfNeeded(ne, info, childOutputExprIds);
                simplified.add(resolved);
                existingExprIds.add(resolved.getExprId());
            }
        }

        for (NamedExpression pulledUpExpr : pulledUpExprs) {
            for (Slot baseSlot : resolveInputSlots(pulledUpExpr.child(0), info, childOutputExprIds)) {
                if (!existingExprIds.contains(baseSlot.getExprId())) {
                    simplified.add(baseSlot);
                    existingExprIds.add(baseSlot.getExprId());
                }
            }
        }
        for (Expression passThroughExpr : passThroughExprs) {
            for (Slot baseSlot : resolveInputSlots(passThroughExpr, info, childOutputExprIds)) {
                if (!existingExprIds.contains(baseSlot.getExprId())) {
                    simplified.add(baseSlot);
                    existingExprIds.add(baseSlot.getExprId());
                }
            }
        }

        if (simplified.equals(project.getProjects())) {
            return project;
        }
        return (LogicalProject<? extends Plan>) project.withProjects(simplified);
    }

    private static List<Expression> collectUnavailablePullUpExprs(
            LogicalProject<? extends Plan> project, PullUpInfo info, Set<ExprId> childOutputExprIds) {
        List<Expression> passThroughExprs = new ArrayList<>();
        for (NamedExpression ne : project.getProjects()) {
            if (isUnavailablePullUpExpression(ne, info, childOutputExprIds)) {
                passThroughExprs.add(getPullUpReplaceExpression(ne.toSlot(), info));
            }
        }
        return passThroughExprs;
    }

    private static boolean isUnavailablePullUpExpression(
            NamedExpression ne, PullUpInfo info, Set<ExprId> childOutputExprIds) {
        if (ne instanceof Slot) {
            return !childOutputExprIds.contains(ne.getExprId())
                    && getPullUpReplaceExpression((Slot) ne, info) != null;
        }
        return ne instanceof Alias
                && getPullUpReplaceExpression(ne.toSlot(), info) != null
                && ne.getInputSlots().stream().anyMatch(slot -> !childOutputExprIds.contains(slot.getExprId()));
    }

    private static Expression getPullUpReplaceExpression(Slot slot, PullUpInfo info) {
        Expression expression = getDirectPullUpReplaceExpression(slot, info);
        while (expression instanceof Slot) {
            Expression next = getDirectPullUpReplaceExpression((Slot) expression, info);
            if (next == null) {
                return expression;
            }
            expression = next;
        }
        return expression;
    }

    private static Expression getDirectPullUpReplaceExpression(Slot slot, PullUpInfo info) {
        for (Map.Entry<Slot, Expression> entry : info.pullUpExprReplaceMap.entrySet()) {
            if (entry.getKey().getExprId().equals(slot.getExprId())) {
                return entry.getValue();
            }
        }
        return null;
    }

    /** Create a new Project above the TopN that restores pulled-up expressions. */
    private static LogicalProject<Plan> addUpperProject(LogicalTopN topN, PullUpInfo info) {
        Map<ExprId, NamedExpression> pulledUpBySlotExprId = new HashMap<>();
        Set<ExprId> currentOutputExprIds = topN.getOutputExprIdSet();
        for (NamedExpression e : info.allPulledUpExprs) {
            pulledUpBySlotExprId.put(e.toSlot().getExprId(), resolveAliasChildIfNeeded(e, info, currentOutputExprIds));
        }

        // Use the current rewritten TopN output so pass-through slots keep
        // the ExprIds produced by the simplified child.
        List<Slot> currentOutput = topN.getOutput();
        Map<ExprId, Slot> currentOutputByExprId = new HashMap<>();
        for (Slot slot : currentOutput) {
            currentOutputByExprId.put(slot.getExprId(), slot);
        }
        List<NamedExpression> upperOutput = new ArrayList<>();
        Set<ExprId> upperOutputExprIds = new HashSet<>();
        for (int i = 0; i < info.originalTopNOutput.size(); i++) {
            Slot origSlot = info.originalTopNOutput.get(i);
            NamedExpression pulledUpExpr = pulledUpBySlotExprId.get(origSlot.getExprId());
            if (pulledUpExpr != null) {
                upperOutput.add(pulledUpExpr);
                upperOutputExprIds.add(pulledUpExpr.getExprId());
            } else {
                Slot currentSlot = currentOutputByExprId.get(origSlot.getExprId());
                if (currentSlot != null) {
                    upperOutput.add(currentSlot);
                    upperOutputExprIds.add(currentSlot.getExprId());
                } else {
                    Expression chainedExpr = getPullUpReplaceExpression(origSlot, info);
                    if (chainedExpr != null) {
                        Expression resolvedExpr = resolveExpression(chainedExpr, info, currentOutputExprIds);
                        upperOutput.add(new Alias(origSlot.getExprId(), resolvedExpr, origSlot.getName()));
                        upperOutputExprIds.add(origSlot.getExprId());
                    } else {
                        // Slot was lost during simplifyProject — pass through directly.
                        // TopN is a pass-through node; the computation for this slot
                        // exists below the TopN even if the intermediate project lost it.
                        if (upperOutputExprIds.add(origSlot.getExprId())) {
                            upperOutput.add(origSlot);
                        }
                    }
                }
            }
        }

        return new LogicalProject<>(ImmutableList.copyOf(upperOutput), topN);
    }

    private static NamedExpression resolveAliasChildIfNeeded(NamedExpression expr, PullUpInfo info,
            Set<ExprId> availableExprIds) {
        if (!(expr instanceof Alias)) {
            return expr;
        }
        Expression resolvedChild = resolveExpression(expr.child(0), info, availableExprIds);
        if (resolvedChild.equals(expr.child(0))) {
            return expr;
        }
        return new Alias(expr.getExprId(), resolvedChild, expr.getName());
    }

    private static List<Slot> resolveInputSlots(Expression expr, PullUpInfo info,
            Set<ExprId> availableExprIds) {
        return ImmutableList.copyOf(resolveExpression(expr, info, availableExprIds).getInputSlots());
    }

    private static Expression resolveExpression(Expression expression, PullUpInfo info,
            Set<ExprId> availableExprIds) {
        Expression resolved = replaceUnavailableSlots(expression, info, availableExprIds);
        while (!resolved.equals(expression)) {
            expression = resolved;
            resolved = replaceUnavailableSlots(expression, info, availableExprIds);
        }
        return resolved;
    }

    private static Expression replaceUnavailableSlots(Expression expression, PullUpInfo info,
            Set<ExprId> availableExprIds) {
        Map<Slot, Expression> replaceMap = new LinkedHashMap<>();
        for (Map.Entry<Slot, Expression> entry : info.pullUpExprReplaceMap.entrySet()) {
            if (!availableExprIds.contains(entry.getKey().getExprId())) {
                replaceMap.put(entry.getKey(), entry.getValue());
            }
        }
        return ExpressionUtils.replace(expression, replaceMap);
    }
}
