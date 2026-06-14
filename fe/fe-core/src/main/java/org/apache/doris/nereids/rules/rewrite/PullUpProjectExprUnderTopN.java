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

import com.google.common.base.Preconditions;
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

        // Deduplicate: when nested TopNs both try to pull up the same expression
        // from the same Project, keep it only in the outermost TopN.
        deduplicatePullUps(collectorCtx);

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
        /**
         * Pulled-up expressions removed from this TopN by deduplicatePullUps because
         * an outer TopN already owns the same Project expression. The inner TopN
         * should not restore the expression itself, but it may still need to pass
         * the expression's input slots through so the outer TopN can restore it.
         *
         * <p>Example before deduplication:
         * <pre>
         * TopN outer
         *   Project(x)
         *     TopN inner
         *       Project(x = a + 1, id)
         *         Scan(a, id)
         * </pre>
         *
         * <p>Both TopNs collect {@code x = a + 1} from the same Project. Dedup keeps
         * it in the outer TopN's {@code allPulledUpExprs}, and records it in the
         * inner TopN as:
         * <pre>
         * passThroughExprByDeduplicatedExpr[x.exprId] = (x = a + 1)
         * </pre>
         *
         * <p>The inner TopN can then pass {@code a} through instead of producing
         * {@code x}, and the outer TopN restores {@code x = a + 1} above itself.
         */
        final Map<ExprId, NamedExpression> passThroughExprByDeduplicatedExpr = new HashMap<>();

        PullUpInfo(LogicalTopN topN) {
            this.topN = topN;
            this.originalTopNOutput = ImmutableList.copyOf(topN.getOutput());
        }

        void addPulledUpExpr(LogicalProject<? extends Plan> project, NamedExpression expr) {
            allPulledUpExprs.add(expr);
            projectToPulledUpExprs.computeIfAbsent(project, k -> new ArrayList<>()).add(expr);
            baseSlotsByExpr.put(expr.getExprId(), ImmutableList.copyOf(expr.getInputSlots()));
        }

        void addPassThroughExprForDeduplicatedExpr(NamedExpression expr) {
            passThroughExprByDeduplicatedExpr.put(expr.getExprId(), expr);
        }
    }

    /** Context shared between collector and replacer passes. */
    static class CollectorContext {
        /**
         * Use IdentityHashMap so that two different TopN nodes with the same
         * content (orderKeys, limit, offset) are treated as distinct keys.
         * LogicalTopN.equals() is content-based, which would cause unrelated
         * TopN nodes to collide in a regular HashMap/LinkedHashMap.
         */
        final Map<LogicalTopN, PullUpInfo> topNToPullUpInfo = new IdentityHashMap<>();
        /**
         * Maintain insertion order for deterministic outer-to-inner iteration
         * in dedup and other passes. The Collector visits the plan top-down,
         * so the order is naturally outer-before-inner.
         */
        final List<LogicalTopN> topNOrder = new ArrayList<>();
        final Map<Slot, Expression> pullUpExprReplaceMap = new LinkedHashMap<>();
        /**
         * When collectFromNode encounters a nested TopN, it saves the current
         * blockedExprIds (accumulated from outer nodes) so that visitLogicalTopN
         * for the inner TopN can merge them into its fresh blocked set.
         */
        final Map<LogicalTopN, Set<ExprId>> outerBlockedByTopN = new IdentityHashMap<>();
        int cteProducerDepth = 0;

        boolean hasPullUpInfo(LogicalTopN topN) {
            return topNToPullUpInfo.containsKey(topN);
        }

        PullUpInfo getPullUpInfo(LogicalTopN topN) {
            return topNToPullUpInfo.get(topN);
        }

        void addPullUpInfo(LogicalTopN topN, PullUpInfo info) {
            topNToPullUpInfo.put(topN, info);
            topNOrder.add(topN);
        }

        void addPullUpExprReplace(NamedExpression expr) {
            if (expr instanceof Alias) {
                pullUpExprReplaceMap.putIfAbsent(expr.toSlot(), expr.child(0));
            }
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
            // If this is a nested TopN, merge in the outer blocked set that was
            // saved by collectFromNode when it encountered this TopN. This
            // ensures that slots consumed by outer operators (e.g. join
            // conditions above this TopN) also block pull-up from projects
            // under this TopN.
            Set<ExprId> outerBlocked = context.outerBlockedByTopN.remove(topN);
            if (outerBlocked != null) {
                blockedExprIds.addAll(outerBlocked);
            }
            collectFromNode((Plan) topN.child(0), info, blockedExprIds, context);
            if (!info.allPulledUpExprs.isEmpty()) {
                for (NamedExpression expr : info.allPulledUpExprs) {
                    context.addPullUpExprReplace(expr);
                }
                context.addPullUpInfo(topN, info);
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
    private static void collectFromNode(Plan node, PullUpInfo info, Set<ExprId> blockedExprIds,
            CollectorContext context) {
        if (node instanceof LogicalProject) {
            LogicalProject<? extends Plan> project = (LogicalProject<? extends Plan>) node;
            for (NamedExpression ne : project.getProjects()) {
                if (blockedExprIds.contains(ne.getExprId())) {
                    for (Slot slot : ne.getInputSlots()) {
                        blockedExprIds.add(slot.getExprId());
                    }
                } else {
                    if (canPullUp(ne)) {
                        info.addPulledUpExpr(project, ne);
                    } else if (ne instanceof Alias && ne.child(0) instanceof Slot) {
                        // Chain: this intermediate project renames a slot that may
                        // come from a pulled-up expression deeper in the tree.
                        // Always register Slot(alias.toSlot()) → child Slot so that
                        // getPullUpReplaceExpression can resolve the chain later.
                        context.pullUpExprReplaceMap.putIfAbsent(ne.toSlot(), ne.child(0));
                    }
                }
            }
            // Continue into the project's child. Chained projects are all visited.
            collectFromNode((Plan) project.child(0), info, blockedExprIds, context);
            return;
        }

        if (node instanceof LogicalTopN) {
            LogicalTopN inner = (LogicalTopN) node;
            // Save the current blockedExprIds (accumulated from outer nodes
            // such as outer TopN + intermediate Joins) so that the inner
            // TopN's own visitLogicalTopN can merge them into its fresh
            // blocked set. Without this, outer join condition slots would
            // not block pull-up from projects under the inner TopN.
            context.outerBlockedByTopN.put(inner, new HashSet<>(blockedExprIds));
            // Stop traversal here — do NOT collect expressions from under
            // the inner TopN using the outer TopN's PullUpInfo. The inner
            // TopN has its own visitLogicalTopN which will handle its subtree
            // independently. If the outer TopN were to collect expressions
            // from under the inner TopN, dedup would move them to the outer
            // TopN and the inner TopN would only see passThroughExprs. The
            // passThrough mechanism only propagates base slots, which breaks
            // downstream Projects that reference the original expression slot
            // by ExprId (e.g. a "c1 AS c2" rename between the two TopNs).
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
                collectFromNode(child, info, newBlocked, context);
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
            collectFromNode(child, info, newBlocked, context);
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

    /**
     * Deduplicate pull-up expressions so that each expression in a Project is only
     * pulled up to the outermost TopN that collects it.
     *
     * <p>Iteration uses {@link CollectorContext#topNOrder} which preserves the
     * Collector's top-down visit order (outer-to-inner). We keep the first
     * occurrence of each (project-reference, exprId) pair and remove duplicates
     * from inner TopNs.
     */
    private static void deduplicatePullUps(CollectorContext context) {
        // Use IdentityHashMap because we need to distinguish Project nodes by object
        // reference, not by content equality.
        Map<LogicalProject<? extends Plan>, Set<ExprId>> handled = new IdentityHashMap<>();

        for (LogicalTopN topN : context.topNOrder) {
            PullUpInfo info = context.topNToPullUpInfo.get(topN);
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
                info.addPassThroughExprForDeduplicatedExpr(expr);
                info.allPulledUpExprs.remove(expr);
                for (List<NamedExpression> list : info.projectToPulledUpExprs.values()) {
                    list.removeIf(e -> e == expr);
                }
                info.baseSlotsByExpr.remove(expr.getExprId());
            }
            info.projectToPulledUpExprs.entrySet().removeIf(e -> e.getValue().isEmpty());
        }
    }

    // =========================================================================
    // Pass 2: Replacer (bottom-up)
    // =========================================================================

    static class Replacer extends DefaultPlanRewriter<CollectorContext> {

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, CollectorContext context) {
            LogicalProject<? extends Plan> rewritten = (LogicalProject<? extends Plan>) visit(project, context);

            // Collect ALL pulled-up expressions across ALL PullUpInfos for this
            // project. After dedup, each expression belongs to exactly one TopN
            // (the outermost one that can pull it up). The project needs to be
            // simplified by removing all of them, exposing their base slots once.
            List<NamedExpression> allPulledUpExprs = collectAllPulledUpExprs(context, rewritten);
            if (allPulledUpExprs.isEmpty() && rewritten != project
                    && rewritten.getProjects().equals(project.getProjects())) {
                allPulledUpExprs = collectAllPulledUpExprs(context, project);
            }
            return simplifyProject(rewritten, allPulledUpExprs, context);
        }

        @Override
        public Plan visitLogicalTopN(LogicalTopN topN, CollectorContext context) {
            LogicalTopN rewritten = (LogicalTopN) visit(topN, context);
            // If the subtree was not modified by the replacer, no Projects
            // below were simplified, so the pulled-up expressions' base
            // slots may not be exposed.  Skip addUpperProject to avoid
            // computing the expression redundantly above AND below.
            if (rewritten == topN) {
                return rewritten;
            }
            PullUpInfo info = context.getPullUpInfo(topN);
            if (info == null) {
                return rewritten;
            }
            if (info.allPulledUpExprs.isEmpty()
                    && info.passThroughExprByDeduplicatedExpr.isEmpty()) {
                return rewritten;
            }
            return addUpperProject(rewritten, info, context);
        }
    }

    /**
     * Collect all pulled-up expressions across all PullUpInfos for a project.
     * After dedup each expression belongs to exactly one TopN, but the project
     * must be simplified by removing all of them at once.
     */
    private static List<NamedExpression> collectAllPulledUpExprs(
            CollectorContext context, LogicalProject<?> project) {
        List<NamedExpression> result = new ArrayList<>();
        for (LogicalTopN topN : context.topNOrder) {
            PullUpInfo info = context.topNToPullUpInfo.get(topN);
            List<NamedExpression> exprs = info.projectToPulledUpExprs.get(project);
            if (exprs != null) {
                result.addAll(exprs);
            }
        }
        return result;
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
            CollectorContext context) {
        Set<ExprId> childOutputExprIds = ((Plan) project.child(0)).getOutputExprIdSet();
        List<Expression> passThroughExprs = collectUnavailablePullUpExprs(project, context, childOutputExprIds);
        // When projects below the TopN were simplified, an above-TopN project may
        // still hold Aliases whose children are intermediate SlotReferences (e.g.
        // element_at results) that were removed from the child output. Even when
        // pulledUpExprs and passThroughExprs are empty, we must still process such
        // Aliases — replace them with SlotReferences pointing to the child output
        // where the computation was restored by addUpperProject.
        boolean hasAliasWithUnavailableChild = false;
        if (pulledUpExprs.isEmpty() && passThroughExprs.isEmpty()) {
            for (NamedExpression ne : project.getProjects()) {
                if (ne instanceof Alias && ne.child(0) instanceof Slot
                        && !childOutputExprIds.contains(((Slot) ne.child(0)).getExprId())) {
                    hasAliasWithUnavailableChild = true;
                    break;
                }
            }
            if (!hasAliasWithUnavailableChild) {
                return project;
            }
        }

        Set<ExprId> pulledUpExprIds = new HashSet<>();
        for (NamedExpression ne : pulledUpExprs) {
            pulledUpExprIds.add(ne.getExprId());
        }

        List<NamedExpression> simplified = new ArrayList<>();
        Set<ExprId> existingExprIds = new HashSet<>();
        for (NamedExpression ne : project.getProjects()) {
            if (!pulledUpExprIds.contains(ne.getExprId())
                    && !isUnavailablePullUpSlot(ne, context, childOutputExprIds)) {
                NamedExpression resolved = resolveAliasChildIfNeeded(ne, context, childOutputExprIds);

                // This rule pulls non-trivial expressions above TopN so the
                // TopN only carries the slots needed to restore them later.
                //
                // A common shape after nested-column or variant rewrites is a
                // chain of adjacent Projects:
                //
                //   TopN (output: id#0, v1#4)
                //     Project1(id#0, slotref#10 AS v1#4)
                //       Project2(id#0, element_at(sa#1, 'fa') AS slotref#10)
                //         Scan(id#0, sa#1)
                //
                // Project1 is only a rename, while Project2 contains the
                // expression that can be restored above TopN.
                // During collection, we record both links:
                //
                //   v1#4      -> slotref#10
                //   slotref#10 -> element_at(sa#1, 'fa')
                // after pullup, the expected plan is
                //
                //  ProjectUpper(id#0, element_at(sa#1, 'fa') AS v1#4)
                //      TopN (output: id#0, sa#1)
                //          Project1'(id#0, sa#1)
                //              Project2'(id#0, sa#1)
                //                  Scan(id#0, sa#1)
                //
                // If Project1 is being simplified after the expression in
                // Project2 was pulled up, Project1 should no longer compute
                // v1#4 itself. The upper Project added by addUpperProject will
                // compute element_at(sa#1, 'fa') AS v1#4 above TopN instead.
                // Project1 must therefore output the input slot sa#1, not
                // element_at(sa#1, 'fa') AS v1#4. Otherwise TopN would output
                // v1#4 but not sa#1, and the upper Project would fail the plan
                // validity check because its input slot sa#1 is not produced by
                // its child.
                boolean pulledAbove = false;
                if (resolved != ne) {
                    Expression replaceExpr = getPullUpReplaceExpression(
                            resolved.toSlot(), context);
                    if (replaceExpr != null && !(replaceExpr instanceof Slot)) {
                        pulledAbove = true;
                        // Expression pulled above → only base slots stay
                        // in the lower project (for lazy mat through TopN).
                        for (Slot slot : resolved.getInputSlots()) {
                            ExprId slotEid = slot.getExprId();
                            if (!existingExprIds.contains(slotEid)
                                    && childOutputExprIds.contains(slotEid)) {
                                simplified.add(slot);
                                existingExprIds.add(slotEid);
                            }
                        }
                    }
                }
                if (!pulledAbove) {
                    if (!existingExprIds.contains(resolved.getExprId())) {
                        simplified.add(resolved);
                        existingExprIds.add(resolved.getExprId());
                    }
                }
            }
        }

        for (NamedExpression pulledUpExpr : pulledUpExprs) {
            for (Slot baseSlot : resolveInputSlots(pulledUpExpr.child(0), context, childOutputExprIds)) {
                if (!existingExprIds.contains(baseSlot.getExprId())) {
                    simplified.add(baseSlot);
                    existingExprIds.add(baseSlot.getExprId());
                }
            }
        }
        for (Expression passThroughExpr : passThroughExprs) {
            for (Slot baseSlot : resolveInputSlots(passThroughExpr, context, childOutputExprIds)) {
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
            LogicalProject<? extends Plan> project, CollectorContext context, Set<ExprId> childOutputExprIds) {
        List<Expression> passThroughExprs = new ArrayList<>();
        for (NamedExpression ne : project.getProjects()) {
            if (isUnavailablePullUpSlot(ne, context, childOutputExprIds)) {
                passThroughExprs.add(getPullUpReplaceExpression((Slot) ne, context));
            }
        }
        return passThroughExprs;
    }

    private static boolean isUnavailablePullUpSlot(
            NamedExpression ne, CollectorContext context, Set<ExprId> childOutputExprIds) {
        return ne instanceof Slot
                && !childOutputExprIds.contains(ne.getExprId())
                && getPullUpReplaceExpression((Slot) ne, context) != null;
    }

    private static Expression getPullUpReplaceExpression(Slot slot, CollectorContext context) {
        Expression result = context.pullUpExprReplaceMap.get(slot);

        // Follow Slot-to-Slot rename chains recorded from adjacent Projects:
        //   v1#4      -> slotref#10
        //   slotref#10 -> element_at(sa#1, 'fa')
        //
        // getPullUpReplaceExpression(v1#4) returns element_at(sa#1, 'fa').
        // It only recurses while the whole replacement is a Slot. It does not
        // rewrite slots inside a non-Slot replacement. For example, even if
        // another mapping has sa#1 -> raw#0, this function returns
        // element_at(sa#1, 'fa'), not element_at(raw#0, 'fa').
        if (result instanceof Slot) {
            Expression chained = getPullUpReplaceExpression((Slot) result, context);
            if (chained != null) {
                return chained;
            }
        }
        return result;
    }

    /** Create a new Project above the TopN that restores pulled-up expressions. */
    private static LogicalProject<Plan> addUpperProject(LogicalTopN topN, PullUpInfo info,
            CollectorContext context) {
        Map<ExprId, NamedExpression> pulledUpBySlotExprId = new HashMap<>();
        Set<ExprId> currentOutputExprIds = topN.getOutputExprIdSet();
        for (NamedExpression e : info.allPulledUpExprs) {
            pulledUpBySlotExprId.put(e.toSlot().getExprId(),
                    resolveAliasChildIfNeeded(e, context, currentOutputExprIds));
        }

        // Use the current (possibly rewritten) TopN's output so that slots
        // whose expressions were deduplicated to an outer TopN reference
        // the correct post-simplification ExprIds instead of stale ones.
        List<Slot> currentOutput = topN.getOutput();
        Map<ExprId, Slot> currentOutputByExprId = new HashMap<>();
        for (Slot slot : currentOutput) {
            currentOutputByExprId.put(slot.getExprId(), slot);
        }
        List<NamedExpression> upperOutput = new ArrayList<>();
        Set<ExprId> upperOutputExprIds = new HashSet<>();
        Set<ExprId> passThroughOutputExprIds = new HashSet<>();
        for (int i = 0; i < info.originalTopNOutput.size(); i++) {
            Slot origSlot = info.originalTopNOutput.get(i);
            NamedExpression pulledUpExpr = pulledUpBySlotExprId.get(origSlot.getExprId());
            if (pulledUpExpr != null) {
                upperOutput.add(pulledUpExpr);
                upperOutputExprIds.add(pulledUpExpr.getExprId());
            } else {
                Slot currentSlot = currentOutputByExprId.get(origSlot.getExprId());
                if (currentSlot != null) {
                    if (!passThroughOutputExprIds.contains(currentSlot.getExprId())) {
                        upperOutput.add(currentSlot);
                        upperOutputExprIds.add(currentSlot.getExprId());
                    }
                    continue;
                }
                // The original TopN output may be a rename whose child was
                // removed when a lower Project was simplified. For example:
                //
                //   Before rewrite:
                //     TopN (output: id#0, v1#4)
                //       Project(id#0, slotref#10 AS v1#4)
                //         Project(id#0, element_at(sa#1, 'fa') AS slotref#10)
                //           Scan(id#0, sa#1)
                //
                //   After simplifying Projects below TopN:
                //     TopN (output: id#0, sa#1)
                //       Project(id#0, sa#1)
                //         Scan(id#0, sa#1)
                //
                // The rewritten TopN no longer produces v1#4, but the final
                // query still expects v1#4. In that case, use the recorded
                // chain v1#4 -> slotref#10 -> element_at(sa#1, 'fa') and add
                // a synthetic Alias above TopN to restore v1#4. If the
                // rewritten TopN still produces origSlot directly, the earlier
                // currentSlot branch must pass it through instead of expanding
                // the chain; otherwise a still-available forwarding slot such
                // as d2 can be incorrectly expanded to dt['b'] above TopN.
                Expression chainedExpr = getPullUpReplaceExpression(origSlot, context);
                if (chainedExpr != null && !(chainedExpr instanceof Slot)
                        && !upperOutputExprIds.contains(origSlot.getExprId())) {
                    NamedExpression synthetic = new Alias(
                            origSlot.getExprId(), chainedExpr, origSlot.getName());
                    upperOutput.add(synthetic);
                    upperOutputExprIds.add(synthetic.getExprId());
                    continue;
                }
                NamedExpression passThroughExpr = info.passThroughExprByDeduplicatedExpr.get(origSlot.getExprId());
                if (passThroughExpr != null) {
                    List<Slot> passThroughSlots = resolveInputSlots(
                            passThroughExpr.child(0), context, currentOutputExprIds);
                    addPassThroughSlots(upperOutput, upperOutputExprIds, passThroughOutputExprIds,
                            currentOutputByExprId, passThroughSlots);
                } else {
                    // When NestedColumnPruning has run before this rule, the
                    // originalTopNOutput may contain intermediate expression
                    // slots (element_at results) that were simplified away by
                    // simplifyProject. These slots are no longer produced by
                    // the rewritten TopN's child and would cause CheckAfterRewrite
                    // failures if passed through. Skip them — the corresponding
                    // computation is already covered by the pulled-up Aliases
                    // or by the base slots added during simplification.
                }
            }
        }

        // Add current TopN output slots that are not already in upperOutput
        // and were part of the original TopN output (i.e. not base structs
        // injected by simplifyProject solely for lazy mat).
        for (Slot slot : currentOutput) {
            if (!upperOutputExprIds.contains(slot.getExprId())
                    && info.originalTopNOutput.contains(slot)) {
                upperOutput.add(slot);
                upperOutputExprIds.add(slot.getExprId());
            }
        }

        return new LogicalProject<>(ImmutableList.copyOf(upperOutput), topN);
    }

    private static NamedExpression resolveAliasChildIfNeeded(NamedExpression expr, CollectorContext context,
            Set<ExprId> availableExprIds) {
        if (!(expr instanceof Alias)) {
            return expr;
        }
        Expression resolvedChild = resolveExpression(expr.child(0), context, availableExprIds);
        if (resolvedChild.equals(expr.child(0))) {
            return expr;
        }
        return new Alias(expr.getExprId(), resolvedChild, expr.getName());
    }

    private static List<Slot> resolveInputSlots(Expression expr, CollectorContext context,
            Set<ExprId> availableExprIds) {
        return ImmutableList.copyOf(resolveExpression(expr, context, availableExprIds).getInputSlots());
    }

    private static Expression resolveExpression(Expression expression, CollectorContext context,
            Set<ExprId> availableExprIds) {
        Expression resolved = replaceUnavailableSlots(expression, context, availableExprIds);
        while (!resolved.equals(expression)) {
            expression = resolved;
            resolved = replaceUnavailableSlots(expression, context, availableExprIds);
        }
        return resolved;
    }

    private static Expression replaceUnavailableSlots(Expression expression, CollectorContext context,
            Set<ExprId> availableExprIds) {
        Map<Slot, Expression> replaceMap = new LinkedHashMap<>();
        for (Map.Entry<Slot, Expression> entry : context.pullUpExprReplaceMap.entrySet()) {
            if (!availableExprIds.contains(entry.getKey().getExprId())) {
                replaceMap.put(entry.getKey(), entry.getValue());
            }
        }
        return ExpressionUtils.replace(expression, replaceMap);
    }

    private static void addPassThroughSlots(
            List<NamedExpression> upperOutput,
            Set<ExprId> upperOutputExprIds,
            Set<ExprId> passThroughOutputExprIds,
            Map<ExprId, Slot> currentOutputByExprId,
            List<Slot> passThroughSlots) {
        for (Slot passThroughSlot : passThroughSlots) {
            Slot currentSlot = currentOutputByExprId.get(passThroughSlot.getExprId());
            Preconditions.checkState(currentSlot != null,
                    "Pass-through slot %s should be produced by rewritten TopN", passThroughSlot);
            if (upperOutputExprIds.add(currentSlot.getExprId())) {
                upperOutput.add(currentSlot);
            }
            passThroughOutputExprIds.add(currentSlot.getExprId());
        }
    }
}
