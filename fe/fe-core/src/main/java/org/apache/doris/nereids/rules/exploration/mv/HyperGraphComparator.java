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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.ConflictRulesMaker;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.FilterEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughJoin;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * HyperGraphComparator
 */
public class HyperGraphComparator {
    // This second join can be inferred to the first join by map value,
    // The map value means the child's should be no-nullable
    static Map<Pair<JoinType, JoinType>, Pair<Boolean, Boolean>> canInferredJoinTypeMap = ImmutableMap
            .<Pair<JoinType, JoinType>, Pair<Boolean, Boolean>>builder()
            .put(Pair.of(JoinType.LEFT_SEMI_JOIN, JoinType.INNER_JOIN), Pair.of(false, false))
            .put(Pair.of(JoinType.RIGHT_SEMI_JOIN, JoinType.INNER_JOIN), Pair.of(false, false))
            .put(Pair.of(JoinType.INNER_JOIN, JoinType.LEFT_OUTER_JOIN), Pair.of(false, true))
            .put(Pair.of(JoinType.INNER_JOIN, JoinType.RIGHT_OUTER_JOIN), Pair.of(true, false))
            .put(Pair.of(JoinType.INNER_JOIN, JoinType.FULL_OUTER_JOIN), Pair.of(true, true))
            .put(Pair.of(JoinType.LEFT_OUTER_JOIN, JoinType.FULL_OUTER_JOIN), Pair.of(true, false))
            .put(Pair.of(JoinType.RIGHT_OUTER_JOIN, JoinType.FULL_OUTER_JOIN), Pair.of(false, true))
            .build();

    // record inferred edges when comparing mv
    private final HyperGraph queryHyperGraph;
    private final HyperGraph viewHyperGraph;
    private final Map<Edge, List<? extends Expression>> pullUpQueryExprWithEdge = new HashMap<>();
    private final Map<Edge, List<? extends Expression>> pullUpViewExprWithEdge = new HashMap<>();
    private final LogicalCompatibilityContext logicalCompatibilityContext;
    private final Map<JoinEdge, Pair<JoinType, Set<Slot>>> inferredViewEdgeWithCond = new HashMap<>();
    private List<JoinEdge> viewJoinEdgesAfterInferring;
    private List<FilterEdge> viewFilterEdgesAfterInferring;

    public HyperGraphComparator(HyperGraph queryHyperGraph, HyperGraph viewHyperGraph,
            LogicalCompatibilityContext logicalCompatibilityContext) {
        this.queryHyperGraph = queryHyperGraph;
        this.viewHyperGraph = viewHyperGraph;
        this.logicalCompatibilityContext = logicalCompatibilityContext;
    }

    /**
     * compare hypergraph
     *
     * @param viewHG the compared hyper graph
     * @return Comparison result
     */
    public static ComparisonResult isLogicCompatible(HyperGraph queryHG, HyperGraph viewHG,
            LogicalCompatibilityContext ctx) {
        return new HyperGraphComparator(queryHG, viewHG, ctx).isLogicCompatible();
    }

    private ComparisonResult isLogicCompatible() {
        // 1 compare nodes
        boolean nodeMatches = logicalCompatibilityContext.getQueryToViewNodeMapping().entrySet()
                .stream().allMatch(e -> compareNodeWithExpr(e.getKey(), e.getValue()));
        if (!nodeMatches) {
            return ComparisonResult.newInvalidResWithErrorMessage("StructInfoNode are not compatible\n");
        }

        // 2 try to construct a map which can be mapped from edge to edge
        Map<Edge, Edge> queryToView = constructQueryToViewMapWithExpr();
        if (!makeViewJoinCompatible(queryToView)) {
            return ComparisonResult.newInvalidResWithErrorMessage("Join types are not compatible\n");
        }
        refreshViewEdges();

        // 3. compare them by expression and nodes. Note compare edges after inferring for nodes
        boolean matchNodes = queryToView.entrySet().stream()
                .allMatch(e -> compareEdgeWithNode(e.getKey(), e.getValue()));
        if (!matchNodes) {
            return ComparisonResult.newInvalidResWithErrorMessage("Join nodes are not compatible\n");
        }
        queryToView.forEach(this::compareEdgeWithExpr);

        // 1. process residual edges
        Sets.difference(getQueryJoinEdgeSet(), queryToView.keySet())
                .forEach(e -> pullUpQueryExprWithEdge.put(e, e.getExpressions()));
        Sets.difference(getQueryFilterEdgeSet(), queryToView.keySet())
                .forEach(e -> pullUpQueryExprWithEdge.put(e, e.getExpressions()));
        Sets.difference(getViewJoinEdgeSet(), Sets.newHashSet(queryToView.values()))
                .forEach(e -> pullUpViewExprWithEdge.put(e, e.getExpressions()));
        Sets.difference(getViewFilterEdgeSet(), Sets.newHashSet(queryToView.values()))
                .forEach(e -> pullUpViewExprWithEdge.put(e, e.getExpressions()));

        return buildComparisonRes();
    }

    private boolean compareNodeWithExpr(StructInfoNode query, StructInfoNode view) {
        List<Set<Expression>> queryExprSetList = query.getExprSetList();
        List<Set<Expression>> viewExprSetList = view.getExprSetList();
        if (queryExprSetList == null || viewExprSetList == null
                || queryExprSetList.size() != viewExprSetList.size()) {
            return false;
        }
        int size = queryExprSetList.size();
        for (int i = 0; i < size; i++) {
            Set<Expression> mappingQueryExprSet = queryExprSetList.get(i).stream()
                    .map(e -> logicalCompatibilityContext.getQueryToViewEdgeExpressionMapping().get(e))
                    .collect(Collectors.toSet());
            if (!mappingQueryExprSet.equals(viewExprSetList.get(i))) {
                return false;
            }
        }
        return true;
    }

    private ComparisonResult buildComparisonRes() {
        ComparisonResult.Builder builder = new ComparisonResult.Builder();
        for (Entry<Edge, List<? extends Expression>> e : pullUpQueryExprWithEdge.entrySet()) {
            List<? extends Expression> rawFilter = e.getValue().stream()
                    .filter(expr -> !ExpressionUtils.isInferred(expr))
                    .collect(Collectors.toList());
            if (!rawFilter.isEmpty() && !canPullUp(e.getKey())) {
                return ComparisonResult.newInvalidResWithErrorMessage(getErrorMessage() + "\nwith error edge " + e);
            }
            builder.addQueryExpressions(rawFilter);
        }
        for (Entry<Edge, List<? extends Expression>> e : pullUpViewExprWithEdge.entrySet()) {
            List<? extends Expression> rawFilter = e.getValue().stream()
                    .filter(expr -> !ExpressionUtils.isInferred(expr))
                    .collect(Collectors.toList());
            if (!rawFilter.isEmpty() && !canPullUp(getViewEdgeAfterInferring(e.getKey()))) {
                return ComparisonResult.newInvalidResWithErrorMessage(getErrorMessage() + "\nwith error edge\n" + e);
            }
            builder.addViewExpressions(rawFilter);
        }
        for (Pair<JoinType, Set<Slot>> inferredCond : inferredViewEdgeWithCond.values()) {
            builder.addViewNoNullableSlot(inferredCond.second);
        }
        return builder.build();
    }

    /**
     * get error message
     */
    public String getErrorMessage() {
        return String.format(
                "graph logical is not equal\n query join edges is\n %s,\n view join edges is\n %s,\n"
                        + "query filter edges\n is %s,\nview filter edges\n is %s\n"
                        + "inferred edge with conditions\n %s",
                getQueryJoinEdges(),
                getViewJoinEdges(),
                getQueryFilterEdges(),
                getViewFilterEdges(),
                inferredViewEdgeWithCond);
    }

    private Edge getViewEdgeAfterInferring(Edge edge) {
        if (edge instanceof JoinEdge) {
            return viewJoinEdgesAfterInferring.get(edge.getIndex());
        } else {
            return viewFilterEdgesAfterInferring.get(edge.getIndex());
        }
    }

    private boolean canPullUp(Edge edge) {
        // Only inner join and filter with none rejectNodes can be pull up
        if (edge instanceof JoinEdge && !((JoinEdge) edge).getJoinType().isInnerJoin()) {
            return false;
        }
        boolean pullFromLeft = edge.getLeftRejectEdge().stream()
                .map(e -> inferredViewEdgeWithCond.getOrDefault(e, Pair.of(e.getJoinType(), null)))
                .allMatch(e -> canPullFromLeft(edge, e.first));
        boolean pullFromRight = edge.getRightRejectEdge().stream()
                .map(e -> inferredViewEdgeWithCond.getOrDefault(e, Pair.of(e.getJoinType(), null)))
                .allMatch(e -> canPullFromRight(edge, e.first));
        return pullFromLeft && pullFromRight;
    }

    private boolean canPullFromLeft(Edge bottomEdge, JoinType topJoinType) {
        if (bottomEdge instanceof FilterEdge) {
            return PushDownFilterThroughJoin.COULD_PUSH_THROUGH_LEFT.contains(topJoinType);
        } else if (bottomEdge instanceof JoinEdge) {
            return JoinType.isAssoc(((JoinEdge) bottomEdge).getJoinType(), topJoinType)
                    || JoinType.isLAssoc(((JoinEdge) bottomEdge).getJoinType(), topJoinType);
        }
        return false;
    }

    private boolean canPullFromRight(Edge bottomEdge, JoinType topJoinType) {
        if (bottomEdge instanceof FilterEdge) {
            return PushDownFilterThroughJoin.COULD_PUSH_THROUGH_RIGHT.contains(topJoinType);
        } else if (bottomEdge instanceof JoinEdge) {
            return JoinType.isAssoc(topJoinType, ((JoinEdge) bottomEdge).getJoinType())
                    || JoinType.isRAssoc(topJoinType, ((JoinEdge) bottomEdge).getJoinType());
        }
        return false;
    }

    private List<JoinEdge> getQueryJoinEdges() {
        return queryHyperGraph.getJoinEdges();
    }

    private Set<JoinEdge> getQueryJoinEdgeSet() {
        return ImmutableSet.copyOf(queryHyperGraph.getJoinEdges());
    }

    private List<FilterEdge> getQueryFilterEdges() {
        return queryHyperGraph.getFilterEdges();
    }

    private Set<FilterEdge> getQueryFilterEdgeSet() {
        return ImmutableSet.copyOf(queryHyperGraph.getFilterEdges());
    }

    private List<Edge> getQueryEdges() {
        return ImmutableList.<Edge>builder()
                .addAll(getQueryJoinEdges())
                .addAll(getQueryFilterEdges()).build();
    }

    private boolean makeViewJoinCompatible(Map<Edge, Edge> queryToView) {
        for (Entry<Edge, Edge> entry : queryToView.entrySet()) {
            if (entry.getKey() instanceof JoinEdge && entry.getValue() instanceof JoinEdge) {
                boolean res = compareJoinEdgeOrInfer((JoinEdge) entry.getKey(), (JoinEdge) entry.getValue());
                if (!res) {
                    return false;
                }
            }
        }
        return true;
    }

    private Set<FilterEdge> getViewFilterEdgeSet() {
        return ImmutableSet.copyOf(viewHyperGraph.getFilterEdges());
    }

    private Set<JoinEdge> getViewJoinEdgeSet() {
        return ImmutableSet.copyOf(viewHyperGraph.getJoinEdges());
    }

    private List<JoinEdge> getViewJoinEdges() {
        return viewHyperGraph.getJoinEdges();
    }

    private List<FilterEdge> getViewFilterEdges() {
        return viewHyperGraph.getFilterEdges();
    }

    private List<Edge> getViewEdges() {
        return ImmutableList.<Edge>builder()
                .addAll(getViewJoinEdges())
                .addAll(getViewFilterEdges()).build();
    }

    private Map<Expression, Expression> getQueryToViewExprMap() {
        return logicalCompatibilityContext.getQueryToViewEdgeExpressionMapping();
    }

    private Map<Integer, Integer> getQueryToViewNodeIdMap() {
        return logicalCompatibilityContext.getQueryToViewNodeIDMapping();
    }

    private Map<Edge, Edge> constructQueryToViewMapWithExpr() {
        Map<Expression, Edge> viewExprToEdge = getViewEdges().stream()
                .flatMap(e -> e.getExpressions().stream().map(expr -> Pair.of(expr, e)))
                .collect(ImmutableMap.toImmutableMap(p -> p.first, p -> p.second));
        Map<Expression, Edge> queryExprToEdge = getQueryEdges().stream()
                .flatMap(e -> e.getExpressions().stream().map(expr -> Pair.of(expr, e)))
                .collect(ImmutableMap.toImmutableMap(p -> p.first, p -> p.second));
        return queryExprToEdge.entrySet().stream()
                .filter(entry -> viewExprToEdge.containsKey(getViewExprFromQueryExpr(entry.getKey())))
                .map(entry -> Pair.of(entry.getValue(),
                        viewExprToEdge.get(getViewExprFromQueryExpr(entry.getKey()))))
                .distinct()
                .collect(ImmutableMap.toImmutableMap(p -> p.first, p -> p.second));
    }

    private Expression getViewExprFromQueryExpr(Expression query) {
        return logicalCompatibilityContext.getQueryToViewEdgeExpressionMapping().get(query);
    }

    private void refreshViewEdges() {
        List<FilterEdge> newFilterEdges = getViewFilterEdges().stream()
                .map(FilterEdge::clear)
                .collect(ImmutableList.toImmutableList());

        List<JoinEdge> newJoinEdges = new ArrayList<>();
        for (JoinEdge joinEdge : getViewJoinEdges()) {
            JoinType newJoinType = inferredViewEdgeWithCond
                    .getOrDefault(joinEdge, Pair.of(joinEdge.getJoinType(), null)).first;
            JoinEdge newJoinEdge = joinEdge.withJoinTypeAndCleanCR(newJoinType);
            newJoinEdges.add(newJoinEdge);
            ConflictRulesMaker.makeJoinConflictRules(newJoinEdge, newJoinEdges);
            ConflictRulesMaker.makeFilterConflictRules(newJoinEdge, newJoinEdges, newFilterEdges);
        }

        viewJoinEdgesAfterInferring = ImmutableList.copyOf(newJoinEdges);
        viewFilterEdgesAfterInferring = ImmutableList.copyOf(newFilterEdges);
    }

    private boolean compareEdgeWithNode(Edge query, Edge view) {
        if (query instanceof FilterEdge && view instanceof FilterEdge) {
            return compareFilterEdgeWithNode((FilterEdge) query, viewFilterEdgesAfterInferring.get(view.getIndex()));
        } else if (query instanceof JoinEdge && view instanceof JoinEdge) {
            return compareJoinEdgeWithNode((JoinEdge) query, viewJoinEdgesAfterInferring.get(view.getIndex()));
        }
        return false;
    }

    private boolean compareFilterEdgeWithNode(FilterEdge query, FilterEdge view) {
        return rewriteQueryNodeMap(query.getReferenceNodes()) == view.getReferenceNodes();
    }

    private boolean compareJoinEdgeWithNode(JoinEdge query, JoinEdge view) {
        boolean res = false;
        if (query.getJoinType().swap() == view.getJoinType()) {
            res |= rewriteQueryNodeMap(query.getLeftExtendedNodes()) == view.getRightExtendedNodes()
                    && rewriteQueryNodeMap(query.getRightExtendedNodes()) == view.getLeftExtendedNodes();
        }
        res |= rewriteQueryNodeMap(query.getLeftExtendedNodes()) == view.getLeftExtendedNodes()
                && rewriteQueryNodeMap(query.getRightExtendedNodes()) == view.getRightExtendedNodes();
        return res;
    }

    private boolean compareJoinEdgeOrInfer(JoinEdge query, JoinEdge view) {
        if (query.getJoinType().equals(view.getJoinType())
                || canInferredJoinTypeMap.containsKey(Pair.of(query.getJoinType(), view.getJoinType()))) {
            if (tryInferEdge(query, view)) {
                return true;
            }
        }

        if (query.getJoinType().swap().equals(view.getJoinType())
                || canInferredJoinTypeMap.containsKey(Pair.of(query.getJoinType().swap(), view.getJoinType()))) {
            if (tryInferEdge(query.swap(), view)) {
                return true;
            }
        }

        return false;
    }

    private boolean tryInferEdge(JoinEdge query, JoinEdge view) {
        if (rewriteQueryNodeMap(query.getLeftRequiredNodes()) != view.getLeftRequiredNodes()
                || rewriteQueryNodeMap(query.getRightRequiredNodes()) != view.getRightRequiredNodes()) {
            return false;
        }
        if (!query.getJoinType().equals(view.getJoinType())) {
            Pair<Boolean, Boolean> noNullableChild = canInferredJoinTypeMap.getOrDefault(
                    Pair.of(query.getJoinType(), view.getJoinType()), null);
            if (noNullableChild == null) {
                return false;
            }
            Set<Slot> noNullableSlot = Sets.union(
                    noNullableChild.first ? view.getJoin().left().getOutputSet() : ImmutableSet.of(),
                    noNullableChild.second ? view.getJoin().right().getOutputSet() : ImmutableSet.of()
            );
            inferredViewEdgeWithCond.put(view, Pair.of(query.getJoinType(), noNullableSlot));
        }
        return true;
    }

    private long rewriteQueryNodeMap(long bitmap) {
        long newBitmap = LongBitmap.newBitmap();
        for (int i : LongBitmap.getIterator(bitmap)) {
            int newIdx = getQueryToViewNodeIdMap().getOrDefault(i, 0);
            newBitmap = LongBitmap.set(newBitmap, newIdx);
        }
        return newBitmap;
    }

    private void compareEdgeWithExpr(Edge query, Edge view) {
        Set<? extends Expression> queryExprSet = query.getExpressionSet();
        Set<? extends Expression> viewExprSet = view.getExpressionSet();

        Set<Expression> exprMappedOfView = new HashSet<>();
        List<Expression> residualQueryExpr = new ArrayList<>();
        for (Expression queryExpr : queryExprSet) {
            if (getQueryToViewExprMap().containsKey(queryExpr) && viewExprSet.contains(
                    getQueryToViewExprMap().get(queryExpr))) {
                exprMappedOfView.add(getQueryToViewExprMap().get(queryExpr));
            } else {
                residualQueryExpr.add(queryExpr);
            }
        }
        List<Expression> residualViewExpr = ImmutableList.copyOf(Sets.difference(viewExprSet, exprMappedOfView));
        pullUpQueryExprWithEdge.put(query, residualQueryExpr);
        pullUpViewExprWithEdge.put(query, residualViewExpr);
    }

}
