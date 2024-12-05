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
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperElement;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.FilterEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.ExpressionPosition;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughJoin;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

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
    // this records the slots which needs to reject null
    // the key is the view join edge which should reject null, the value is a pair, the first value of the pair is the
    // query join type, the second value is also a pair which left represents the slots in the left of view join that
    // should reject null, right represents the slots in the right of view join that should reject null.
    private final Map<JoinEdge, Pair<JoinType, Pair<Set<Slot>, Set<Slot>>>> inferredViewEdgeWithCond = new HashMap<>();
    private List<JoinEdge> viewJoinEdgesAfterInferring;
    private List<FilterEdge> viewFilterEdgesAfterInferring;
    private final long eliminateViewNodesMap;

    /**
     * constructor
     */
    public HyperGraphComparator(HyperGraph queryHyperGraph, HyperGraph viewHyperGraph,
            LogicalCompatibilityContext logicalCompatibilityContext) {
        this.queryHyperGraph = queryHyperGraph;
        this.viewHyperGraph = viewHyperGraph;
        this.logicalCompatibilityContext = logicalCompatibilityContext;
        this.eliminateViewNodesMap = LongBitmap.newBitmapDiff(
                viewHyperGraph.getNodesMap(),
                LongBitmap.newBitmap(logicalCompatibilityContext.getQueryToViewNodeIDMapping().values()));
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
        // 1 remove unused nodes
        if (!tryEliminateNodesAndEdge()) {
            return ComparisonResult.newInvalidResWithErrorMessage("Query and Mv has different nodes");
        }

        // 2 compare nodes
        boolean nodeMatches = logicalCompatibilityContext.getQueryToViewNodeMapping().entrySet()
                .stream().allMatch(e -> compareNodeWithExpr(e.getKey(), e.getValue()));
        if (!nodeMatches) {
            return ComparisonResult.newInvalidResWithErrorMessage("StructInfoNode are not compatible\n");
        }

        // 3 try to construct a map which can be mapped from edge to edge
        Map<Edge, Edge> queryToViewJoinEdge = constructQueryToViewJoinMapWithExpr();
        if (!makeViewJoinCompatible(queryToViewJoinEdge)) {
            return ComparisonResult.newInvalidResWithErrorMessage("Join types are not compatible\n");
        }
        refreshViewEdges();

        // 4 compare them by expression and nodes. Note compare edges after inferring for nodes
        boolean matchNodes = queryToViewJoinEdge.entrySet().stream()
                .allMatch(e -> compareEdgeWithNode(e.getKey(), e.getValue()));
        if (!matchNodes) {
            return ComparisonResult.newInvalidResWithErrorMessage("Join nodes are not compatible\n");
        }
        Map<Edge, Edge> queryToViewFilterEdge = constructQueryToViewFilterMapWithExpr();
        matchNodes = queryToViewFilterEdge.entrySet().stream()
                .allMatch(e -> compareEdgeWithNode(e.getKey(), e.getValue()));
        if (!matchNodes) {
            return ComparisonResult.newInvalidResWithErrorMessage("Join nodes are not compatible\n");
        }

        queryToViewJoinEdge.forEach(this::compareJoinEdgeWithExpr);
        queryToViewFilterEdge.forEach(this::compareFilterEdgeWithExpr);
        // 5 process residual edges
        Sets.difference(getQueryJoinEdgeSet(), queryToViewJoinEdge.keySet())
                .forEach(e -> pullUpQueryExprWithEdge.put(e, e.getExpressions()));
        Sets.difference(getQueryFilterEdgeSet(), queryToViewFilterEdge.keySet())
                .forEach(e -> pullUpQueryExprWithEdge.put(e, e.getExpressions()));
        Sets.difference(getViewJoinEdgeSet(), Sets.newHashSet(queryToViewJoinEdge.values()))
                .stream()
                .filter(e -> !LongBitmap.isOverlap(e.getReferenceNodes(), eliminateViewNodesMap))
                .forEach(e -> pullUpViewExprWithEdge.put(e, e.getExpressions()));
        Sets.difference(getViewFilterEdgeSet(), Sets.newHashSet(queryToViewFilterEdge.values()))
                .stream()
                .filter(e -> !LongBitmap.isOverlap(e.getReferenceNodes(), eliminateViewNodesMap))
                .forEach(e -> pullUpViewExprWithEdge.put(e, e.getExpressions()));

        return buildComparisonRes();
    }

    private @Nullable Plan constructViewPlan(long nodeBitmap, Set<Slot> requireOutputs) {
        if (LongBitmap.getCardinality(nodeBitmap) != 1) {
            return null;
        }
        Plan basePlan = viewHyperGraph.getNode(LongBitmap.lowestOneIndex(nodeBitmap)).getPlan();
        if (basePlan.getOutputSet().containsAll(requireOutputs)) {
            return basePlan;
        }
        List<NamedExpression> projects = viewHyperGraph
                .getNamedExpressions(nodeBitmap, basePlan.getOutputSet(), requireOutputs);
        if (projects == null) {
            return null;
        }
        return new LogicalProject<>(projects, basePlan);
    }

    private boolean canEliminatePrimaryByForeign(long primaryNodes, long foreignNodes,
            Set<Slot> primarySlots, Set<Slot> foreignSlots, JoinEdge joinEdge) {
        Plan foreign = constructViewPlan(foreignNodes, foreignSlots);
        Plan primary = constructViewPlan(primaryNodes, primarySlots);
        if (foreign == null || primary == null) {
            return false;
        }
        return JoinUtils.canEliminateByFk(joinEdge.getJoin(), primary, foreign);
    }

    private boolean canEliminateViewEdge(JoinEdge joinEdge) {
        // eliminate by unique
        if (joinEdge.getJoinType().isLeftOuterJoin() && joinEdge.isRightSimple()) {
            long eliminatedRight =
                    LongBitmap.newBitmapIntersect(joinEdge.getRightExtendedNodes(), eliminateViewNodesMap);
            if (LongBitmap.getCardinality(eliminatedRight) != 1) {
                return false;
            }
            Plan rigthPlan = constructViewPlan(joinEdge.getRightExtendedNodes(), joinEdge.getRightInputSlots());
            if (rigthPlan == null) {
                return false;
            }
            return JoinUtils.canEliminateByLeft(joinEdge.getJoin(),
                    rigthPlan.getLogicalProperties().getFunctionalDependencies());
        }
        // eliminate by pk fk
        if (joinEdge.getJoinType().isInnerJoin()) {
            if (!joinEdge.isSimple()) {
                return false;
            }
            long eliminatedLeft =
                    LongBitmap.newBitmapIntersect(joinEdge.getLeftExtendedNodes(), eliminateViewNodesMap);
            long eliminatedRight =
                    LongBitmap.newBitmapIntersect(joinEdge.getRightExtendedNodes(), eliminateViewNodesMap);
            if (LongBitmap.getCardinality(eliminatedLeft) == 0
                    && LongBitmap.getCardinality(eliminatedRight) == 1) {
                return canEliminatePrimaryByForeign(joinEdge.getRightExtendedNodes(), joinEdge.getLeftExtendedNodes(),
                        joinEdge.getRightInputSlots(), joinEdge.getLeftInputSlots(), joinEdge);
            } else if (LongBitmap.getCardinality(eliminatedLeft) == 1
                    && LongBitmap.getCardinality(eliminatedRight) == 0) {
                return canEliminatePrimaryByForeign(joinEdge.getLeftExtendedNodes(), joinEdge.getRightExtendedNodes(),
                        joinEdge.getLeftInputSlots(), joinEdge.getRightInputSlots(), joinEdge);
            }
        }
        return false;
    }

    private boolean tryEliminateNodesAndEdge() {
        boolean hasFilterEdgeAbove = viewHyperGraph.getFilterEdges().stream()
                .filter(e -> LongBitmap.getCardinality(e.getReferenceNodes()) == 1)
                .anyMatch(e -> LongBitmap.isSubset(e.getReferenceNodes(), eliminateViewNodesMap));
        if (hasFilterEdgeAbove) {
            // If there is some filter edge above the eliminated node, we should rebuild a plan
            // Right now, just reject it.
            return false;
        }
        return viewHyperGraph.getJoinEdges().stream()
                .filter(joinEdge -> LongBitmap.isOverlap(joinEdge.getReferenceNodes(), eliminateViewNodesMap))
                .allMatch(this::canEliminateViewEdge);
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
            Set<Expression> queryExpressions = queryExprSetList.get(i);
            Set<Expression> mappingQueryExprSet = new HashSet<>();
            for (Expression queryExpression : queryExpressions) {
                Optional<Expression> mappingViewExprByQueryExpr = getMappingViewExprByQueryExpr(queryExpression, query,
                        this.logicalCompatibilityContext,
                        ExpressionPosition.NODE);
                if (!mappingViewExprByQueryExpr.isPresent()) {
                    return false;
                }
                mappingQueryExprSet.add(mappingViewExprByQueryExpr.get());
            }
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
        for (Pair<JoinType, Pair<Set<Slot>, Set<Slot>>> inferredCond : inferredViewEdgeWithCond.values()) {
            builder.addViewNoNullableSlot(inferredCond.second);
        }
        builder.addQueryAllPulledUpExpressions(
                getQueryFilterEdges().stream()
                        .filter(this::canPullUp)
                        .flatMap(filter -> filter.getExpressions().stream()).collect(Collectors.toList()));
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

    private Map<Integer, Integer> getQueryToViewNodeIdMap() {
        return logicalCompatibilityContext.getQueryToViewNodeIDMapping();
    }

    private Map<Edge, Edge> constructQueryToViewJoinMapWithExpr() {
        Map<Expression, Edge> viewExprToEdge = getViewJoinEdges().stream()
                .flatMap(e -> e.getExpressions().stream().map(expr -> Pair.of(expr, e)))
                .collect(ImmutableMap.toImmutableMap(p -> p.first, p -> p.second));
        Map<Expression, Edge> queryExprToEdge = getQueryJoinEdges().stream()
                .flatMap(e -> e.getExpressions().stream().map(expr -> Pair.of(expr, e)))
                .collect(ImmutableMap.toImmutableMap(p -> p.first, p -> p.second));

        HashMap<Edge, Edge> edgeMap = new HashMap<>();
        for (Entry<Expression, Edge> entry : queryExprToEdge.entrySet()) {
            if (edgeMap.containsKey(entry.getValue())) {
                continue;
            }
            Expression viewExpr = getMappingViewExprByQueryExpr(entry.getKey(),
                    entry.getValue(),
                    logicalCompatibilityContext,
                    ExpressionPosition.JOIN_EDGE).orElse(null);
            if (viewExprToEdge.containsKey(viewExpr)) {
                edgeMap.put(entry.getValue(), Objects.requireNonNull(viewExprToEdge.get(viewExpr)));
            }
        }
        return edgeMap;
    }

    // Such as the filter as following, their expression is same, but should be different filter edge
    // Only construct edge that can mapping, the edges which can not mapping would be handled by buildComparisonRes
    //     LogicalJoin[569]
    //       |--LogicalProject[567]
    //       |  +--LogicalFilter[566] ( predicates=(l_orderkey#10 IS NULL OR ( not (l_orderkey#10 = 1))) )
    //       |     +--LogicalJoin[565]
    //       |        |--LogicalProject[562]
    //       |        |  +--LogicalOlapScan
    //       |        +--LogicalProject[564]
    //       |           +--LogicalFilter[563] ( predicates=(l_orderkey#10 IS NULL OR ( not (l_orderkey#10 = 1))))
    //       |              +--LogicalOlapScan
    //       +--LogicalProject[568]
    //         +--LogicalOlapScan
    private Map<Edge, Edge> constructQueryToViewFilterMapWithExpr() {
        Multimap<Expression, Edge> viewExprToEdge = HashMultimap.create();
        getViewFilterEdges().stream()
                .flatMap(e -> e.getExpressions().stream().map(expr -> Pair.of(expr, e)))
                .forEach(pair -> viewExprToEdge.put(pair.key(), pair.value()));

        Multimap<Expression, Edge> queryExprToEdge = HashMultimap.create();
        getQueryFilterEdges().stream()
                .flatMap(e -> e.getExpressions().stream().map(expr -> Pair.of(expr, e)))
                .forEach(pair -> queryExprToEdge.put(pair.key(), pair.value()));

        HashMap<Edge, Edge> queryToViewEdgeMap = new HashMap<>();
        for (Entry<Expression, Collection<Edge>> entry : queryExprToEdge.asMap().entrySet()) {
            Expression queryExprViewBased = null;
            for (Edge queryEdge : entry.getValue()) {
                queryExprViewBased = getMappingViewExprByQueryExpr(entry.getKey(),
                        queryEdge,
                        logicalCompatibilityContext,
                        ExpressionPosition.FILTER_EDGE).orElse(null);
                if (queryExprViewBased == null) {
                    continue;
                }
                Collection<Edge> viewEdges = viewExprToEdge.get(queryExprViewBased);
                if (viewEdges.isEmpty()) {
                    continue;
                }
                for (Edge viewEdge : viewEdges) {
                    if (!isSubTreeNodesEquals(queryEdge, viewEdge, logicalCompatibilityContext)) {
                        // Such as query filter edge is <{1} --FILTER-- {}> but view filter edge is
                        // <{0, 1} --FILTER-- {}>, though they are all
                        // l_orderkey#10 IS NULL OR ( not (l_orderkey#10 = 1)) but they are different actually
                        continue;
                    }
                    queryToViewEdgeMap.put(queryEdge, viewEdge);
                }
            }
        }
        return queryToViewEdgeMap;
    }

    private static boolean isSubTreeNodesEquals(Edge queryEdge, Edge viewEdge,
            LogicalCompatibilityContext logicalCompatibilityContext) {
        if (!(queryEdge instanceof FilterEdge) || !(viewEdge instanceof FilterEdge)) {
            return false;
        }
        // subTreeNodes should be equal
        BiMap<Integer, Integer> queryToViewNodeIdMapping =
                logicalCompatibilityContext.getQueryToViewNodeIDMapping();
        List<Integer> queryNodeIndexViewBasedList = new ArrayList<>();
        for (int queryNodeIndex : LongBitmap.getIterator(queryEdge.getSubTreeNodes())) {
            Integer queryNodeIndexViewBased = queryToViewNodeIdMapping.get(queryNodeIndex);
            if (queryNodeIndexViewBased == null) {
                return false;
            }
            queryNodeIndexViewBasedList.add(queryNodeIndexViewBased);
        }
        return LongBitmap.newBitmap(queryNodeIndexViewBasedList) == viewEdge.getSubTreeNodes();
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
        return getViewNodesByQuery(query.getReferenceNodes()) == view.getReferenceNodes();
    }

    private boolean compareJoinEdgeWithNode(JoinEdge query, JoinEdge view) {
        boolean res = false;
        if (query.getJoinType().swap() == view.getJoinType()) {
            res |= getViewNodesByQuery(query.getLeftExtendedNodes()) == view.getRightExtendedNodes()
                    && getViewNodesByQuery(query.getRightExtendedNodes()) == view.getLeftExtendedNodes();
        }
        res |= getViewNodesByQuery(query.getLeftExtendedNodes()) == view.getLeftExtendedNodes()
                && getViewNodesByQuery(query.getRightExtendedNodes()) == view.getRightExtendedNodes();
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
        if (getViewNodesByQuery(query.getLeftRequiredNodes()) != view.getLeftRequiredNodes()
                || getViewNodesByQuery(query.getRightRequiredNodes()) != view.getRightRequiredNodes()) {
            return false;
        }
        if (!query.getJoinType().equals(view.getJoinType())) {
            Pair<Boolean, Boolean> noNullableChild = canInferredJoinTypeMap.getOrDefault(
                    Pair.of(query.getJoinType(), view.getJoinType()), null);
            if (noNullableChild == null) {
                return false;
            }
            Pair<Set<Slot>, Set<Slot>> noNullableSlotSetPair = Pair.of(ImmutableSet.of(), ImmutableSet.of());
            if (noNullableChild.first) {
                noNullableSlotSetPair.first = view.getJoin().left().getOutputSet();
            }
            if (noNullableChild.second) {
                noNullableSlotSetPair.second = view.getJoin().right().getOutputSet();
            }
            inferredViewEdgeWithCond.put(view, Pair.of(query.getJoinType(), noNullableSlotSetPair));
        }
        return true;
    }

    private long getViewNodesByQuery(long bitmap) {
        long newBitmap = LongBitmap.newBitmap();
        for (int i : LongBitmap.getIterator(bitmap)) {
            int newIdx = getQueryToViewNodeIdMap().getOrDefault(i, 0);
            newBitmap = LongBitmap.set(newBitmap, newIdx);
        }
        return newBitmap;
    }

    private Optional<Expression> getMappingViewExprByQueryExpr(Expression queryExpression,
            HyperElement queryExpressionBelongedHyperElement,
            LogicalCompatibilityContext context,
            ExpressionPosition expressionPosition) {
        Expression queryShuttledExpr;
        Collection<Pair<Expression, HyperElement>> viewExpressions;
        if (ExpressionPosition.JOIN_EDGE.equals(expressionPosition)) {
            queryShuttledExpr = context.getQueryJoinShuttledExpr(queryExpression);
            viewExpressions = context.getViewJoinExprFromQuery(queryShuttledExpr);
        } else if (ExpressionPosition.FILTER_EDGE.equals(expressionPosition)) {
            queryShuttledExpr = context.getQueryFilterShuttledExpr(queryExpression);
            viewExpressions = context.getViewFilterExprFromQuery(queryShuttledExpr);
        } else {
            queryShuttledExpr = context.getQueryNodeShuttledExpr(queryExpression);
            viewExpressions = context.getViewNodeExprFromQuery(queryShuttledExpr);
        }
        if (viewExpressions.size() == 1) {
            return Optional.of(viewExpressions.iterator().next().key());
        }
        long queryReferenceNodes = queryExpressionBelongedHyperElement.getReferenceNodes();
        long viewReferenceNodes = getViewNodesByQuery(queryReferenceNodes);
        for (Pair<Expression, HyperElement> viewExpressionPair : viewExpressions) {
            if (viewExpressionPair.value().getReferenceNodes() == viewReferenceNodes) {
                return Optional.of(viewExpressionPair.key());
            }
        }
        return Optional.empty();
    }

    private void compareJoinEdgeWithExpr(Edge query, Edge view) {
        Set<? extends Expression> queryExprSet = query.getExpressionSet();
        Set<? extends Expression> viewExprSet = view.getExpressionSet();

        Set<Expression> exprMappedOfView = new HashSet<>();
        List<Expression> residualQueryExpr = new ArrayList<>();
        for (Expression queryExpr : queryExprSet) {
            Expression viewExpr = getMappingViewExprByQueryExpr(queryExpr,
                    query,
                    logicalCompatibilityContext,
                    ExpressionPosition.JOIN_EDGE).orElse(null);
            if (viewExprSet.contains(viewExpr)) {
                exprMappedOfView.add(viewExpr);
            } else {
                residualQueryExpr.add(queryExpr);
            }
        }
        List<Expression> residualViewExpr = ImmutableList.copyOf(Sets.difference(viewExprSet, exprMappedOfView));
        pullUpQueryExprWithEdge.put(query, residualQueryExpr);
        pullUpViewExprWithEdge.put(query, residualViewExpr);
    }

    private void compareFilterEdgeWithExpr(Edge query, Edge view) {
        Set<? extends Expression> queryExprSet = query.getExpressionSet();
        Set<? extends Expression> viewExprSet = view.getExpressionSet();

        Set<Expression> exprMappedOfView = new HashSet<>();
        List<Expression> residualQueryExpr = new ArrayList<>();
        for (Expression queryExpr : queryExprSet) {
            Expression viewExpr = getMappingViewExprByQueryExpr(queryExpr,
                    query,
                    logicalCompatibilityContext,
                    ExpressionPosition.FILTER_EDGE).orElse(null);
            if (viewExprSet.contains(viewExpr)) {
                exprMappedOfView.add(viewExpr);
            } else {
                residualQueryExpr.add(queryExpr);
            }
        }
        List<Expression> residualViewExpr = ImmutableList.copyOf(Sets.difference(viewExprSet, exprMappedOfView));
        pullUpQueryExprWithEdge.put(query, residualQueryExpr);
        pullUpViewExprWithEdge.put(query, residualViewExpr);
    }
}
