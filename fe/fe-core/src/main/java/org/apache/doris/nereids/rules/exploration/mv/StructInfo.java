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

import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperElement;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils.TableQueryOperatorChecker;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand.PredicateAddContext;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand.PredicateAdder;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * StructInfo for plan, this contains necessary info for query rewrite by materialized view
 * the struct info is used by all materialization, so it's struct info should only get, should not
 * modify, if wanting to modify, should copy and then modify
 */
public class StructInfo {
    public static final PlanPatternChecker PLAN_PATTERN_CHECKER = new PlanPatternChecker();
    public static final ScanPlanPatternChecker SCAN_PLAN_PATTERN_CHECKER = new ScanPlanPatternChecker();
    // struct info splitter
    public static final PlanSplitter PLAN_SPLITTER = new PlanSplitter();
    private static final RelationCollector RELATION_COLLECTOR = new RelationCollector();
    private static final PredicateCollector PREDICATE_COLLECTOR = new PredicateCollector();
    // source data
    private final Plan originalPlan;
    private final ObjectId originalPlanId;
    private final HyperGraph hyperGraph;
    private final boolean valid;
    // derived data following
    // top plan which may include project or filter, except for join and scan
    private final Plan topPlan;
    // bottom plan which top plan only contain join or scan. this is needed by hyper graph
    private final Plan bottomPlan;
    private final List<CatalogRelation> relations;
    // This is generated by cascadesContext, this may be different in different cascadesContext
    // So if the cascadesContext currently is different form the cascadesContext which generated it.
    // Should regenerate the tableBitSet by current cascadesContext and call withTableBitSet method
    private final BitSet tableBitSet;
    // this is for LogicalCompatibilityContext later
    private final Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap;
    // this recorde the predicates which can pull up, not shuttled
    private final Predicates predicates;
    // split predicates is shuttled
    private SplitPredicate splitPredicate;
    private EquivalenceClass equivalenceClass;
    // For value of Map, the key is the position of expression
    // the value is the expressions and the hyper element of expression pair
    // Key of pair is the expression shuttled and the value is the origin expression and the hyper element it belonged
    // Sometimes origin expressions are different and shuttled expression is same
    // Such as origin expressions are l_partkey#0 > 1 and l_partkey#10 > 1 and shuttled expression is l_partkey#10 > 1
    // this is for building LogicalCompatibilityContext later.
    private final Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
            shuttledExpressionsToExpressionsMap;
    // For value of Map, the key is the position of expression
    // the value is the original expression and shuttled expression map
    // Such as origin expressions are l_partkey#0 > 1 and shuttled expression is l_partkey#10 > 1
    // the map would be {ExpressionPosition.FILTER, {
    //     l_partkey#0 > 1 : l_partkey#10 > 1
    // }}
    // this is for building LogicalCompatibilityContext later.
    private final Map<ExpressionPosition, Map<Expression, Expression>> expressionToShuttledExpressionToMap;

    // Record the exprId and the corresponding expr map, this is used by expression shuttled
    private final Map<ExprId, Expression> namedExprIdAndExprMapping;
    private final List<? extends Expression> planOutputShuttledExpressions;

    /**
     * The construct method for StructInfo
     */
    private StructInfo(Plan originalPlan, ObjectId originalPlanId, HyperGraph hyperGraph, boolean valid, Plan topPlan,
            Plan bottomPlan, List<CatalogRelation> relations,
            Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap,
            @Nullable Predicates predicates,
            Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
                    shuttledExpressionsToExpressionsMap,
            Map<ExpressionPosition, Map<Expression, Expression>> expressionToShuttledExpressionToMap,
            Map<ExprId, Expression> namedExprIdAndExprMapping,
            BitSet tableIdSet,
            SplitPredicate splitPredicate,
            EquivalenceClass equivalenceClass,
            List<? extends Expression> planOutputShuttledExpressions) {
        this.originalPlan = originalPlan;
        this.originalPlanId = originalPlanId;
        this.hyperGraph = hyperGraph;
        this.valid = valid;
        this.topPlan = topPlan;
        this.bottomPlan = bottomPlan;
        this.relations = relations;
        this.tableBitSet = tableIdSet;
        this.relationIdStructInfoNodeMap = relationIdStructInfoNodeMap;
        this.predicates = predicates;
        this.splitPredicate = splitPredicate;
        this.equivalenceClass = equivalenceClass;
        this.shuttledExpressionsToExpressionsMap = shuttledExpressionsToExpressionsMap;
        this.expressionToShuttledExpressionToMap = expressionToShuttledExpressionToMap;
        this.namedExprIdAndExprMapping = namedExprIdAndExprMapping;
        this.planOutputShuttledExpressions = planOutputShuttledExpressions;
    }

    /**
     * Construct StructInfo with new predicates
     */
    public StructInfo withPredicates(Predicates predicates) {
        return new StructInfo(this.originalPlan, this.originalPlanId, this.hyperGraph, this.valid, this.topPlan,
                this.bottomPlan, this.relations, this.relationIdStructInfoNodeMap, predicates,
                this.shuttledExpressionsToExpressionsMap, this.expressionToShuttledExpressionToMap,
                this.namedExprIdAndExprMapping, this.tableBitSet,
                null, null, this.planOutputShuttledExpressions);
    }

    /**
     * Construct StructInfo with new tableBitSet
     */
    public StructInfo withTableBitSet(BitSet tableBitSet) {
        return new StructInfo(this.originalPlan, this.originalPlanId, this.hyperGraph, this.valid, this.topPlan,
                this.bottomPlan, this.relations, this.relationIdStructInfoNodeMap, this.predicates,
                this.shuttledExpressionsToExpressionsMap, this.expressionToShuttledExpressionToMap,
                this.namedExprIdAndExprMapping, tableBitSet,
                this.splitPredicate, this.equivalenceClass, this.planOutputShuttledExpressions);
    }

    private static boolean collectStructInfoFromGraph(HyperGraph hyperGraph,
            Plan topPlan,
            Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
                    shuttledExpressionsToExpressionsMap,
            Map<ExpressionPosition, Map<Expression, Expression>> expressionToShuttledExpressionToMap,
            Map<ExprId, Expression> namedExprIdAndExprMapping,
            List<CatalogRelation> relations,
            Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap,
            BitSet hyperTableBitSet,
            CascadesContext cascadesContext) {

        // Collect relations from hyper graph which in the bottom plan firstly
        hyperGraph.getNodes().forEach(node -> {
            // plan relation collector and set to map
            Plan nodePlan = node.getPlan();
            List<CatalogRelation> nodeRelations = new ArrayList<>();
            nodePlan.accept(RELATION_COLLECTOR, nodeRelations);
            relations.addAll(nodeRelations);
            nodeRelations.forEach(relation -> hyperTableBitSet.set(
                    cascadesContext.getStatementContext().getTableId(relation.getTable()).asInt()));
            // plan relation collector and set to map
            StructInfoNode structInfoNode = (StructInfoNode) node;
            // record expressions in node
            if (structInfoNode.getExpressions() != null) {
                structInfoNode.getExpressions().forEach(expression -> {
                    ExpressionLineageReplacer.ExpressionReplaceContext replaceContext =
                            new ExpressionLineageReplacer.ExpressionReplaceContext(
                                    Lists.newArrayList(expression), ImmutableSet.of(),
                                    ImmutableSet.of(), new BitSet());
                    structInfoNode.getPlan().accept(ExpressionLineageReplacer.INSTANCE, replaceContext);
                    // Replace expressions by expression map
                    List<Expression> replacedExpressions = replaceContext.getReplacedExpressions();
                    putShuttledExpressionToExpressionsMap(shuttledExpressionsToExpressionsMap,
                            expressionToShuttledExpressionToMap,
                            ExpressionPosition.NODE, replacedExpressions.get(0), expression, node);
                    // Record this, will be used in top level expression shuttle later, see the method
                    // ExpressionLineageReplacer#visitGroupPlan
                    namedExprIdAndExprMapping.putAll(replaceContext.getExprIdExpressionMap());
                });
            }
            // every node should only have one relation, this is for LogicalCompatibilityContext
            if (!nodeRelations.isEmpty()) {
                relationIdStructInfoNodeMap.put(nodeRelations.get(0).getRelationId(), structInfoNode);
            }
        });
        // Collect expression from join condition in hyper graph
        for (JoinEdge edge : hyperGraph.getJoinEdges()) {
            List<? extends Expression> joinConjunctExpressions = edge.getExpressions();
            // shuttle expression in edge for the build of LogicalCompatibilityContext later.
            // Record the exprId to expr map in the processing to strut info
            // TODO get exprId to expr map when complex project is ready in join dege
            ExpressionLineageReplacer.ExpressionReplaceContext replaceContext =
                    new ExpressionLineageReplacer.ExpressionReplaceContext(
                            joinConjunctExpressions.stream().map(expr -> (Expression) expr)
                                    .collect(Collectors.toList()),
                            ImmutableSet.of(), ImmutableSet.of(), new BitSet());
            topPlan.accept(ExpressionLineageReplacer.INSTANCE, replaceContext);
            // Replace expressions by expression map
            List<Expression> replacedExpressions = replaceContext.getReplacedExpressions();
            for (int i = 0; i < replacedExpressions.size(); i++) {
                putShuttledExpressionToExpressionsMap(shuttledExpressionsToExpressionsMap,
                        expressionToShuttledExpressionToMap,
                        ExpressionPosition.JOIN_EDGE, replacedExpressions.get(i), joinConjunctExpressions.get(i),
                        edge);
            }
            // Record this, will be used in top level expression shuttle later, see the method
            // ExpressionLineageReplacer#visitGroupPlan
            namedExprIdAndExprMapping.putAll(replaceContext.getExprIdExpressionMap());
        }
        // Collect expression from where in hyper graph
        hyperGraph.getFilterEdges().forEach(filterEdge -> {
            List<? extends Expression> filterExpressions = filterEdge.getExpressions();
            filterExpressions.forEach(predicate -> {
                // this is used for LogicalCompatibilityContext
                ExpressionUtils.extractConjunction(predicate).forEach(expr ->
                        putShuttledExpressionToExpressionsMap(shuttledExpressionsToExpressionsMap,
                                expressionToShuttledExpressionToMap,
                                ExpressionPosition.FILTER_EDGE,
                                ExpressionUtils.shuttleExpressionWithLineage(predicate, topPlan, new BitSet()),
                                predicate, filterEdge));
            });
        });
        return true;
    }

    // derive some useful predicate by predicates
    private static Pair<SplitPredicate, EquivalenceClass> predicatesDerive(Predicates predicates, Plan originalPlan) {
        // construct equivalenceClass according to equals predicates
        List<Expression> shuttledExpression = ExpressionUtils.shuttleExpressionWithLineage(
                        new ArrayList<>(predicates.getPulledUpPredicates()), originalPlan, new BitSet()).stream()
                .map(Expression.class::cast)
                .collect(Collectors.toList());
        SplitPredicate splitPredicate = Predicates.splitPredicates(ExpressionUtils.and(shuttledExpression));
        EquivalenceClass equivalenceClass = new EquivalenceClass();
        for (Expression expression : ExpressionUtils.extractConjunction(splitPredicate.getEqualPredicate())) {
            if (expression instanceof Literal) {
                continue;
            }
            if (expression instanceof EqualTo) {
                EqualTo equalTo = (EqualTo) expression;
                equivalenceClass.addEquivalenceClass(
                        (SlotReference) equalTo.getArguments().get(0),
                        (SlotReference) equalTo.getArguments().get(1));
            }
        }
        return Pair.of(splitPredicate, equivalenceClass);
    }

    /**
     * Build Struct info from plan.
     * Maybe return multi structInfo when original plan already be rewritten by mv
     */
    public static StructInfo of(Plan originalPlan, CascadesContext cascadesContext) {
        return of(originalPlan, originalPlan, cascadesContext);
    }

    /**
     * Build Struct info from plan.
     * Maybe return multi structInfo when original plan already be rewritten by mv
     */
    public static StructInfo of(Plan derivedPlan, Plan originalPlan, CascadesContext cascadesContext) {
        // Split plan by the boundary which contains multi child
        LinkedHashSet<Class<? extends Plan>> set = Sets.newLinkedHashSet();
        set.add(LogicalJoin.class);
        PlanSplitContext planSplitContext = new PlanSplitContext(set);
        // if single table without join, the bottom is
        derivedPlan.accept(PLAN_SPLITTER, planSplitContext);
        return StructInfo.of(originalPlan, planSplitContext.getTopPlan(), planSplitContext.getBottomPlan(),
                HyperGraph.builderForMv(planSplitContext.getBottomPlan()).build(), cascadesContext);
    }

    /**
     * The construct method for init StructInfo
     */
    public static StructInfo of(Plan originalPlan, @Nullable Plan topPlan, @Nullable Plan bottomPlan,
            HyperGraph hyperGraph,
            CascadesContext cascadesContext) {
        ObjectId originalPlanId = originalPlan.getGroupExpression()
                .map(GroupExpression::getId).orElseGet(() -> new ObjectId(-1));
        // if any of topPlan or bottomPlan is null, split the top plan to two parts by join node
        if (topPlan == null || bottomPlan == null) {
            Set<Class<? extends Plan>> set = Sets.newLinkedHashSet();
            set.add(LogicalJoin.class);
            PlanSplitContext planSplitContext = new PlanSplitContext(set);
            originalPlan.accept(PLAN_SPLITTER, planSplitContext);
            bottomPlan = planSplitContext.getBottomPlan();
            topPlan = planSplitContext.getTopPlan();
        }
        // collect struct info fromGraph
        List<CatalogRelation> relationList = new ArrayList<>();
        Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap = new LinkedHashMap<>();
        Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
                shuttledHashConjunctsToConjunctsMap = new LinkedHashMap<>();
        Map<ExprId, Expression> namedExprIdAndExprMapping = new LinkedHashMap<>();
        BitSet tableBitSet = new BitSet();
        Map<ExpressionPosition, Map<Expression, Expression>> expressionToShuttledExpressionToMap = new HashMap<>();
        boolean valid = collectStructInfoFromGraph(hyperGraph, topPlan, shuttledHashConjunctsToConjunctsMap,
                expressionToShuttledExpressionToMap,
                namedExprIdAndExprMapping,
                relationList,
                relationIdStructInfoNodeMap,
                tableBitSet,
                cascadesContext);
        valid = valid
                && hyperGraph.getNodes().stream().allMatch(n -> ((StructInfoNode) n).getExpressions() != null);
        // if relationList has any relation which contains table operator,
        // such as query with sample, index, table, is invalid
        boolean invalid = relationList.stream().anyMatch(relation ->
                ((AbstractPlan) relation).accept(TableQueryOperatorChecker.INSTANCE, null));
        valid = valid && !invalid;
        // collect predicate from top plan which not in hyper graph
        Set<Expression> topPlanPredicates = new LinkedHashSet<>();
        topPlan.accept(PREDICATE_COLLECTOR, topPlanPredicates);
        Predicates predicates = Predicates.of(topPlanPredicates);
        // this should use the output of originalPlan to make sure the output right order
        List<? extends Expression> planOutputShuttledExpressions =
                ExpressionUtils.shuttleExpressionWithLineage(originalPlan.getOutput(), originalPlan, new BitSet());
        return new StructInfo(originalPlan, originalPlanId, hyperGraph, valid, topPlan, bottomPlan,
                relationList, relationIdStructInfoNodeMap, predicates, shuttledHashConjunctsToConjunctsMap,
                expressionToShuttledExpressionToMap,
                namedExprIdAndExprMapping, tableBitSet, null, null,
                planOutputShuttledExpressions);
    }

    public List<CatalogRelation> getRelations() {
        return relations;
    }

    public Predicates getPredicates() {
        return predicates;
    }

    public Plan getOriginalPlan() {
        return originalPlan;
    }

    public HyperGraph getHyperGraph() {
        return hyperGraph;
    }

    /**
     * lazy init for performance
     */
    public SplitPredicate getSplitPredicate() {
        if (this.splitPredicate == null && this.predicates != null) {
            Pair<SplitPredicate, EquivalenceClass> derivedPredicates = predicatesDerive(this.predicates, topPlan);
            this.splitPredicate = derivedPredicates.key();
            this.equivalenceClass = derivedPredicates.value();
        }
        return this.splitPredicate;
    }

    /**
     * lazy init for performance
     */
    public EquivalenceClass getEquivalenceClass() {
        if (this.equivalenceClass == null && this.predicates != null) {
            Pair<SplitPredicate, EquivalenceClass> derivedPredicates = predicatesDerive(this.predicates, topPlan);
            this.splitPredicate = derivedPredicates.key();
            this.equivalenceClass = derivedPredicates.value();
        }
        return this.equivalenceClass;
    }

    public boolean isValid() {
        return valid;
    }

    public Plan getTopPlan() {
        return topPlan;
    }

    public Plan getBottomPlan() {
        return bottomPlan;
    }

    public Map<RelationId, StructInfoNode> getRelationIdStructInfoNodeMap() {
        return relationIdStructInfoNodeMap;
    }

    public Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
            getShuttledExpressionsToExpressionsMap() {
        return shuttledExpressionsToExpressionsMap;
    }

    public Map<ExpressionPosition, Map<Expression, Expression>> getExpressionToShuttledExpressionToMap() {
        return expressionToShuttledExpressionToMap;
    }

    private static void putShuttledExpressionToExpressionsMap(
            Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
                    shuttledExpressionsToExpressionsMap,
            Map<ExpressionPosition, Map<Expression, Expression>> expressionPositionToExpressionToMap,
            ExpressionPosition expressionPosition,
            Expression shuttledExpression, Expression originalExpression, HyperElement valueBelongedElement) {
        Multimap<Expression, Pair<Expression, HyperElement>> shuttledExpressionToExpressionMap =
                shuttledExpressionsToExpressionsMap.get(expressionPosition);
        if (shuttledExpressionToExpressionMap == null) {
            shuttledExpressionToExpressionMap = HashMultimap.create();
            shuttledExpressionsToExpressionsMap.put(expressionPosition, shuttledExpressionToExpressionMap);
        }
        shuttledExpressionToExpressionMap.put(shuttledExpression, Pair.of(originalExpression, valueBelongedElement));

        Map<Expression, Expression> originalExprToShuttledExprMap =
                expressionPositionToExpressionToMap.get(expressionPosition);
        if (originalExprToShuttledExprMap == null) {
            originalExprToShuttledExprMap = new HashMap<>();
            expressionPositionToExpressionToMap.put(expressionPosition, originalExprToShuttledExprMap);
        }
        originalExprToShuttledExprMap.put(originalExpression, shuttledExpression);
    }

    public List<? extends Expression> getExpressions() {
        return topPlan instanceof LogicalProject
                ? ((LogicalProject<Plan>) topPlan).getProjects() : topPlan.getOutput();
    }

    public ObjectId getOriginalPlanId() {
        return originalPlanId;
    }

    public Map<ExprId, Expression> getNamedExprIdAndExprMapping() {
        return namedExprIdAndExprMapping;
    }

    public BitSet getTableBitSet() {
        return tableBitSet;
    }

    public List<? extends Expression> getPlanOutputShuttledExpressions() {
        return planOutputShuttledExpressions;
    }

    /**
     * Judge the source graph logical is whether the same as target
     * For inner join should judge only the join tables,
     * for other join type should also judge the join direction, it's input filter that can not be pulled up etc.
     */
    public static ComparisonResult isGraphLogicalEquals(StructInfo queryStructInfo, StructInfo viewStructInfo,
            LogicalCompatibilityContext compatibilityContext) {
        return HyperGraphComparator
                .isLogicCompatible(queryStructInfo.hyperGraph, viewStructInfo.hyperGraph, compatibilityContext);
    }

    @Override
    public String toString() {
        return "StructInfo{ originalPlanId = " + originalPlanId + ", relations = " + relations + '}';
    }

    private static class RelationCollector extends DefaultPlanVisitor<Void, List<CatalogRelation>> {
        @Override
        public Void visit(Plan plan, List<CatalogRelation> collectedRelations) {
            if (plan instanceof CatalogRelation) {
                collectedRelations.add((CatalogRelation) plan);
            }
            return super.visit(plan, collectedRelations);
        }
    }

    private static class PredicateCollector extends DefaultPlanVisitor<Void, Set<Expression>> {
        @Override
        public Void visit(Plan plan, Set<Expression> predicates) {
            // Just collect the filter in top plan, if meet other node except project and filter, return
            if (!(plan instanceof LogicalProject)
                    && !(plan instanceof LogicalFilter)
                    && !(plan instanceof LogicalAggregate)) {
                return null;
            }
            if (plan instanceof LogicalFilter) {
                predicates.addAll(ExpressionUtils.extractConjunction(((LogicalFilter) plan).getPredicate()));
            }
            return super.visit(plan, predicates);
        }
    }

    /**
     * Split the plan into bottom and up, the boundary is given by context,
     * the bottom contains the boundary, and top plan doesn't contain the boundary.
     */
    public static class PlanSplitter extends DefaultPlanVisitor<Void, PlanSplitContext> {
        @Override
        public Void visit(Plan plan, PlanSplitContext context) {
            if (context.getTopPlan() == null) {
                context.setTopPlan(plan);
            }
            if (plan.children().isEmpty() && context.getBottomPlan() == null) {
                context.setBottomPlan(plan);
                return null;
            }
            if (context.isBoundary(plan)) {
                context.setBottomPlan(plan);
                return null;
            }
            return super.visit(plan, context);
        }
    }

    /**
     * Judge if source contains all target
     */
    public static boolean containsAll(BitSet source, BitSet target) {
        if (source.size() < target.size()) {
            return false;
        }
        for (int i = target.nextSetBit(0); i >= 0; i = target.nextSetBit(i + 1)) {
            boolean contains = source.get(i);
            if (!contains) {
                return false;
            }
        }
        return true;
    }

    /**
     * Plan split context, this hold bottom and top plan, and boundary plan setting
     */
    public static class PlanSplitContext {
        private Plan bottomPlan;
        private Plan topPlan;
        private Set<Class<? extends Plan>> boundaryPlanClazzSet;

        public PlanSplitContext(Set<Class<? extends Plan>> boundaryPlanClazzSet) {
            this.boundaryPlanClazzSet = boundaryPlanClazzSet;
        }

        public Plan getBottomPlan() {
            return bottomPlan;
        }

        public void setBottomPlan(Plan bottomPlan) {
            this.bottomPlan = bottomPlan;
        }

        public Plan getTopPlan() {
            return topPlan;
        }

        public void setTopPlan(Plan topPlan) {
            this.topPlan = topPlan;
        }

        /**
         * isBoundary
         */
        public boolean isBoundary(Plan plan) {
            for (Class<? extends Plan> boundaryPlanClazz : boundaryPlanClazzSet) {
                if (boundaryPlanClazz.isAssignableFrom(plan.getClass())) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * The context for plan check context, make sure that the plan in query and mv is valid or not
     */
    public static class PlanCheckContext {
        // the aggregate above join
        private boolean containsTopAggregate = false;
        private int topAggregateNum = 0;
        private boolean alreadyMeetJoin = false;
        private final Set<JoinType> supportJoinTypes;

        public PlanCheckContext(Set<JoinType> supportJoinTypes) {
            this.supportJoinTypes = supportJoinTypes;
        }

        public boolean isContainsTopAggregate() {
            return containsTopAggregate;
        }

        public void setContainsTopAggregate(boolean containsTopAggregate) {
            this.containsTopAggregate = containsTopAggregate;
        }

        public boolean isAlreadyMeetJoin() {
            return alreadyMeetJoin;
        }

        public void setAlreadyMeetJoin(boolean alreadyMeetJoin) {
            this.alreadyMeetJoin = alreadyMeetJoin;
        }

        public Set<JoinType> getSupportJoinTypes() {
            return supportJoinTypes;
        }

        public int getTopAggregateNum() {
            return topAggregateNum;
        }

        public void plusTopAggregateNum() {
            this.topAggregateNum += 1;
        }

        public static PlanCheckContext of(Set<JoinType> supportJoinTypes) {
            return new PlanCheckContext(supportJoinTypes);
        }
    }

    /**
     * PlanPatternChecker, this is used to check the plan pattern is valid or not
     */
    public static class PlanPatternChecker extends DefaultPlanVisitor<Boolean, PlanCheckContext> {
        @Override
        public Boolean visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                PlanCheckContext checkContext) {
            checkContext.setAlreadyMeetJoin(true);
            if (!checkContext.getSupportJoinTypes().contains(join.getJoinType())) {
                return false;
            }
            if (!join.getOtherJoinConjuncts().isEmpty()) {
                return false;
            }
            return visit(join, checkContext);
        }

        @Override
        public Boolean visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                PlanCheckContext checkContext) {
            if (!checkContext.isAlreadyMeetJoin()) {
                checkContext.setContainsTopAggregate(true);
                checkContext.plusTopAggregateNum();
            }
            return visit(aggregate, checkContext);
        }

        @Override
        public Boolean visitGroupPlan(GroupPlan groupPlan, PlanCheckContext checkContext) {
            return groupPlan.getGroup().getLogicalExpressions().stream()
                    .anyMatch(logicalExpression -> logicalExpression.getPlan().accept(this, checkContext));
        }

        @Override
        public Boolean visit(Plan plan, PlanCheckContext checkContext) {
            if (plan instanceof Filter
                    || plan instanceof Project
                    || plan instanceof CatalogRelation
                    || plan instanceof Join
                    || plan instanceof LogicalSort
                    || plan instanceof LogicalAggregate
                    || plan instanceof GroupPlan
                    || plan instanceof LogicalRepeat) {
                return doVisit(plan, checkContext);
            }
            return false;
        }

        private Boolean doVisit(Plan plan, PlanCheckContext checkContext) {
            for (Plan child : plan.children()) {
                boolean valid = child.accept(this, checkContext);
                if (!valid) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * ScanPlanPatternChecker, this is used to check the plan pattern is valid or not
     */
    public static class ScanPlanPatternChecker extends DefaultPlanVisitor<Boolean, PlanCheckContext> {

        @Override
        public Boolean visitGroupPlan(GroupPlan groupPlan, PlanCheckContext checkContext) {
            return groupPlan.getGroup().getLogicalExpressions().stream()
                    .anyMatch(logicalExpression -> logicalExpression.getPlan().accept(this, checkContext));
        }

        @Override
        public Boolean visit(Plan plan, PlanCheckContext checkContext) {
            if (plan instanceof Filter
                    || plan instanceof Project
                    || plan instanceof CatalogRelation
                    || plan instanceof GroupPlan
                    || plan instanceof LogicalRepeat) {
                return doVisit(plan, checkContext);
            }
            return false;
        }

        private Boolean doVisit(Plan plan, PlanCheckContext checkContext) {
            for (Plan child : plan.children()) {
                boolean valid = child.accept(this, checkContext);
                if (!valid) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Add or remove partition on base table and mv when materialized view scan contains invalid partitions
     */
    public static class PartitionRemover extends DefaultPlanRewriter<Map<BaseTableInfo, Set<String>>> {
        @Override
        public Plan visitLogicalOlapScan(LogicalOlapScan olapScan,
                Map<BaseTableInfo, Set<String>> context) {
            // todo Support other partition table
            BaseTableInfo tableInfo = new BaseTableInfo(olapScan.getTable());
            if (!context.containsKey(tableInfo)) {
                return olapScan;
            }
            Set<String> targetPartitionNameSet = context.get(tableInfo);
            List<Long> selectedPartitionIds = new ArrayList<>(olapScan.getSelectedPartitionIds());
            // need remove partition
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(partitionId -> !targetPartitionNameSet.contains(
                            olapScan.getTable().getPartition(partitionId).getName()))
                    .collect(Collectors.toList());
            return olapScan.withSelectedPartitionIds(selectedPartitionIds);
        }
    }

    /**
     * Collect partitions on base table
     */
    public static class QueryScanPartitionsCollector extends DefaultPlanVisitor<Plan,
            Map<BaseTableInfo, Set<Partition>>> {
        @Override
        public Plan visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation,
                Map<BaseTableInfo, Set<Partition>> targetTablePartitionMap) {
            TableIf table = catalogRelation.getTable();
            BaseTableInfo relatedPartitionTable = new BaseTableInfo(table);
            if (!targetTablePartitionMap.containsKey(relatedPartitionTable)) {
                return catalogRelation;
            }
            if (catalogRelation instanceof LogicalOlapScan) {
                // Handle olap table
                LogicalOlapScan logicalOlapScan = (LogicalOlapScan) catalogRelation;
                Set<Partition> tablePartitions = targetTablePartitionMap.get(relatedPartitionTable);
                for (Long partitionId : logicalOlapScan.getSelectedPartitionIds()) {
                    tablePartitions.add(logicalOlapScan.getTable().getPartition(partitionId));
                }
            } else {
                // todo Support other type partition table
                // Not support to partition check now when query external catalog table, support later.
                targetTablePartitionMap.clear();
            }
            return catalogRelation;
        }
    }

    /**
     * Add filter on table scan according to table filter map
     *
     * @return Pair(Plan, Boolean) first is the added filter plan, value is the identifier that represent whether
     *         need to add filter.
     *         return null if add filter fail.
     */
    public static Pair<Plan, Boolean> addFilterOnTableScan(Plan queryPlan, Map<BaseTableInfo,
            Set<String>> partitionOnOriginPlan, String partitionColumn, CascadesContext parentCascadesContext) {
        // Firstly, construct filter form invalid partition, this filter should be added on origin plan
        PredicateAddContext predicateAddContext = new PredicateAddContext(partitionOnOriginPlan, partitionColumn);
        Plan queryPlanWithUnionFilter = queryPlan.accept(new PredicateAdder(),
                predicateAddContext);
        if (!predicateAddContext.isHandleSuccess()) {
            return null;
        }
        if (!predicateAddContext.isNeedAddFilter()) {
            return Pair.of(queryPlan, false);
        }
        // Deep copy the plan to avoid the plan output is the same with the later union output, this may cause
        // exec by mistake
        queryPlanWithUnionFilter = new LogicalPlanDeepCopier().deepCopy(
                (LogicalPlan) queryPlanWithUnionFilter, new DeepCopierContext());
        // rbo rewrite after adding filter on origin plan
        return Pair.of(MaterializedViewUtils.rewriteByRules(parentCascadesContext, context -> {
            Rewriter.getWholeTreeRewriter(context).execute();
            return context.getRewritePlan();
        }, queryPlanWithUnionFilter, queryPlan), true);
    }

    /**
     * Expressions may appear in three place in hype graph, this identifies the position where
     * expression appear in hyper graph
     */
    public static enum ExpressionPosition {
        JOIN_EDGE,
        NODE,
        FILTER_EDGE
    }
}
