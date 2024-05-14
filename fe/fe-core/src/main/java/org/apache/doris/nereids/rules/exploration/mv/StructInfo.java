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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand.PredicateAdder;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
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
    private final BitSet tableBitSet = new BitSet();
    // this is for LogicalCompatibilityContext later
    private final Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap;
    // this recorde the predicates which can pull up, not shuttled
    private Predicates predicates;
    // split predicates is shuttled
    private final SplitPredicate splitPredicate;
    private final EquivalenceClass equivalenceClass;
    // Key is the expression shuttled and the value is the origin expression
    // this is for building LogicalCompatibilityContext later.
    private final Map<ExpressionPosition, Map<Expression, Expression>> shuttledExpressionsToExpressionsMap;
    // Record the exprId and the corresponding expr map, this is used by expression shuttled
    private final Map<ExprId, Expression> namedExprIdAndExprMapping;

    /**
     * The construct method for StructInfo
     */
    public StructInfo(Plan originalPlan, ObjectId originalPlanId, HyperGraph hyperGraph, boolean valid, Plan topPlan,
            Plan bottomPlan, List<CatalogRelation> relations,
            Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap,
            @Nullable Predicates predicates,
            Map<ExpressionPosition, Map<Expression, Expression>> shuttledExpressionsToExpressionsMap,
            Map<ExprId, Expression> namedExprIdAndExprMapping) {
        this.originalPlan = originalPlan;
        this.originalPlanId = originalPlanId;
        this.hyperGraph = hyperGraph;
        this.valid = valid
                && hyperGraph.getNodes().stream().allMatch(n -> ((StructInfoNode) n).getExpressions() != null);
        this.topPlan = topPlan;
        this.bottomPlan = bottomPlan;
        this.relations = relations;
        relations.forEach(relation -> this.tableBitSet.set((int) (relation.getTable().getId())));
        this.relationIdStructInfoNodeMap = relationIdStructInfoNodeMap;
        this.predicates = predicates;
        if (predicates == null) {
            // collect predicate from top plan which not in hyper graph
            Set<Expression> topPlanPredicates = new LinkedHashSet<>();
            topPlan.accept(PREDICATE_COLLECTOR, topPlanPredicates);
            this.predicates = Predicates.of(topPlanPredicates);
        }
        Pair<SplitPredicate, EquivalenceClass> derivedPredicates =
                predicatesDerive(this.predicates, topPlan, tableBitSet);
        this.splitPredicate = derivedPredicates.key();
        this.equivalenceClass = derivedPredicates.value();
        this.shuttledExpressionsToExpressionsMap = shuttledExpressionsToExpressionsMap;
        this.namedExprIdAndExprMapping = namedExprIdAndExprMapping;
    }

    /**
     * Construct StructInfo with new predicates
     */
    public StructInfo withPredicates(Predicates predicates) {
        return new StructInfo(this.originalPlan, this.originalPlanId, this.hyperGraph, this.valid, this.topPlan,
                this.bottomPlan, this.relations, this.relationIdStructInfoNodeMap, predicates,
                this.shuttledExpressionsToExpressionsMap, this.namedExprIdAndExprMapping);
    }

    private static boolean collectStructInfoFromGraph(HyperGraph hyperGraph,
            Plan topPlan,
            Map<ExpressionPosition, Map<Expression, Expression>> shuttledExpressionsToExpressionsMap,
            Map<ExprId, Expression> namedExprIdAndExprMapping,
            List<CatalogRelation> relations,
            Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap) {

        // Collect relations from hyper graph which in the bottom plan firstly
        BitSet hyperTableBitSet = new BitSet();
        hyperGraph.getNodes().forEach(node -> {
            // plan relation collector and set to map
            Plan nodePlan = node.getPlan();
            List<CatalogRelation> nodeRelations = new ArrayList<>();
            nodePlan.accept(RELATION_COLLECTOR, nodeRelations);
            relations.addAll(nodeRelations);
            nodeRelations.forEach(relation -> hyperTableBitSet.set((int) relation.getTable().getId()));
            // every node should only have one relation, this is for LogicalCompatibilityContext
            if (!nodeRelations.isEmpty()) {
                relationIdStructInfoNodeMap.put(nodeRelations.get(0).getRelationId(), (StructInfoNode) node);
            }
        });

        // Collect expression from join condition in hyper graph
        for (JoinEdge edge : hyperGraph.getJoinEdges()) {
            List<Expression> hashJoinConjuncts = edge.getHashJoinConjuncts();
            // shuttle expression in edge for the build of LogicalCompatibilityContext later.
            // Record the exprId to expr map in the processing to strut info
            // TODO get exprId to expr map when complex project is ready in join dege
            hashJoinConjuncts.forEach(conjunctExpr -> {
                ExpressionLineageReplacer.ExpressionReplaceContext replaceContext =
                        new ExpressionLineageReplacer.ExpressionReplaceContext(
                                Lists.newArrayList(conjunctExpr), ImmutableSet.of(),
                                ImmutableSet.of(), hyperTableBitSet);
                topPlan.accept(ExpressionLineageReplacer.INSTANCE, replaceContext);
                // Replace expressions by expression map
                List<Expression> replacedExpressions = replaceContext.getReplacedExpressions();
                putShuttledExpressionsToExpressionsMap(shuttledExpressionsToExpressionsMap,
                        ExpressionPosition.JOIN_EDGE, replacedExpressions.get(0), conjunctExpr);
                // Record this, will be used in top level expression shuttle later, see the method
                // ExpressionLineageReplacer#visitGroupPlan
                namedExprIdAndExprMapping.putAll(replaceContext.getExprIdExpressionMap());
            });
            List<Expression> otherJoinConjuncts = edge.getOtherJoinConjuncts();
            if (!otherJoinConjuncts.isEmpty()) {
                return false;
            }
        }
        // Record expressions in node
        hyperGraph.getNodes().forEach(node -> {
            // plan relation collector and set to map
            StructInfoNode structInfoNode = (StructInfoNode) node;
            // record expressions in node
            if (structInfoNode.getExpressions() != null) {
                structInfoNode.getExpressions().forEach(expression -> {
                    ExpressionLineageReplacer.ExpressionReplaceContext replaceContext =
                            new ExpressionLineageReplacer.ExpressionReplaceContext(
                                    Lists.newArrayList(expression), ImmutableSet.of(),
                                    ImmutableSet.of(), hyperTableBitSet);
                    structInfoNode.getPlan().accept(ExpressionLineageReplacer.INSTANCE, replaceContext);
                    // Replace expressions by expression map
                    List<Expression> replacedExpressions = replaceContext.getReplacedExpressions();
                    putShuttledExpressionsToExpressionsMap(shuttledExpressionsToExpressionsMap,
                            ExpressionPosition.NODE, replacedExpressions.get(0), expression);
                    // Record this, will be used in top level expression shuttle later, see the method
                    // ExpressionLineageReplacer#visitGroupPlan
                    namedExprIdAndExprMapping.putAll(replaceContext.getExprIdExpressionMap());
                });
            }
        });
        // Collect expression from where in hyper graph
        hyperGraph.getFilterEdges().forEach(filterEdge -> {
            List<? extends Expression> filterExpressions = filterEdge.getExpressions();
            filterExpressions.forEach(predicate -> {
                // this is used for LogicalCompatibilityContext
                ExpressionUtils.extractConjunction(predicate).forEach(expr ->
                        putShuttledExpressionsToExpressionsMap(shuttledExpressionsToExpressionsMap,
                                ExpressionPosition.FILTER_EDGE,
                                ExpressionUtils.shuttleExpressionWithLineage(predicate, topPlan, hyperTableBitSet),
                                predicate));
            });
        });
        return true;
    }

    // derive some useful predicate by predicates
    private Pair<SplitPredicate, EquivalenceClass> predicatesDerive(Predicates predicates, Plan originalPlan,
            BitSet tableBitSet) {
        // construct equivalenceClass according to equals predicates
        List<Expression> shuttledExpression = ExpressionUtils.shuttleExpressionWithLineage(
                        new ArrayList<>(predicates.getPulledUpPredicates()), originalPlan, tableBitSet).stream()
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
    public static StructInfo of(Plan originalPlan) {
        return of(originalPlan, originalPlan);
    }

    /**
     * Build Struct info from plan.
     * Maybe return multi structInfo when original plan already be rewritten by mv
     */
    public static StructInfo of(Plan derivedPlan, Plan originalPlan) {
        // Split plan by the boundary which contains multi child
        LinkedHashSet<Class<? extends Plan>> set = Sets.newLinkedHashSet();
        set.add(LogicalJoin.class);
        PlanSplitContext planSplitContext = new PlanSplitContext(set);
        // if single table without join, the bottom is
        derivedPlan.accept(PLAN_SPLITTER, planSplitContext);
        return StructInfo.of(originalPlan, planSplitContext.getTopPlan(), planSplitContext.getBottomPlan(),
                HyperGraph.builderForMv(planSplitContext.getBottomPlan()).build());
    }

    /**
     * The construct method for init StructInfo
     */
    public static StructInfo of(Plan originalPlan, @Nullable Plan topPlan, @Nullable Plan bottomPlan,
            HyperGraph hyperGraph) {
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
        Map<ExpressionPosition, Map<Expression, Expression>> shuttledHashConjunctsToConjunctsMap =
                new LinkedHashMap<>();
        Map<ExprId, Expression> namedExprIdAndExprMapping = new LinkedHashMap<>();
        boolean valid = collectStructInfoFromGraph(hyperGraph, topPlan, shuttledHashConjunctsToConjunctsMap,
                namedExprIdAndExprMapping,
                relationList,
                relationIdStructInfoNodeMap);
        return new StructInfo(originalPlan, originalPlanId, hyperGraph, valid, topPlan, bottomPlan,
                relationList, relationIdStructInfoNodeMap, null, shuttledHashConjunctsToConjunctsMap,
                namedExprIdAndExprMapping);
    }

    /**
     * Build Struct info from group.
     * Maybe return multi structInfo when original plan already be rewritten by mv
     */
    public static StructInfo of(Group group) {
        // TODO build graph from original plan and get relations and predicates from graph
        return null;
    }

    public List<CatalogRelation> getRelations() {
        return relations;
    }

    public Predicates getPredicates() {
        return predicates;
    }

    public EquivalenceClass getEquivalenceClass() {
        return equivalenceClass;
    }

    public Plan getOriginalPlan() {
        return originalPlan;
    }

    public HyperGraph getHyperGraph() {
        return hyperGraph;
    }

    public SplitPredicate getSplitPredicate() {
        return splitPredicate;
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

    public Map<ExpressionPosition, Map<Expression, Expression>> getShuttledExpressionsToExpressionsMap() {
        return shuttledExpressionsToExpressionsMap;
    }

    private static void putShuttledExpressionsToExpressionsMap(
            Map<ExpressionPosition, Map<Expression, Expression>> shuttledExpressionsToExpressionsMap,
            ExpressionPosition expressionPosition,
            Expression key, Expression value) {
        Map<Expression, Expression> expressionExpressionMap = shuttledExpressionsToExpressionsMap.get(
                expressionPosition);
        if (expressionExpressionMap == null) {
            expressionExpressionMap = new LinkedHashMap<>();
            shuttledExpressionsToExpressionsMap.put(expressionPosition, expressionExpressionMap);
        }
        expressionExpressionMap.put(key, value);
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
            if (aggregate.getSourceRepeat().isPresent()) {
                return false;
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
                    || plan instanceof GroupPlan) {
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
     * Add predicates on base table when materialized view scan contains invalid partitions
     */
    public static class InvalidPartitionRemover extends DefaultPlanRewriter<Pair<MTMV, Set<Long>>> {
        // materialized view scan is always LogicalOlapScan, so just handle LogicalOlapScan
        @Override
        public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, Pair<MTMV, Set<Long>> context) {
            if (olapScan.getTable().getName().equals(context.key().getName())) {
                List<Long> selectedPartitionIds = olapScan.getSelectedPartitionIds();
                return olapScan.withSelectedPartitionIds(selectedPartitionIds.stream()
                        .filter(partitionId -> !context.value().contains(partitionId))
                        .collect(Collectors.toList()));
            }
            return olapScan;
        }
    }

    /**
     * Collect partitions which scan used according to given table
     */
    public static class QueryScanPartitionsCollector extends DefaultPlanVisitor<Plan, Map<Long, Set<PartitionItem>>> {
        @Override
        public Plan visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation,
                Map<Long, Set<PartitionItem>> context) {
            TableIf table = catalogRelation.getTable();
            if (!context.containsKey(table.getId())) {
                return catalogRelation;
            }
            // Only support check olap partition currently
            if (catalogRelation instanceof LogicalOlapScan) {
                LogicalOlapScan logicalOlapScan = (LogicalOlapScan) catalogRelation;
                PartitionInfo partitionInfo = logicalOlapScan.getTable().getPartitionInfo();
                logicalOlapScan.getSelectedPartitionIds().stream()
                        .map(partitionInfo::getItem)
                        .forEach(partitionItem -> context.computeIfPresent(table.getId(), (key, oldValue) -> {
                            oldValue.add(partitionItem);
                            return oldValue;
                        }));
            }
            return catalogRelation;
        }
    }

    /**
     * Add filter on table scan according to table filter map
     */
    public static Plan addFilterOnTableScan(Plan queryPlan, Map<TableIf, Set<Expression>> filterOnOriginPlan,
            CascadesContext parentCascadesContext) {
        // Firstly, construct filter form invalid partition, this filter should be added on origin plan
        Plan queryPlanWithUnionFilter = queryPlan.accept(new PredicateAdder(), filterOnOriginPlan);
        // Deep copy the plan to avoid the plan output is the same with the later union output, this may cause
        // exec by mistake
        queryPlanWithUnionFilter = new LogicalPlanDeepCopier().deepCopy(
                (LogicalPlan) queryPlanWithUnionFilter, new DeepCopierContext());
        // rbo rewrite after adding filter on origin plan
        return MaterializedViewUtils.rewriteByRules(parentCascadesContext, context -> {
            Rewriter.getWholeTreeRewriter(context).execute();
            return context.getRewritePlan();
        }, queryPlanWithUnionFilter, queryPlan);
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
