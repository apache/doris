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

import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * StructInfo for plan, this contains necessary info for query rewrite by materialized view
 */
public class StructInfo {
    public static final JoinPatternChecker JOIN_PATTERN_CHECKER = new JoinPatternChecker();
    public static final AggregatePatternChecker AGGREGATE_PATTERN_CHECKER = new AggregatePatternChecker();
    // struct info splitter
    public static final PlanSplitter PLAN_SPLITTER = new PlanSplitter();
    private static final RelationCollector RELATION_COLLECTOR = new RelationCollector();
    private static final PredicateCollector PREDICATE_COLLECTOR = new PredicateCollector();
    // source data
    private final Plan originalPlan;
    private final HyperGraph hyperGraph;
    private boolean valid = true;
    // derived data following
    // top plan which may include project or filter, except for join and scan
    private Plan topPlan;
    // bottom plan which top plan only contain join or scan. this is needed by hyper graph
    private Plan bottomPlan;
    private final List<CatalogRelation> relations = new ArrayList<>();
    // this is for LogicalCompatibilityContext later
    private final Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap = new HashMap<>();
    // this recorde the predicates which can pull up, not shuttled
    private Predicates predicates;
    // split predicates is shuttled
    private SplitPredicate splitPredicate;
    private EquivalenceClass equivalenceClass;
    // this is for LogicalCompatibilityContext later
    private final Map<Expression, Expression> shuttledHashConjunctsToConjunctsMap = new HashMap<>();

    private StructInfo(Plan originalPlan, @Nullable Plan topPlan, @Nullable Plan bottomPlan, HyperGraph hyperGraph) {
        this.originalPlan = originalPlan;
        this.hyperGraph = hyperGraph;
        this.topPlan = topPlan;
        this.bottomPlan = bottomPlan;
        init();
    }

    private void init() {
        // split the top plan to two parts by join node
        if (topPlan == null || bottomPlan == null) {
            PlanSplitContext planSplitContext = new PlanSplitContext(Sets.newHashSet(LogicalJoin.class));
            originalPlan.accept(PLAN_SPLITTER, planSplitContext);
            this.bottomPlan = planSplitContext.getBottomPlan();
            this.topPlan = planSplitContext.getTopPlan();
        }
        collectStructInfoFromGraph();
        initPredicates();
        predicatesDerive();
    }

    public void addPredicates(List<Expression> canPulledUpExpressions) {
        canPulledUpExpressions.forEach(this.predicates::addPredicate);
        predicatesDerive();
    }

    private void collectStructInfoFromGraph() {
        // Collect expression from join condition in hyper graph
        this.hyperGraph.getJoinEdges().forEach(edge -> {
            List<Expression> hashJoinConjuncts = edge.getHashJoinConjuncts();
            hashJoinConjuncts.forEach(conjunctExpr -> {
                // shuttle expression in edge for LogicalCompatibilityContext later
                shuttledHashConjunctsToConjunctsMap.put(
                        ExpressionUtils.shuttleExpressionWithLineage(
                                Lists.newArrayList(conjunctExpr), edge.getJoin()).get(0),
                        conjunctExpr);
            });
            List<Expression> otherJoinConjuncts = edge.getOtherJoinConjuncts();
            if (!otherJoinConjuncts.isEmpty()) {
                this.valid = false;
            }
        });
        if (!this.isValid()) {
            return;
        }
        // Collect relations from hyper graph which in the bottom plan
        this.hyperGraph.getNodes().forEach(node -> {
            // plan relation collector and set to map
            Plan nodePlan = node.getPlan();
            List<CatalogRelation> nodeRelations = new ArrayList<>();
            nodePlan.accept(RELATION_COLLECTOR, nodeRelations);
            this.relations.addAll(nodeRelations);
            // every node should only have one relation, this is for LogicalCompatibilityContext
            relationIdStructInfoNodeMap.put(nodeRelations.get(0).getRelationId(), (StructInfoNode) node);
        });
        // Collect expression from where in hyper graph
        this.hyperGraph.getFilterEdges().forEach(filterEdge -> {
            List<? extends Expression> filterExpressions = filterEdge.getExpressions();
            filterExpressions.forEach(predicate -> {
                // this is used for LogicalCompatibilityContext
                ExpressionUtils.extractConjunction(predicate).forEach(expr ->
                        shuttledHashConjunctsToConjunctsMap.put(
                                ExpressionUtils.shuttleExpressionWithLineage(predicate, topPlan), predicate));
            });
        });
    }

    private void initPredicates() {
        // Collect predicate from top plan which not in hyper graph
        this.predicates = Predicates.of();
        Set<Expression> topPlanPredicates = new HashSet<>();
        topPlan.accept(PREDICATE_COLLECTOR, topPlanPredicates);
        topPlanPredicates.forEach(this.predicates::addPredicate);
    }

    // derive some useful predicate by predicates
    private void predicatesDerive() {
        // construct equivalenceClass according to equals predicates
        List<Expression> shuttledExpression = ExpressionUtils.shuttleExpressionWithLineage(
                        new ArrayList<>(this.predicates.getPulledUpPredicates()), originalPlan).stream()
                .map(Expression.class::cast)
                .collect(Collectors.toList());
        SplitPredicate splitPredicate = Predicates.splitPredicates(ExpressionUtils.and(shuttledExpression));
        this.splitPredicate = splitPredicate;

        this.equivalenceClass = new EquivalenceClass();
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
    }

    /**
     * Build Struct info from plan.
     * Maybe return multi structInfo when original plan already be rewritten by mv
     */
    public static List<StructInfo> of(Plan originalPlan) {
        // TODO only consider the inner join currently, Should support outer join
        // Split plan by the boundary which contains multi child
        PlanSplitContext planSplitContext = new PlanSplitContext(Sets.newHashSet(LogicalJoin.class));
        // if single table without join, the bottom is
        originalPlan.accept(PLAN_SPLITTER, planSplitContext);

        List<HyperGraph> structInfos = HyperGraph.toStructInfo(planSplitContext.getBottomPlan());
        return structInfos.stream()
                .map(hyperGraph -> new StructInfo(originalPlan, planSplitContext.getTopPlan(),
                        planSplitContext.getBottomPlan(), hyperGraph))
                .collect(Collectors.toList());
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

    public Map<Expression, Expression> getShuttledHashConjunctsToConjunctsMap() {
        return shuttledHashConjunctsToConjunctsMap;
    }

    public List<? extends Expression> getExpressions() {
        return originalPlan instanceof LogicalProject
                ? ((LogicalProject<Plan>) originalPlan).getProjects() : originalPlan.getOutput();
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
     * JoinPatternChecker
     */
    public static class JoinPatternChecker extends DefaultPlanVisitor<Boolean, Set<JoinType>> {
        @Override
        public Boolean visit(Plan plan, Set<JoinType> requiredJoinType) {
            super.visit(plan, requiredJoinType);
            if (!(plan instanceof Filter)
                    && !(plan instanceof Project)
                    && !(plan instanceof CatalogRelation)
                    && !(plan instanceof Join)) {
                return false;
            }
            if (plan instanceof Join) {
                Join join = (Join) plan;
                if (!requiredJoinType.contains(join.getJoinType())) {
                    return false;
                }
                if (!join.getOtherJoinConjuncts().isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * AggregatePatternChecker
     */
    public static class AggregatePatternChecker extends DefaultPlanVisitor<Boolean, Void> {
        @Override
        public Boolean visit(Plan plan, Void context) {
            if (plan instanceof LogicalAggregate) {
                LogicalAggregate<Plan> aggregate = (LogicalAggregate<Plan>) plan;
                Optional<LogicalRepeat<?>> sourceRepeat = aggregate.getSourceRepeat();
                if (sourceRepeat.isPresent()) {
                    return false;
                }
                super.visit(aggregate, context);
                return true;
            }
            if (plan instanceof LogicalProject || plan instanceof LogicalFilter) {
                super.visit(plan, context);
                return true;
            }
            super.visit(plan, context);
            return false;
        }
    }
}
