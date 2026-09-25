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

package org.apache.doris.nereids.spm.builder;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Grouping;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeTVFScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnionProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWorkTableReference;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Physical-plan decompiler (M1).
 *
 * Decompiles the optimal physical plan tree output by the optimizer into one
 * semantically equivalent standard SQL (planSql). planSql freezes the optimizer's key
 * decisions (JOIN order, aggregate structure, sort semantics) through SQL structure and
 * HINTs, so the plan can be replayed during query rewrite. This is the core engine of
 * the SPM "plan-to-SQL" approach.
 *
 * Design (design doc 6.2):
 *
 * - Pass-through: physical infrastructure nodes such as PhysicalDistribute and
 *   PhysicalResultSink have no SQL equivalent and return the child result directly.
 * - Merge: a local aggregate (LOCAL) registers the aggregate function names onto the
 *   child relation without adding nesting.
 * - Wrap: Filter / Join / global aggregate / TopN / Project / Window create a new
 *   SQLRelation and set an alias; parent operators reference it as a subquery through
 *   toRelationSQL().
 *
 * Known M1 simplifications:
 *
 * - Scan uses the real column names (no c_N normalization; column-name collision
 *   scenarios are handled in a later milestone).
 */
public class SPMPlan2SQLBuilder extends PlanVisitor<SQLRelation, Void> {

    private static final Logger LOG = LogManager.getLogger(SPMPlan2SQLBuilder.class);

    /** JOIN distribution HINT prefix constants. */
    private static final String HINT_JOIN_BROADCAST = "BROADCAST";
    private static final String HINT_JOIN_SHUFFLE = "SHUFFLE";

    /** Expression printer (carries the columnNames mapping of SQLRelation). */
    private final SPMExprSqlBuilder exprSqlBuilder = new SPMExprSqlBuilder();

    /** Per-decompile alias sequence for LATERAL VIEW clauses whose output slot has no
     * user qualifier (reset in toSQL). */
    private int lateralViewSeq = 0;

    /** CTE body relations keyed by CTEId (registered by visitPhysicalCTEProducer). The
     * consumer references the CTE by its alias instead of inlining the body, so the
     * decompiled planSql keeps the WITH structure - one definition shared by every
     * consumer - like StarRocks does. */
    private final Map<CTEId, SQLRelation> cteBodies = new HashMap<>();

    /** CTEId -> the generated alias (t_N) that references the WITH definition. */
    private final Map<CTEId, String> cteAliases = new HashMap<>();

    /**
     * The WITH entries (alias AS (body)) in producer visit order - the definition
     * order of the emitted WITH clause. They are attached to the ROOT relation in
     * toSQL(): every consumer lies inside the statement, so the definitions are
     * visible everywhere regardless of how deeply the CTE anchors are nested in the
     * physical tree (attaching at each anchor instead would place a definition inside
     * one join branch while a consumer sits in another branch). Producers are visited
     * in dependency order - a consumer requires its producer to be registered already -
     * so the list is a valid SQL definition order (a CTE used by another CTE is
     * defined before it).
     */
    private final List<String> cteDefinitions = new ArrayList<>();

    /** Recursive CTE output column names keyed by CTE name. Registered from the anchor
     * branch of a PhysicalRecursiveUnion; used to name the recursive work-table self
     * reference and the outer reference so the decompiled WITH RECURSIVE stays
     * re-parseable. */
    private final Map<String, List<String>> recursiveCteColumns = new HashMap<>();

    /** Local (partial) aggregate output column -> the aggregate input expression it
     * aggregates, e.g. local partial_sum(x)#N registers N -> x. A global
     * aggregate references such a column as sum(partial_sum(x)#N); the global
     * decompile rewrites it back to sum(x) so the decompiled SQL stays a single
     * logical aggregate (no partial_ intermediate functions). */
    private final Map<ExprId, Expression> localAggParams = new HashMap<>();

    /**
     * Buffer columns produced by the distinct-dedup branch: their defining partial
     * aggregate consumed a DATA column (e.g. DISTINCT_LOCAL's partial_count(key)),
     * not another partial buffer. In a DISTINCT_GLOBAL merge stage a count(...) over
     * such a buffer is the user's count(DISTINCT key).
     *
     * A plan may mix plain aggregates with a distinct one (TPCDS q28:
     * avg(x), count(x), count(DISTINCT x)): the plain count rides along the same
     * DISTINCT_GLOBAL stage, but its buffer is a merge chain (its defining partial
     * aggregated another partial buffer, e.g. partial_count(partial_count(x))), so it
     * must NOT be rendered with DISTINCT. Both chains otherwise resolve to the same
     * data column, which is why the provenance has to be recorded while the
     * intermediate stages are eliminated.
     */
    private final Set<ExprId> distinctMergeBuffers = new HashSet<>();

    /** The OUTERMOST projection of the decompiled tree (by identity). It alone prunes its
     * output to the final user-visible columns; every intermediate projection outputs the
     * full child column set plus its own expressions, so an upper layer (filter / join /
     * aggregate / projection) can always resolve the columns it references. */
    private final Set<Plan> outputProjects =
            Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    /**
     * Identity map: physical plan node -> the ExprIds of its output columns that upper
     * layers actually consume (the final result columns plus every column referenced by
     * a decompiled expression above). An entry ABSENT, or mapped to null, means "no
     * pruning for this node and everything below it" (the pre-pruning behaviour).
     *
     * The decompiled planSql inflates when every intermediate projection re-emits the
     * whole child column set (a deep TPCDS join stack re-lists hundreds of c_N columns
     * per level). This map drives live-column pruning: each SELECT list is filtered to
     * the live columns only, so a column that is never referenced above and is not part
     * of the final result disappears from every intermediate projection. Equivalence is
     * preserved because only dead columns are dropped.
     */
    private final Map<Plan, Set<ExprId>> neededOutputs =
            new java.util.IdentityHashMap<>();

    /**
     * CTEId -> the CTE body output columns referenced by every inlined consumer (the
     * consumer output slots mapped back to their producer slots). Filled while the main
     * query is propagated top-down; the CTE producer subtree is propagated afterwards
     * with exactly this column set, so a big CTE body (a deep TPCDS join stack) only
     * keeps the columns its consumers actually read.
     */
    private final Map<CTEId, Set<ExprId>> cteConsumerNeeds = new HashMap<>();

    /**
     * Per-decompile generated-column-name state: when a decompiled output column has no
     * clean user alias it is exported under a generated {@code c_<seq>} name (c_1, c_2, ...),
     * assigned in decompile order and memoized by the output ExprId so every reference
     * to the same column prints the same alias. A fresh per-decompile sequence (instead
     * of embedding the analyzer's ExprId) keeps the generated names small, readable and
     * independent of how many internal ExprIds the optimizer allocated.
     */
    private final Map<ExprId, String> generatedColumnNames = new HashMap<>();
    private int generatedColumnSeq = 0;

    /**
     * Marks the OUTERMOST projection of the decompiled tree: the first PhysicalProject
     * reached from the root along single-child pass-through nodes (ResultSink / Sort /
     * Distribute / ...). Walking stops at an aggregate - an aggregate that feeds the
     * result carries the final SELECT list itself, so there is no outermost projection
     * and every projection below it is an intermediate one (full child output).
     */
    private void markOutputProject(Plan plan) {
        Plan current = plan;
        while (current != null) {
            if (current instanceof PhysicalProject) {
                outputProjects.add(current);
                return;
            }
            if (current instanceof PhysicalHashAggregate || current.arity() != 1) {
                return;
            }
            current = current.child(0);
        }
    }

    // ==================== live-column analysis (dead-column pruning) ====================

    /** Whether this subtree may be pruned by the live-column analysis. Non-linear
     * decompile shapes (CTE bodies, set operations, recursive unions, grouping sets)
     * are left untouched: their select lists are kept whole. */
    private static boolean isPrunableNode(Plan node) {
        return node instanceof PhysicalProject
                || node instanceof PhysicalWindow
                || node instanceof PhysicalHashJoin
                || node instanceof PhysicalNestedLoopJoin
                || node instanceof PhysicalHashAggregate
                || node instanceof PhysicalFilter
                || node instanceof PhysicalTopN
                || node instanceof PhysicalQuickSort
                || node instanceof PhysicalLimit
                || node instanceof PhysicalDistribute
                || node instanceof PhysicalLazyMaterialize
                || node instanceof PhysicalLazyMaterializeOlapScan
                || node instanceof PhysicalPartitionTopN
                || node instanceof PhysicalAssertNumRows
                || node instanceof PhysicalResultSink
                || node instanceof PhysicalCTEAnchor;
    }

    /** Pre-analysis entry: fills neededOutputs top-down from the root. */
    private void computeNeeded(Plan root) {
        neededOutputs.clear();
        cteConsumerNeeds.clear();
        Set<ExprId> rootNeed = new HashSet<>();
        for (Slot slot : root.getOutput()) {
            rootNeed.add(slot.getExprId());
        }
        propagateNeed(root, rootNeed);
    }

    /**
     * Top-down live-column propagation. need is the set of output ExprIds of
     * node that upper layers consume; null means "keep everything" (never prune
     * below). Every parent hands each child the child's output columns that must stay
     * alive: the columns the parent passes through and the columns the parent's own
     * decompiled expressions reference.
     */
    private void propagateNeed(Plan node, Set<ExprId> need) {
        if (node == null) {
            return;
        }
        if (node instanceof PhysicalCTEConsumer) {
            // a CTE consumer has no children; record which body columns its live output
            // columns map back to, so the producer subtree can be pruned to exactly them
            if (need != null) {
                PhysicalCTEConsumer consumer = (PhysicalCTEConsumer) node;
                Set<ExprId> bodyNeeds = cteConsumerNeeds.computeIfAbsent(
                        consumer.getCteId(), k -> new HashSet<>());
                for (Slot slot : consumer.getOutput()) {
                    if (need.contains(slot.getExprId())) {
                        bodyNeeds.add(consumer.getProducerSlot(slot).getExprId());
                    }
                }
            }
            return;
        }
        if (need == null || !isPrunableNode(node)) {
            // keep everything on this node and below (no pruning boundary)
            for (Plan child : node.children()) {
                propagateNeed(child, null);
            }
            return;
        }
        Set<ExprId> nodeNeed = new HashSet<>(need);
        neededOutputs.put(node, nodeNeed);

        if (node instanceof PhysicalCTEAnchor) {
            // child(0) is the CTE producer whose body every consumer inlines. The main
            // query (child(1)) is propagated FIRST so every consumer records which body
            // columns it reads; the body subtree is then pruned to exactly those columns.
            // A body with no consumer (or one whose consumers never resolve) keeps every
            // column.
            PhysicalCTEAnchor<? extends Plan, ? extends Plan> anchor =
                    (PhysicalCTEAnchor<? extends Plan, ? extends Plan>) node;
            if (anchor.child(1) != null) {
                propagateNeed(anchor.child(1), nodeNeed);
            }
            Plan producer = anchor.child(0);
            if (producer == null) {
                return;
            }
            if (producer instanceof PhysicalCTEProducer) {
                PhysicalCTEProducer<? extends Plan> cteProducer =
                        (PhysicalCTEProducer<? extends Plan>) producer;
                Set<ExprId> bodyNeed = cteConsumerNeeds.get(cteProducer.getCteId());
                if (bodyNeed != null && !bodyNeed.isEmpty()) {
                    propagateNeed(cteProducer.child(0), bodyNeed);
                } else {
                    propagateNeed(cteProducer.child(0), null);
                }
            } else {
                propagateNeed(producer, null);
            }
            return;
        }
        if (node instanceof PhysicalProject) {
            propagateProjectNeed((PhysicalProject<? extends Plan>) node, nodeNeed);
        } else if (node instanceof PhysicalHashJoin || node instanceof PhysicalNestedLoopJoin) {
            propagateJoinNeed((AbstractPhysicalJoin<? extends Plan, ? extends Plan>) node, nodeNeed);
        } else if (node instanceof PhysicalHashAggregate) {
            propagateAggregateNeed((PhysicalHashAggregate<? extends Plan>) node, nodeNeed);
        } else if (node instanceof PhysicalWindow) {
            propagateWindowNeed((PhysicalWindow<? extends Plan>) node, nodeNeed);
        } else if (node instanceof PhysicalFilter) {
            PhysicalFilter<? extends Plan> filter = (PhysicalFilter<? extends Plan>) node;
            Set<ExprId> childNeed = new HashSet<>(nodeNeed);
            addExprSlots(filter.getPredicate(), childNeed);
            propagateNeed(filter.child(0), childNeed);
        } else if (node instanceof PhysicalTopN) {
            PhysicalTopN<? extends Plan> topN = (PhysicalTopN<? extends Plan>) node;
            Set<ExprId> childNeed = new HashSet<>(nodeNeed);
            for (org.apache.doris.nereids.properties.OrderKey key : topN.getOrderKeys()) {
                addExprSlots(key.getExpr(), childNeed);
            }
            propagateNeed(topN.child(0), childNeed);
        } else if (node instanceof PhysicalQuickSort) {
            PhysicalQuickSort<? extends Plan> sort = (PhysicalQuickSort<? extends Plan>) node;
            Set<ExprId> childNeed = new HashSet<>(nodeNeed);
            for (org.apache.doris.nereids.properties.OrderKey key : sort.getOrderKeys()) {
                addExprSlots(key.getExpr(), childNeed);
            }
            propagateNeed(sort.child(0), childNeed);
        } else {
            // pure pass-through nodes (limit / distribute / lazy materialize / ...):
            // their output slots are the child slots, so the same need flows down
            for (Plan child : node.children()) {
                propagateNeed(child, nodeNeed);
            }
        }
    }

    /** PhysicalProject: live pass-through columns plus the columns referenced by the
     * project's own expressions whose output is itself live. */
    private void propagateProjectNeed(PhysicalProject<? extends Plan> project, Set<ExprId> need) {
        Plan child = project.child(0);
        if (child == null) {
            return;
        }
        Set<ExprId> childNeed = new HashSet<>();
        boolean finalProject = outputProjects.contains(project);
        if (finalProject) {
            // the outermost projection emits its full projection list: every referenced
            // child column must stay alive
            for (NamedExpression projectExpr : project.getProjects()) {
                addExprSlots(projectExpr, childNeed);
            }
        } else {
            // pass-through: the child columns that are still live above this projection
            Set<ExprId> childOut = outputIdSet(child);
            for (ExprId id : need) {
                if (childOut.contains(id)) {
                    childNeed.add(id);
                }
            }
            // plus the columns referenced by this projection's own live expressions
            for (NamedExpression projectExpr : project.getProjects()) {
                if (need.contains(projectExpr.getExprId())) {
                    addExprSlots(projectExpr, childNeed);
                }
            }
        }
        propagateNeed(child, childNeed);
    }

    /** Join: the preserved output columns flow to the side that produces them; the
     * hash / other / mark conjuncts are always decompiled, so every column they
     * reference stays alive on its own side. */
    private void propagateJoinNeed(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join,
            Set<ExprId> need) {
        Plan left = join.left();
        Plan right = join.right();
        if (left == null || right == null) {
            return;
        }
        Set<ExprId> leftOut = outputIdSet(left);
        Set<ExprId> rightOut = outputIdSet(right);
        Set<ExprId> conjRefs = new HashSet<>();
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            addExprSlots(conjunct, conjRefs);
        }
        for (Expression conjunct : join.getOtherJoinConjuncts()) {
            addExprSlots(conjunct, conjRefs);
        }
        for (Expression conjunct : join.getMarkJoinConjuncts()) {
            addExprSlots(conjunct, conjRefs);
        }
        Set<ExprId> leftNeed = new HashSet<>();
        Set<ExprId> rightNeed = new HashSet<>();
        for (ExprId id : need) {
            if (leftOut.contains(id)) {
                leftNeed.add(id);
            }
            if (rightOut.contains(id)) {
                rightNeed.add(id);
            }
        }
        for (ExprId id : conjRefs) {
            if (leftOut.contains(id)) {
                leftNeed.add(id);
            }
            if (rightOut.contains(id)) {
                rightNeed.add(id);
            }
        }
        propagateNeed(left, leftNeed);
        propagateNeed(right, rightNeed);
    }

    /** Aggregate: the global stage keeps its whole output list (GROUP BY keys and
     * aggregate functions), but the input columns its expressions reference must stay
     * alive below. Local / intermediate execution stages are pass-through nodes. */
    private void propagateAggregateNeed(PhysicalHashAggregate<? extends Plan> agg, Set<ExprId> need) {
        AggPhase phase = agg.getAggPhase();
        if (phase.isLocal() || isIntermediateAggStage(agg)) {
            for (Plan child : agg.children()) {
                propagateNeed(child, need);
            }
            return;
        }
        Plan child = agg.child(0);
        if (child == null) {
            return;
        }
        Set<ExprId> childNeed = new HashSet<>();
        for (Expression groupBy : agg.getGroupByExpressions()) {
            addExprSlots(groupBy, childNeed);
        }
        for (NamedExpression output : agg.getOutputExpressions()) {
            addAggregateOutputRefs(output, childNeed);
        }
        propagateNeed(child, childNeed);
    }

    /** Window: live pass-through columns plus the input columns of the live window
     * expressions. */
    private void propagateWindowNeed(PhysicalWindow<? extends Plan> window, Set<ExprId> need) {
        Plan child = window.child(0);
        if (child == null) {
            return;
        }
        Set<ExprId> childNeed = new HashSet<>();
        Set<ExprId> childOut = outputIdSet(child);
        for (ExprId id : need) {
            if (childOut.contains(id)) {
                childNeed.add(id);
            }
        }
        for (NamedExpression windowExpr : window.getWindowExpressions()) {
            if (need.contains(windowExpr.getExprId())) {
                addExprSlots(windowExpr, childNeed);
            }
        }
        propagateNeed(child, childNeed);
    }

    /** The columns referenced by one global-aggregate output expression, with every
     * local partial-buffer reference resolved down to its data columns (mirrors
     * appendAggSelect / resolveBufferSlots so the pruned SELECT lists keep exactly the
     * columns the decompiled aggregate prints). */
    private void addAggregateOutputRefs(NamedExpression output, Set<ExprId> out) {
        Expression inner = output instanceof Alias ? ((Alias) output).child() : output;
        if (inner instanceof SlotReference) {
            // group-by key pass-through
            out.add(((SlotReference) inner).getExprId());
            return;
        }
        if (inner instanceof AggregateExpression) {
            AggregateExpression aggExpr = (AggregateExpression) inner;
            List<Expression> args = aggExpr.getFunction().children().isEmpty()
                    ? new ArrayList<>(aggExpr.children()) : aggExpr.getFunction().children();
            for (Expression arg : args) {
                addResolvedExprSlots(arg, out);
            }
            return;
        }
        addExprSlots(inner, out);
    }

    /** Collects every slot of expr, resolving partial-buffer slots through
     * localAggParams down to their data columns (mirrors resolveBufferSlots). */
    private void addResolvedExprSlots(Expression expr, Set<ExprId> out) {
        if (expr instanceof SlotReference) {
            Expression param = localAggParams.get(((SlotReference) expr).getExprId());
            if (param != null) {
                addResolvedExprSlots(param, out);
                return;
            }
            out.add(((SlotReference) expr).getExprId());
            return;
        }
        for (Expression childExpr : expr.children()) {
            addResolvedExprSlots(childExpr, out);
        }
    }

    /** Output ExprId set of a plan node. */
    private static Set<ExprId> outputIdSet(Plan node) {
        Set<ExprId> ids = new HashSet<>();
        for (Slot slot : node.getOutput()) {
            ids.add(slot.getExprId());
        }
        return ids;
    }

    /** Adds every slot ExprId used by an expression. */
    private static void addExprSlots(Expression expr, Set<ExprId> out) {
        if (expr == null) {
            return;
        }
        collectAllSlotIds(expr, out);
    }

    /** Pre-walk that fills localAggParams bottom-up so the live-column analysis
     * (which runs before the decompile walk) can resolve partial-buffer references. The
     * decompile walk re-fills the same map while it descends (harmless duplicate). */
    private void collectLocalAggParams(Plan node) {
        for (Plan child : node.children()) {
            collectLocalAggParams(child);
        }
        if (node instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<? extends Plan> agg = (PhysicalHashAggregate<? extends Plan>) node;
            if (agg.getAggPhase().isLocal() || isIntermediateAggStage(agg)) {
                recordLocalAggStage(agg);
            }
        }
    }

    /** Registers one local (partial) aggregate stage's buffer outputs (see the field
     * comment of localAggParams). */
    private void recordLocalAggStage(PhysicalHashAggregate<? extends Plan> agg) {
        for (NamedExpression output : agg.getOutputExpressions()) {
            Expression inner = output instanceof Alias ? ((Alias) output).child() : output;
            if (inner instanceof AggregateExpression && isPartialAggregate((AggregateExpression) inner)) {
                Expression rawParam = extractPartialParam((AggregateExpression) inner);
                if (agg.getGroupByExpressions().isEmpty()
                        && isDistinctMergeContribution(((AggregateExpression) inner).children())) {
                    distinctMergeBuffers.add(output.getExprId());
                }
                Expression param = rawParam;
                if (param == null) {
                    // count(*) has no data argument (extractPartialParam -> null). Map the
                    // buffer to the partial expression itself so the enclosing
                    // merge-finalize count(*) resolves its argument back to the star, which
                    // appendAggSelect collapses into count(*) (isNestedNoArgCount) - without
                    // this the raw buffer slot (named e.g. "partial_count(*)") would leak
                    // into the decompiled SQL as the invalid count(partial_count(*)).
                    param = inner;
                }
                while (param instanceof SlotReference) {
                    Expression resolved = localAggParams.get(((SlotReference) param).getExprId());
                    if (resolved == null) {
                        break;
                    }
                    param = resolved;
                }
                localAggParams.put(output.getExprId(), param);
            }
        }
    }

    /**
     * Whether an output of an eliminated DISTINCT_LOCAL stage is the distinct-dedup
     * contribution (see distinctMergeBuffers).
     *
     * The distinction has to be read from the aggregate expression's own children -
     * the physical input of the buffer - because the merge function's argument is
     * normalized to the data column for EVERY buffer:
     *
     * - merge-chain buffer (plain aggregate riding along): its input is another
     *   partial buffer, already recorded in localAggParams, or a count-star function
     *   with no argument at all;
     * - distinct-dedup buffer: its input is the dedup KEY data column, either as the
     *   bare slot (partial_count(key)) or wrapped as the partial function's argument
     *   (count(key)).
     */
    private boolean isDistinctMergeContribution(List<Expression> exprChildren) {
        if (exprChildren.isEmpty()) {
            return false;
        }
        Expression bufferArg = exprChildren.get(0);
        if (bufferArg instanceof SlotReference) {
            return !localAggParams.containsKey(((SlotReference) bufferArg).getExprId());
        }
        List<Expression> argChildren = bufferArg.children();
        if (argChildren.isEmpty()) {
            // e.g. the count(*) star buffer: raw-row counting, never the distinct merge
            return false;
        }
        for (Expression child : argChildren) {
            if (!(child instanceof SlotReference)
                    || localAggParams.containsKey(((SlotReference) child).getExprId())) {
                return false;
            }
        }
        return true;
    }

    /** The live column filter for one explicit SELECT list: the list a node emits is
     * pruned to the node's needed output columns (no entry in the map, or a null need,
     * keeps the whole list - e.g. mock plan trees and unpruned subtrees). */
    private void filterLiveSelects(Plan node, List<Pair<ExprId, String>> selects) {
        Set<ExprId> need = neededOutputs.get(node);
        if (need == null) {
            return;
        }
        selects.removeIf(p -> !need.contains(p.key()));
    }

    /**
     * Decompile entry: physical plan -> planSql.
     *
     * @param plan the optimal physical plan
     * @return planSql (standard SQL text)
     */
    public String toSQL(Plan plan) {
        // reset the per-decompile alias / generated-name sequences: t_N and c_N only
        // need to be unique WITHIN the one produced SQL, so numbering restarts here and
        // the decompiled text stays compact across calls
        SQLRelation.resetAliasCounter();
        cteBodies.clear();
        cteAliases.clear();
        cteDefinitions.clear();
        generatedColumnNames.clear();
        generatedColumnSeq = 0;
        lateralViewSeq = 0;
        outputProjects.clear();
        localAggParams.clear();
        distinctMergeBuffers.clear();
        markOutputProject(plan);
        collectLocalAggParams(plan);
        computeNeeded(plan);
        SQLRelation relation = plan.accept(this, null);
        // attach the collected CTE definitions (WITH) to the outermost relation: they
        // are collected in producer dependency order and are visible to the whole
        // statement, which is exactly the scope a CTE anchor binds
        if (!cteDefinitions.isEmpty()) {
            List<String> merged = relation.getCte() == null
                    ? new ArrayList<>() : new ArrayList<>(relation.getCte());
            merged.addAll(cteDefinitions);
            relation.setCte(merged);
        }
        // A top-level ASSERT_ROWS (e.g. EXISTS / single-row assertion) renders the
        // ASSERT_ROWS wrapper; nested ones are already handled by toRelationSQL().
        if (relation.isAssertRows()) {
            return "ASSERT_ROWS (" + relation.toSQL() + ") " + relation.getRelationAlias();
        }
        return relation.toSQL();
    }

    /**
     * Handles a child node.
     *
     * @param plan child physical plan
     * @return SQLRelation of the child plan
     */
    private SQLRelation process(Plan plan) {
        return plan.accept(this, null);
    }

    // ==================== default: unsupported operators ====================

    @Override
    public SQLRelation visit(Plan plan, Void context) {
        throw new UnsupportedOperationException(
                "SPMPlan2SQLBuilder does not support plan: " + plan.getClass().getSimpleName());
    }

    // ==================== pass-through mode ====================
    /**
     * PhysicalStorageLayerAggregate: the cloud storage-layer aggregation pushdown
     * (COUNT / COUNT_ON_MATCH / MIN_MAX / MIX) REPLACES the aggregate - the pushdown node
     * itself computes the result and the enclosing operator references its output slot
     * directly, so the plan visitor default (decompile the wrapped relation) would
     * silently drop the aggregation and freeze e.g. "SELECT * FROM t" for a count(*)
     * query. No clause re-expresses the pushed-down state faithfully, so fail the
     * decompile: CREATE keeps the user-supplied planSql text and the rewrite degrades to
     * the parameterized-tree path.
     */
    @Override
    public SQLRelation visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, Void context) {
        throw new UnsupportedOperationException(
                "SPM decompile: storage-layer aggregate pushdown (cloud count / min-max)"
                        + " is not supported yet");
    }

    /**
     * PhysicalDistribute: a data exchange node with no SQL equivalent; returns the child.
     */
    @Override
    public SQLRelation visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute, Void context) {
        return process(distribute.child(0));
    }

    /**
     * PhysicalLazyMaterialize: an optimizer lazy-column-materialization wrapper with no
     * SQL equivalent; returns the child.
     */
    @Override
    public SQLRelation visitPhysicalLazyMaterialize(
            PhysicalLazyMaterialize<? extends Plan> lazy, Void context) {
        return process(lazy.child(0));
    }

    /**
     * PhysicalLazyMaterializeOlapScan: an OlapScan wrapped with lazy column
     * materialization; decompiled as a normal scan.
     */
    @Override
    public SQLRelation visitPhysicalLazyMaterializeOlapScan(
            PhysicalLazyMaterializeOlapScan scan, Void context) {
        return visitPhysicalRelation(scan, context);
    }

    /**
     * PhysicalResultSink (and sinks in general): result collection nodes with no SQL
     * equivalent; passed through. For the RESULT sink the final output column ORDER is
     * forced to the sink's output list (the user SELECT order) - the physical aggregate
     * / projection below may emit columns in a different order (e.g. group-by keys
     * before aggregates), which would reorder the user-visible result columns.
     */
    @Override
    public SQLRelation visitPhysicalSink(PhysicalSink<? extends Plan> sink, Void context) {
        if (sink instanceof PhysicalResultSink) {
            SQLRelation child = process(sink.child(0));
            List<Slot> outputs = sink.getOutput();
            if (outputs.isEmpty()) {
                return child;
            }
            // Final output columns in the USER SELECT order. Each output column is
            // emitted as its in-scope reference (child.getColumnNames()), and - when the
            // reference is a decompile-internal alias (c_N) that hides the original
            // column label - re-labelled with the ResultSink output slot's name
            // (output.getName()): that label is exactly what a normal execution of the
            // original query shows in the result header (a plain alias such as
            // "d_week_seq1", a table column name, or the expression text of an
            // un-aliased expression column), so the frozen planSql keeps the SAME output
            // column names as the original SQL (SR model: the final SELECT list is
            // driven by the logical query's output columns, not by the internal c_N
            // aliases the decompiler had to mint for unambiguous references).
            List<Pair<ExprId, String>> ordered = new ArrayList<>();
            boolean anyRenamed = false;
            for (Slot output : outputs) {
                String ref = child.getColumnNames().get(output.getExprId());
                if (ref == null) {
                    ref = output.getName();
                }
                String display = output.getName();
                if (display == null || display.isEmpty() || display.equals(ref)) {
                    ordered.add(Pair.of(output.getExprId(), ref));
                    continue;
                }
                // Re-expose the column under its original label: "c_3 AS d_week_seq1"
                // (or "AS `round(...)`" for an un-aliased expression column, whose header
                // text is the expression itself). The quoted name never participates in
                // name resolution of the frozen SQL body, so quoting is always safe.
                ordered.add(Pair.of(output.getExprId(), ref + " AS " + quoteIdentifier(display)));
                anyRenamed = true;
            }
            if (child.getRelationName() == null) {
                // inline relation (plain table scan): SELECT * renders in the table
                // schema order and the ResultSink output follows the scan order;
                // nothing to reorder.
                return child;
            }
            if (child.getSelects().isEmpty()) {
                // SELECT * over a wrapped subquery (TopN/Sort wrapper whose FROM is a
                // subquery): the columns are referenceable, so rewrite the outer SELECT
                // list to the user column order in place (preserving ORDER BY / LIMIT).
                child.setSelects(ordered);
                return child;
            }
            // Explicit projection already emitted (e.g. a bare aggregate
            // "sum(...) AS revenue" or a pass-through projection). Overwriting it in
            // place would replace the expression with a bare name that is not resolvable
            // in the FROM scope, so:
            //  - if the emitted columns already carry the original labels in the user
            //    SELECT order, keep the projection untouched;
            //  - otherwise wrap the child in a subquery and select the ordered
            //    (re-labelled) columns from it.
            if (!anyRenamed && sameExprIdOrder(child.getSelects(), ordered)) {
                return child;
            }
            SQLRelation outer = new SQLRelation();
            String alias = outer.newAlias();
            outer.setFrom("(" + child.toSQL() + ") " + alias);
            outer.setSelects(ordered);
            return outer;
        }
        return visit((Plan) sink, context);
    }

    /** Whether the projection ExprIds appear in the same order as ordered. */
    private static boolean sameExprIdOrder(List<Pair<ExprId, String>> projection,
            List<Pair<ExprId, String>> ordered) {
        if (projection.size() != ordered.size()) {
            return false;
        }
        for (int i = 0; i < projection.size(); i++) {
            if (projection.get(i).key() != ordered.get(i).key()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Quotes an identifier for use as executable SQL text whenever it is not a plain
     * {@code [A-Za-z_][A-Za-z0-9_]*} identifier: a column named {@code a-b} must be
     * emitted as {@code `a-b`}, otherwise the frozen projection re-parses as the
     * subtraction a - b. Embedded backticks are doubled. Plain names stay verbatim, so
     * ordinary schemas keep byte-identical frozen SQL. Also used for result-column
     * labels (the expression text of an un-aliased output column such as
     * {@code round((sun_sales1 / sun_sales2), 2)} is wrapped so the frozen planSql can
     * carry the original column header verbatim).
     */
    static String quoteIdentifier(String name) {
        if (name == null) {
            return null;
        }
        if (name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return name;
        }
        return "`" + name.replace("`", "``") + "`";
    }

    /**
     * Quotes every dot-separated component of a (possibly) qualified metadata name
     * (catalog.db.table), so a table whose name is not a plain identifier (`my-table`)
     * is still emitted as an identifier reference rather than as an expression.
     */
    static String quoteQualifiedName(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        String[] parts = name.split("\\.", -1);
        StringBuilder sb = new StringBuilder(name.length() + 4);
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append('.');
            }
            sb.append(quoteIdentifier(parts[i]));
        }
        return sb.toString();
    }

    // ==================== Scan (wrapped as subquery or inline) ====================

    /**
     * PhysicalRelation (PhysicalOlapScan / PhysicalFileScan, etc.): table scan.
     *
     * M1 simplification: the scan output columns are registered to columnNames by their
     * real column names and from is inlined as the table name (not wrapped). The
     * predicate is handled by the parent PhysicalFilter.
     */
    @Override
    public SQLRelation visitPhysicalRelation(PhysicalRelation relation, Void context) {
        if (!(relation instanceof PhysicalCatalogRelation)) {
            throw new UnsupportedOperationException(
                    "SPMPlan2SQLBuilder does not support relation: " + relation.getClass().getSimpleName());
        }
        PhysicalCatalogRelation catalogRelation = (PhysicalCatalogRelation) relation;
        rejectUnsupportedScan(relation);
        SQLRelation sqlRelation = new SQLRelation();
        // Emit the fully qualified name (catalog.db.table) so the frozen planSql resolves
        // the same table when it is replayed from a session whose current database (or
        // catalog) differs from the one used at CREATE time (cross-db queries,
        // information_schema, ...). Tables without a database (e.g. FunctionGenTable)
        // keep the bare name. Each component is backtick-quoted when it is not a plain
        // identifier, so a metadata name containing operators is re-parsed as an
        // identifier instead of an expression.
        sqlRelation.setFrom(catalogRelation.getTable().getDatabase() == null
                ? quoteIdentifier(catalogRelation.getTable().getName())
                : quoteQualifiedName(catalogRelation.getTable().getNameWithFullQualifiers()));
        // Register output columns: ExprId -> real column name. Internal system columns
        // (e.g. rowid columns a join may request from the scan) are execution details
        // and are never registered so they cannot leak into projections / ON clauses.
        for (Slot slot : relation.getOutput()) {
            if (isSystemColumnName(slot.getName())) {
                continue;
            }
            // registered as executable SQL text: a special-character column (`a-b`)
            // must be backtick-quoted, otherwise a frozen projection SELECT a-b
            // re-parses as the subtraction a - b and returns a different value
            sqlRelation.registerRef(slot.getExprId(), quoteIdentifier(slot.getName()));
        }
        return sqlRelation;
    }

    // ==================== table-valued functions ====================

    /**
     * PhysicalTVFRelation: a table-valued function in FROM (e.g.
     * numbers('number' = '5')). The function's own SQL text is used because a TVF
     * argument list is a property list, not a normal expression list; the output
     * columns keep their names.
     */
    @Override
    public SQLRelation visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, Void context) {
        SQLRelation relation = new SQLRelation();
        relation.setFrom(tvfRelation.getFunction().toSql());
        for (Slot slot : tvfRelation.getOutput()) {
            relation.registerRef(slot.getExprId(), quoteIdentifier(slot.getName()));
        }
        return relation;
    }

    /** PhysicalLazyMaterializeTVFScan: a TVF scan wrapped by lazy materialization. */
    @Override
    public SQLRelation visitPhysicalLazyMaterializeTVFScan(
            PhysicalLazyMaterializeTVFScan scan, Void context) {
        return visitPhysicalTVFRelation(scan, context);
    }

    /**
     * A scan carrying execution modifiers the decompiler cannot express in plain SQL must
     * never be frozen as an unrestricted catalog.db.table scan: replay would silently run
     * over all eligible partitions (e.g. FROM t PARTITION(p1) freezes and later executes
     * over every partition), use the wrong index, or drop the sampling. Fail the decompile
     * so CREATE falls back to the user-supplied planSql text instead.
     *
     * Note: selectedTabletIds is deliberately NOT a rejection criterion - bucket pruning
     * is derived from the query's own predicates (rule PruneOlapScanTablet), so the
     * replayed SQL re-derives the same selection; only sample / partition / index
     * selections are not reconstructible from the frozen text.
     */
    private static void rejectUnsupportedScan(PhysicalRelation relation) {
        if (relation instanceof org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan) {
            org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan scan =
                    (org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan) relation;
            boolean partitionSubset = !scan.getSelectedPartitionIds().isEmpty()
                    && scan.getSelectedPartitionIds().size()
                            != scan.getTable().getPartitions().size();
            if (scan.getSelectedIndexId() != scan.getTable().getBaseIndexId() || partitionSubset
                    || scan.getTableSample().isPresent()) {
                throw new UnsupportedOperationException(
                        "SPM decompile: restricted olap scan (index/partition/sample selection)"
                                + " is not supported yet");
            }
            return;
        }
        if (relation instanceof PhysicalFileScan) {
            // File scans (external catalogs) carry the same class of modifiers - partition
            // pruning state, TABLESAMPLE, FOR VERSION AS OF snapshot state and scan
            // parameters - while the generic serializer emits only catalog.db.table. A
            // placeholder-bearing "FOR VERSION AS OF 123 ... WHERE k = 1" baseline would
            // replay against the current unrestricted table and return different rows,
            // so every non-default modifier fails the decompile: CREATE keeps the user
            // planSql text and the rewrite degrades to the parameterized-tree path.
            PhysicalFileScan scan = (PhysicalFileScan) relation;
            boolean partitionPruned = scan.getSelectedPartitions() != null
                    && scan.getSelectedPartitions() != LogicalFileScan.SelectedPartitions.NOT_PRUNED;
            if (partitionPruned || scan.getTableSample().isPresent()
                    || scan.getTableSnapshot().isPresent() || scan.getScanParams().isPresent()) {
                throw new UnsupportedOperationException(
                        "SPM decompile: restricted file scan"
                                + " (partition/sample/snapshot/scan params) is not supported yet");
            }
        }
    }

    // ==================== Generate (LATERAL VIEW) ====================

    /**
     * PhysicalGenerate: one LATERAL VIEW clause over the child relation. The child
     * relation's FROM is extended with a LATERAL VIEW clause carrying the generator,
     * the table alias and the column list; the generator output columns are registered
     * on the same relation so parent operators can reference them. The alias is taken
     * from the user's alias when it survived analysis (slot qualifier), otherwise a
     * per-decompile lv_N alias is used. Generate conjuncts (if any) are appended to the
     * relation's WHERE.
     */
    @Override
    public SQLRelation visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, Void context) {
        SQLRelation childRelation = process(generate.child(0));
        if (generate.getGenerators().size() != 1) {
            throw new UnsupportedOperationException("SPM decompile generate: expected one generator, got "
                    + generate.getGenerators().size());
        }
        // The LATERAL VIEW must attach to the child's COMPLETE query block. Attaching it
        // to the bare FROM fragment (getFrom()) would keep the child's WHERE / GROUP BY /
        // HAVING / ORDER BY / LIMIT clauses on the OUTER relation, where they apply AFTER
        // the explode: a derived table with LIMIT 10 would limit the exploded rows
        // instead of the lateral-view input, a GROUP BY would regroup the generator
        // output, and frozen replay could return rows the captured plan filtered out.
        // A FROM-less child (e.g. LATERAL VIEW over "SELECT 1 AS x") needs the wrapper
        // for the same reason.
        SQLRelation relation;
        String baseSql;
        if (childRelation.getFrom().isEmpty() || childRelation.hasOwnBlock()) {
            if (childRelation.getRelationName() == null) {
                childRelation.newAlias();
            }
            baseSql = childRelation.toRelationSQL();
            // the wrapper carries the child's column mapping only; the child's clauses
            // stay inside baseSql (moving them onto this relation would re-apply them
            // after the lateral view)
            relation = new SQLRelation();
            relation.getColumnNames().putAll(childRelation.getColumnNames());
        } else {
            relation = childRelation;
            baseSql = relation.getFrom();
        }
        List<Slot> outputs = generate.getGeneratorOutput();
        String alias = "";
        for (Slot slot : outputs) {
            if (!slot.getQualifier().isEmpty()) {
                alias = slot.getQualifier().get(slot.getQualifier().size() - 1);
                break;
            }
        }
        if (alias.isEmpty()) {
            alias = "lv_" + (lateralViewSeq++);
        }
        // The analyzer may name a generator output with an internal column name
        // ("$c$N"); such a name cannot be referenced in SQL, so give it a generated
        // visible name and register the slot under that name for the parent operators.
        List<String> columnNames = new ArrayList<>(outputs.size());
        for (Slot slot : outputs) {
            String name = slot.getName();
            columnNames.add(name == null || name.startsWith("$c$")
                    ? "lv_col_" + (lateralViewSeq++) : name);
        }
        String generatorSql = exprSqlBuilder.print(generate.getGenerators().get(0), relation);
        String columnList = columnNames.stream()
                .map(SPMPlan2SQLBuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));
        relation.setFrom(baseSql + " LATERAL VIEW " + generatorSql + " "
                + quoteIdentifier(alias) + " AS " + columnList);
        for (int i = 0; i < outputs.size(); i++) {
            relation.registerRef(outputs.get(i).getExprId(), quoteIdentifier(columnNames.get(i)));
        }
        // When this relation is wrapped as a subquery by its parent, its SELECT list is
        // the subquery output: the generator columns must be part of it, otherwise the
        // parent's reference to a generated column fails to resolve ("Unknown column
        // ... in table list"). An empty SELECT list means "*" and already covers them.
        if (!relation.getSelects().isEmpty()) {
            List<Pair<ExprId, String>> selects = new ArrayList<>(relation.getSelects());
            for (int i = 0; i < outputs.size(); i++) {
                selects.add(Pair.of(outputs.get(i).getExprId(), quoteIdentifier(columnNames.get(i))));
            }
            relation.setSelects(selects);
        }
        if (!generate.getConjuncts().isEmpty()) {
            String conjuncts = generate.getConjuncts().stream()
                    .map(expr -> exprSqlBuilder.print(expr, relation))
                    .collect(Collectors.joining(" AND "));
            relation.setWhere(relation.getWhere().isEmpty()
                    ? conjuncts
                    : relation.getWhere() + " AND " + conjuncts);
        }
        return relation;
    }

    // ==================== CTE (anchor / producer / consumer) ====================

    /**
     * PhysicalCTEAnchor: two children - child(0) is the PhysicalCTEProducer (the CTE
     * body, processed first: it registers the WITH definition and the alias consumers
     * reference) and child(1) is the main query. The WITH entry itself is attached to
     * the root relation in toSQL(), not here (see cteDefinitions).
     */
    @Override
    public SQLRelation visitPhysicalCTEAnchor(
            PhysicalCTEAnchor<? extends Plan, ? extends Plan> anchor, Void context) {
        process(anchor.child(0));
        return process(anchor.child(1));
    }

    /**
     * PhysicalCTEProducer: processes the CTE body and registers it (plus a fresh alias
     * and the WITH entry) under the CTEId, so every PhysicalCTEConsumer references the
     * single shared definition instead of inlining a body copy. The body subtree is
     * already pruned to the union of all consumer needs by the live-column analysis.
     */
    @Override
    public SQLRelation visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> producer, Void context) {
        SQLRelation body = process(producer.child(0));
        String alias = SQLRelation.newTableAlias();
        cteBodies.put(producer.getCteId(), body);
        cteAliases.put(producer.getCteId(), alias);
        cteDefinitions.add(alias + " AS (" + body.toSQL() + ")");
        return body;
    }

    /**
     * PhysicalCTEConsumer: emits {@code FROM <cte alias>} - a reference to the WITH
     * definition registered by the producer - and maps the consumer output slots onto
     * the body's registered column references. Each reference gets its own subquery
     * alias (t_N), so two consumers of one CTE can appear side by side (self join)
     * without a duplicate relation name.
     */
    @Override
    public SQLRelation visitPhysicalCTEConsumer(PhysicalCTEConsumer consumer, Void context) {
        SQLRelation body = cteBodies.get(consumer.getCteId());
        String alias = cteAliases.get(consumer.getCteId());
        if (body == null || alias == null) {
            throw new UnsupportedOperationException(
                    "SPMPlan2SQLBuilder cannot find CTE body for " + consumer.getCteId());
        }
        SQLRelation relation = new SQLRelation();
        relation.setFrom(alias);
        relation.newAlias();
        for (Slot slot : consumer.getOutput()) {
            Slot producerSlot = consumer.getProducerSlot(slot);
            String col = body.getColumnNames().get(producerSlot.getExprId());
            relation.registerRef(slot.getExprId(),
                    col != null ? col : quoteIdentifier(producerSlot.getName()));
        }
        return relation;
    }

    // ==================== Recursive CTE ====================

    /**
     * PhysicalRecursiveUnion: the recursive CTE node. Its physical tree is
     *
     *     PhysicalRecursiveUnion (cteName)
     *     ├── child(0): [Distribute] PhysicalRecursiveUnionAnchor → anchor body
     *     └── child(1): [Distribute] PhysicalRecursiveUnionProducer → recursive body
     *         (the recursive body's base is a PhysicalWorkTableReference to the CTE itself)
     *
     * A recursive CTE cannot be inlined (its recursive member references the CTE by
     * name), so it is decompiled as a self-contained subquery with a WITH RECURSIVE
     * prefix: (WITH RECURSIVE cte(cols) AS (anchor UNION [ALL] recursive)
     * SELECT cols FROM cte) t_N. The caller (e.g. the outer filter / project) wraps it
     * like any other relation, so the rest of the decompiler is unchanged.
     */
    @Override
    public SQLRelation visitPhysicalRecursiveUnion(
            PhysicalRecursiveUnion<? extends Plan, ? extends Plan> recCte, Void context) {
        String cteName = recCte.getCteName();
        // anchor first (child 0): its output column names become the CTE's visible
        // column names, needed to name the recursive work-table self reference
        SQLRelation anchor = process(recCte.child(0));
        List<String> anchorColNames = new ArrayList<>();
        for (Slot slot : recCte.getRegularChildOutput(0)) {
            anchorColNames.add(slot.getName());
        }
        // The physical anchor / recursive subtrees may carry optimizer-added
        // intermediate columns that are not part of the CTE (e.g. a GROUP BY key of a
        // "SELECT 3, 4 FROM t GROUP BY k1" recursive member). Doris requires the anchor
        // and the recursive member to expose the SAME column arity, so each branch SQL
        // is aligned onto exactly the CTE columns (getRegularChildOutput(i), CTE order).
        alignCteBranchSelects(anchor, recCte.getRegularChildOutput(0));
        recursiveCteColumns.put(cteName, anchorColNames);
        // recursive member (child 1)
        SQLRelation recursive = process(recCte.child(1));
        alignCteBranchSelects(recursive, recCte.getRegularChildOutput(1));
        recursiveCteColumns.remove(cteName);

        // body = (anchor) UNION [ALL] (recursive)
        String op = recCte.isUnionAll() ? "UNION ALL" : "UNION";
        String bodySql = "(" + anchor.toSQL() + ") " + op + " (" + recursive.toSQL() + ")";

        // outer reference: WITH RECURSIVE cte(cols) AS (body) SELECT cols FROM cte
        String colList = anchorColNames.stream()
                .map(SPMPlan2SQLBuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));
        SQLRelation relation = new SQLRelation();
        relation.setCte(Collections.singletonList(
                "RECURSIVE " + quoteIdentifier(cteName) + "(" + colList + ") AS (" + bodySql + ")"));
        relation.setFrom(quoteIdentifier(cteName));
        List<Pair<ExprId, String>> selects = new ArrayList<>();
        List<Slot> cteOutput = recCte.getOutput();
        for (int i = 0; i < cteOutput.size(); i++) {
            String col = i < anchorColNames.size() ? anchorColNames.get(i) : cteOutput.get(i).getName();
            String ref = quoteIdentifier(col);
            relation.registerRef(cteOutput.get(i).getExprId(), ref);
            selects.add(Pair.of(cteOutput.get(i).getExprId(), ref));
        }
        relation.setSelects(selects);
        relation.newAlias();
        return relation;
    }

    /** PhysicalRecursiveUnionAnchor: a sentinel on the anchor branch; passes through. */
    @Override
    public SQLRelation visitPhysicalRecursiveUnionAnchor(
            PhysicalRecursiveUnionAnchor<? extends Plan> anchor, Void context) {
        return process(anchor.child());
    }

    /**
     * Aligns one recursive-CTE branch (anchor or recursive member) onto the CTE's
     * exposed columns. The decompiled branch subtree can carry optimizer-added
     * intermediate columns that do not belong to the CTE - e.g. a "SELECT 3, 4 FROM t
     * GROUP BY k1" recursive member keeps its GROUP BY key k1 in the physical plan, so
     * the decompiled branch emits three columns while the anchor emits two. AnalyzeCTE
     * rejects a recursive CTE whose anchor and member have different output arities
     * ("anchor and recursive child's output size must be same"), so the branch SQL is
     * re-projected onto exactly the CTE columns (cteCols, in CTE order). When a
     * CTE column cannot be resolved on the branch the branch is left untouched (the
     * arity is already consistent in that case).
     */
    private void alignCteBranchSelects(SQLRelation branch, List<SlotReference> cteCols) {
        List<Pair<ExprId, String>> selects = branch.getSelects();
        if (cteCols == null || cteCols.isEmpty() || selects == null || selects.isEmpty()
                || selects.size() <= cteCols.size()) {
            return;
        }
        List<Pair<ExprId, String>> aligned = new ArrayList<>();
        for (SlotReference cteCol : cteCols) {
            Pair<ExprId, String> match = null;
            for (Pair<ExprId, String> select : selects) {
                if (select.key().equals(cteCol.getExprId())) {
                    match = select;
                    break;
                }
            }
            if (match == null) {
                // CTE column not emitted under its own exprId: match the exported alias
                String name = cteCol.getName();
                for (Pair<ExprId, String> select : selects) {
                    String value = select.value();
                    int asIdx = value.toLowerCase().lastIndexOf(" as ");
                    String alias = asIdx >= 0 ? value.substring(asIdx + 4).trim() : value;
                    if (alias.equals(name)) {
                        match = select;
                        break;
                    }
                }
            }
            if (match == null) {
                return; // cannot resolve every CTE column: leave the branch untouched
            }
            aligned.add(match);
        }
        if (aligned.size() == cteCols.size()) {
            branch.setSelects(aligned);
        }
    }

    /** PhysicalRecursiveUnionProducer: a sentinel on the recursive branch; passes through. */
    @Override
    public SQLRelation visitPhysicalRecursiveUnionProducer(
            PhysicalRecursiveUnionProducer<? extends Plan> producer, Void context) {
        return process(producer.child());
    }

    /**
     * PhysicalWorkTableReference: the recursive member's self reference to the CTE
     * (FROM cte inside the recursive SELECT). Its output columns map onto the
     * CTE's visible column names (registered from the anchor branch); a slot without a
     * mapped CTE name falls back to its own name so a parent operator can never
     * reference an unresolved work-table column.
     */
    @Override
    public SQLRelation visitPhysicalWorkTableReference(PhysicalWorkTableReference reference, Void context) {
        SQLRelation relation = new SQLRelation();
        String tableName = reference.getTableName();
        relation.setFrom(quoteIdentifier(tableName));
        List<String> cols = recursiveCteColumns.get(reference.getNameParts().isEmpty()
                ? tableName : reference.getNameParts().get(reference.getNameParts().size() - 1));
        List<Slot> outputs = reference.getOutput();
        for (int i = 0; i < outputs.size(); i++) {
            String col = cols != null && i < cols.size() ? cols.get(i) : outputs.get(i).getName();
            relation.registerRef(outputs.get(i).getExprId(), quoteIdentifier(col));
        }
        return relation;
    }

    /**
     * PhysicalOneRowRelation: a relation that produces one row from a projection list
     * with no FROM (e.g. the anchor "SELECT 1" of a recursive CTE). Rendered as a bare
     * SELECT list.
     */
    @Override
    public SQLRelation visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRow, Void context) {
        SQLRelation relation = new SQLRelation();
        List<Pair<ExprId, String>> selects = new ArrayList<>();
        for (NamedExpression project : oneRow.getProjects()) {
            String sql = exprSqlBuilder.print(project, relation);
            String ref = sql;
            // A FROM-less projection that is the query's result (SELECT 1 AS a) never
            // reaches a projection layer that could relabel the output: the alias must
            // be emitted HERE, otherwise the frozen SQL is "SELECT _spm_const_var(1)"
            // and the replay exposes an expression-derived header instead of a. Only
            // explicit aliases are emitted; nameFromChild names are the parser's
            // fallback from the expression text and are not identifier-safe.
            if (project instanceof Alias && !((Alias) project).isNameFromChild()) {
                ref = quoteIdentifier(((Alias) project).getName());
                sql = sql + " AS " + ref;
            }
            relation.registerRef(project.getExprId(), ref);
            selects.add(Pair.of(project.getExprId(), sql));
        }
        relation.setSelects(selects);
        return relation;
    }

    // ==================== Empty relation ====================

    /**
     * PhysicalEmptyRelation: a relation that is statically known to return no rows.
     * Rendered as SELECT [projects] FROM (SELECT 1) WHERE FALSE so parent operators
     * can still reference its output columns.
     */
    @Override
    public SQLRelation visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, Void context) {
        SQLRelation relation = new SQLRelation();
        List<Pair<ExprId, String>> selects = Lists.newArrayList();
        for (NamedExpression project : emptyRelation.getProjects()) {
            ExprId id = project.getExprId();
            String name = generatedColumnName(id);
            selects.add(Pair.of(id, "1 AS " + name));
            relation.registerRef(id, name);
        }
        relation.setSelects(selects);
        relation.setFrom("(SELECT 1)");
        relation.setWhere("FALSE");
        return relation;
    }

    // ==================== Partition top-N (window partition sort) ====================

    /**
     * PhysicalPartitionTopN: implements the partition-sort of a window function
     * (e.g. row_number() OVER (PARTITION BY ... ORDER BY ...)). The PhysicalWindow above
     * renders the actual OVER clause, so this node is a pass-through.
     */
    @Override
    public SQLRelation visitPhysicalPartitionTopN(
            PhysicalPartitionTopN<? extends Plan> partitionTopN, Void context) {
        return process(partitionTopN.child(0));
    }

    // ==================== Filter (wrap + WHERE) ====================

    /**
     * PhysicalFilter: wraps the child relation and puts the predicate into WHERE.
     */
    @Override
    public SQLRelation visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, Void context) {
        SQLRelation child = process(filter.child(0));
        SQLRelation relation = new SQLRelation();
        relation.setFrom(child.toRelationSQL());
        relation.setWhere(exprSqlBuilder.print(filter.getPredicate(), child));
        relation.getColumnNames().putAll(child.getColumnNames());
        relation.newAlias();
        return relation;
    }

    // ==================== Join (wrap + ON + distribution HINT) ====================

    /**
     * PhysicalHashJoin and PhysicalNestedLoopJoin share the same handling logic.
     */
    @Override
    public SQLRelation visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, Void context) {
        return visitPhysicalJoin(hashJoin, context);
    }

    @Override
    public SQLRelation visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin, Void context) {
        return visitPhysicalJoin(nestedLoopJoin, context);
    }

    /**
     * Common Join handling: recursively process left/right, assemble FROM (including
     * the distribution HINT), build the ON condition, merge column names and wrap.
     *
     * Column-name collision handling (design doc 6.2.2): when the two sides share the
     * same relation alias (e.g. a self join where both sides inline as "t1") both are
     * forced to wrap with a new alias; when the two sides register a column under the
     * same SQL name, the join relation qualifies every column with "alias." so outer
     * references stay unambiguous.
     */
    private SQLRelation visitPhysicalJoin(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        JoinType joinType = join.getJoinType();
        boolean isMarkJoin = join.isMarkJoin();
        // ===== ASOF / MARK / NULL-AWARE join handling =====
        // A MARK join is any SEMI/ANTI join that carries a mark slot (the output of an
        // IN / NOT IN / EXISTS / NOT EXISTS predicate). The MARK / MARK_CONDITION /
        // MARK_SLOT keywords are only the SQL surface that passes the mark parameters
        // back into the very same SEMI/ANTI join, so every MARK join decompiles
        // natively in one of three shapes (comments2):
        //  1. correlation in ON, no mark key  : SEMI/ANTI MARK JOIN ... MARK_SLOT m ON <conds>
        //  2. only a mark key, no correlation : SEMI/ANTI MARK JOIN ... MARK_CONDITION(<key>) MARK_SLOT m ON true
        //  3. correlation in ON + a mark key  : SEMI/ANTI MARK JOIN ... MARK_CONDITION(<key>) MARK_SLOT m ON <conds>
        // When only the mark key exists the ON clause is emitted as "ON true" so the
        // SEMI/ANTI join stays parseable; the parser keeps hash / other empty and the
        // translator turns the mark-key-only join into the null-aware operator for BE.
        // ASOF joins are LEFT-direction only (ASOF RIGHT has no SQL keyword and the
        // optimizer never generates it). The pure-filter NULL_AWARE_LEFT_ANTI (the
        // WHERE x NOT IN (sub) filter) with residual conjuncts or no key equality keeps
        // the NOT IN rewrite (decompileNullAwareAnti); the clean equi-key shape is the
        // native LEFT NULL_AWARE ANTI JOIN.
        SQLRelation left = process(join.left());
        SQLRelation right = process(join.right());
        boolean nativeMarkJoin = isMarkJoin && (joinType == JoinType.LEFT_SEMI_JOIN
                || joinType == JoinType.RIGHT_SEMI_JOIN
                || joinType == JoinType.LEFT_ANTI_JOIN
                || joinType == JoinType.RIGHT_ANTI_JOIN);
        // A MARK join may also surface as a CROSS join (PhysicalNestedLoopJoin) when it
        // carries NO hash / other / mark conjunct at all: the folded output of an
        // uncorrelated EXISTS / NOT EXISTS boolean (LogicalJoin CROSS_JOIN + mark slot,
        // right side reduced to "SELECT 1 FROM t2 LIMIT 1"). It decompiles as
        // "CROSS MARK JOIN <right> MARK_SLOT <name>" with no ON clause (a CROSS join
        // cannot carry one), and the parser rebuilds the CROSS_JOIN node + mark slot.
        boolean crossMarkJoin = isMarkJoin && joinType == JoinType.CROSS_JOIN;
        if (crossMarkJoin && (!join.getHashJoinConjuncts().isEmpty()
                || !join.getOtherJoinConjuncts().isEmpty()
                || !join.getMarkJoinConjuncts().isEmpty())) {
            throw new UnsupportedOperationException(
                    "SPM decompile CROSS MARK join: unexpected join conjuncts");
        }
        if (isMarkJoin && !(nativeMarkJoin || crossMarkJoin)) {
            // A NULL_AWARE-typed mark join is never produced by the optimizer (SELECT-list
            // IN / NOT IN marks keep the LEFT/RIGHT SEMI/ANTI types); refuse it loudly
            // instead of freezing an unrepresentable shape.
            throw new UnsupportedOperationException(
                    "SPM decompile mark join: unsupported join type " + joinType);
        }
        if (joinType == JoinType.NULL_AWARE_LEFT_ANTI_JOIN
                && (!join.getOtherJoinConjuncts().isEmpty()
                || join.getHashJoinConjuncts().isEmpty())) {
            return decompileNullAwareAnti(join, left, right);
        }

        SQLRelation joinRelation = new SQLRelation();
        String hints = getJoinDistributionHints(join);
        String hintStr = hints.isEmpty() ? "" : "[" + hints + "]";

        // ===== column-name collision handling =====
        // same relation alias on both sides (e.g. self join) -> force subquery wrap
        if (StringUtils.equalsIgnoreCase(left.getRelationAlias(), right.getRelationAlias())) {
            left.newAlias();
            right.newAlias();
        }
        // SEMI / ANTI joins only project the preserved side (Doris JoinType semantics:
        // LEFT SEMI/ANTI outputs left columns only, RIGHT SEMI/ANTI right columns only).
        // The DROPPED side's columns are still registered during ON rendering (the ON
        // condition references both sides), then removed afterwards so upper projections
        // never leak them ("Unknown column 'L_SHIPDATE' in table list" over a LEFT SEMI
        // JOIN). Registering only the preserved side up front would leave the ON-side
        // references of the dropped side unresolved (bare-name fallback -> "ambiguous").
        boolean leftProjected = true;
        boolean rightProjected = true;
        switch (joinType) {
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                rightProjected = false;
                break;
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                leftProjected = false;
                break;
            default:
                break;
        }
        Set<ExprId> droppedSideIds = new HashSet<>();
        if (!leftProjected) {
            droppedSideIds.addAll(left.getColumnNames().keySet());
        }
        if (!rightProjected) {
            droppedSideIds.addAll(right.getColumnNames().keySet());
        }

        boolean columnConflicts = intersectsIgnoreCase(
                left.getColumnNames().values(), right.getColumnNames().values());
        if (columnConflicts) {
            // qualify every column with its side's alias (needed by the ON clause AND by
            // the explicit projection below)
            for (Map.Entry<ExprId, String> entry : left.getColumnNames().entrySet()) {
                joinRelation.registerRef(entry.getKey(),
                        left.getRelationAlias() + "." + entry.getValue());
            }
            for (Map.Entry<ExprId, String> entry : right.getColumnNames().entrySet()) {
                joinRelation.registerRef(entry.getKey(),
                        right.getRelationAlias() + "." + entry.getValue());
            }
        } else {
            joinRelation.getColumnNames().putAll(left.getColumnNames());
            joinRelation.getColumnNames().putAll(right.getColumnNames());
        }

        // ON condition: hashJoinConjuncts normally join the residual conjuncts. For an
        // ASOF join the residual conjuncts are the MATCH_CONDITION expression, and the
        // ON clause keeps only the hash conjuncts (ASOF SQL syntax places
        // MATCH_CONDITION before ON). Rendered AFTER the column-name qualification so
        // conflicting columns print qualified (t1.a = t2.a) instead of leaking an
        // ambiguous plain name.
        List<Expression> onConjuncts = Lists.newArrayList(join.getHashJoinConjuncts());
        boolean asofJoin = join.getJoinType().isAsofJoin();
        if (asofJoin) {
            if (join.getOtherJoinConjuncts().size() > 1) {
                throw new UnsupportedOperationException(
                        "SPM decompile ASOF join: multiple MATCH_CONDITION expressions");
            }
        } else {
            onConjuncts.addAll(join.getOtherJoinConjuncts());
        }
        String onSql = "";
        if (!onConjuncts.isEmpty() && join.getJoinType() != JoinType.CROSS_JOIN) {
            onSql = " ON " + onConjuncts.stream()
                    .map(e -> exprSqlBuilder.print(e, joinRelation))
                    .collect(Collectors.joining(" AND "));
        }
        String matchSql = "";
        if (asofJoin && !join.getOtherJoinConjuncts().isEmpty()) {
            matchSql = " MATCH_CONDITION(" + join.getOtherJoinConjuncts().stream()
                    .map(e -> exprSqlBuilder.print(e, joinRelation))
                    .collect(Collectors.joining(" AND ")) + ")";
        }

        // ===== MARK join output column + mark key =====
        // MARK_CONDITION carries the (three-valued) IN / NOT IN key when the mark join
        // has one; MARK_SLOT names the exported mark column (an upper filter / projection
        // references it through the registered alias, so the same name is reused). Both
        // are rendered before the dropped side's columns are removed (the mark key may
        // reference columns of both sides).
        String markSpec = "";
        if (nativeMarkJoin || crossMarkJoin) {
            MarkJoinSlotReference markSlot = join.getMarkJoinSlotReference().orElse(null);
            if (markSlot == null) {
                throw new UnsupportedOperationException("SPM decompile mark join: missing mark slot");
            }
            String markName = generatedColumnName(markSlot.getExprId());
            if (!join.getMarkJoinConjuncts().isEmpty()) {
                String markCond = join.getMarkJoinConjuncts().stream()
                        .map(e -> exprSqlBuilder.print(e, joinRelation))
                        .collect(Collectors.joining(" AND "));
                markSpec = " MARK_CONDITION(" + markCond + ")";
            }
            markSpec = markSpec + " MARK_SLOT " + markName;
            joinRelation.registerRef(markSlot.getExprId(), markName);
        }

        // drop the non-projected side's columns now that ON is rendered
        if (!droppedSideIds.isEmpty()) {
            joinRelation.getColumnNames().keySet().removeAll(droppedSideIds);
        }

        String joinTypeWord = joinTypeToSql(joinType);
        if (nativeMarkJoin) {
            // LEFT SEMI JOIN -> LEFT SEMI MARK JOIN (and the RIGHT / ANTI variants)
            joinTypeWord = joinTypeWord.replace(" SEMI JOIN", " SEMI MARK JOIN")
                    .replace(" ANTI JOIN", " ANTI MARK JOIN");
        } else if (crossMarkJoin) {
            // CROSS JOIN -> CROSS MARK JOIN (a mark join with no conjunct at all); the
            // parser rebuilds it as a CROSS_JOIN node carrying the mark slot
            joinTypeWord = "CROSS MARK JOIN";
        }
        // A MARK join with no correlation (mark key only, shape 2 above) still needs an
        // ON clause to stay parseable: emit "ON true". The parser keeps hash / other
        // empty for a literal-true ON (the optimizer drops it), so the node replays as
        // a mark-key-only SEMI/ANTI join and the translator derives the null-aware
        // operator for BE.
        String effectiveOn = onSql;
        if (nativeMarkJoin && effectiveOn.isEmpty() && matchSql.isEmpty()) {
            effectiveOn = " ON true";
        }
        joinRelation.setFrom(left.toRelationSQL() + " " + joinTypeWord + hintStr + " "
                + right.toRelationSQL() + markSpec + matchSql + effectiveOn);
        if (columnConflicts) {
            // Conflicting column names: the qualified references (t_a.X) above are only
            // valid INSIDE the join scope (ON clause). Once this join is wrapped into a
            // (SELECT ... FROM <join>) t_N subquery, upper layers can only see the columns
            // the subquery SELECT emits, so the join must export an EXPLICIT projection:
            // each output column is referenced by its qualified name (valid here) and
            // re-exported under a unique plain alias (c_<seq>) that upper references
            // resolve to (otherwise a later wrap would leak qualified names such as
            // "t_a.X" that are no longer in scope -> "Unknown column in t_N", or leave
            // duplicate bare names -> "ambiguous").
            List<Pair<ExprId, String>> explicitProjection = new ArrayList<>();
            for (Map.Entry<ExprId, String> entry : joinRelation.getColumnNames().entrySet()) {
                if (isSystemColumnName(entry.getValue())) {
                    continue;
                }
                String alias = generatedColumnName(entry.getKey());
                explicitProjection.add(Pair.of(entry.getKey(), entry.getValue() + " AS " + alias));
                joinRelation.registerRef(entry.getKey(), alias);
            }
            // prune the exported columns to the join's live output columns
            filterLiveSelects(join, explicitProjection);
            joinRelation.setSelects(explicitProjection);
        }
        joinRelation.newAlias();
        return joinRelation;
    }

    /**
     * Decompiles a plain (non-mark) NULL-AWARE LEFT ANTI join: a pure NOT IN filter
     * join with no mark slot. It appears for a WHERE x NOT IN (sub) predicate that is
     * not correlated; the anti join itself drops every left row whose NOT IN result is
     * not TRUE, i.e. it implements the three-valued semantics of NOT IN.
     *
     * A standard LEFT ANTI JOIN cannot express the NULL-aware behaviour, so the filter
     * is re-expressed as the original three-valued predicate on the preserved side:
     *
     *   plan:  PhysicalHashJoin NULL_AWARE_LEFT_ANTI, no mark slot, no mark conjuncts
     *          hashCondition=[(t1.k1 = t2.k2)], otherCondition=[],
     *          left = t1, right = t2
     *          source query: select * from t1 where k1 not in (select k2 from t2)
     *   sql:   select t1.k1, t1.k2, t1.k3 from t1
     *          where (t1.k1) not in (select t2.k2 from t2)
     *
     * The WHERE clause keeps the three-valued NOT IN semantics: a row is kept only when
     * the predicate is TRUE. A row whose probe is NULL, or whose build side contains a
     * NULL value without a match, evaluates to NULL and is dropped, exactly like the
     * original WHERE filter (for example t2.k2 holds NULL and covers t1.k1 - the row
     * disappears instead of being wrongly emitted as with a plain anti join).
     *
     * A correlated NOT IN filter is NOT rewritten here: Doris folds its NULL logic into
     * a plain LEFT_ANTI join whose residual predicate already carries the NULL tests
     * (for example ((k1 = k2) OR k1 IS NULL) OR k2 IS NULL), so the regular join
     * decompiler handles that shape without this method.
     *
     * The equi key (probe = build) is read from the hash conjuncts; any other conjunct
     * becomes the WHERE clause of the NOT IN subquery.
     *
     * Fallback-only entry (see visitPhysicalJoin): the clean equi-key shape (no residual
     * conjuncts, one hash key equality) now decompiles to the native LEFT NULL_AWARE
     * ANTI JOIN keyword, so this method is only reached when the build side carries
     * residual conjuncts or the hash conjuncts hold no key equality - shapes whose
     * conjuncts must stay inside the NOT IN subquery to preserve the three-valued
     * semantics.
     */
    private SQLRelation decompileNullAwareAnti(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join,
            SQLRelation left, SQLRelation right) {
        List<Expression> hash = join.getHashJoinConjuncts();
        List<Expression> other = join.getOtherJoinConjuncts();
        if (!join.getMarkJoinConjuncts().isEmpty()) {
            throw new UnsupportedOperationException(
                    "SPM decompile NULL_AWARE anti: unexpected mark conjuncts");
        }

        // locate the single IN key equality (probe on the preserved left side, build on
        // the right side); every other conjunct becomes a subquery filter
        Expression key = null;
        List<Expression> subWhere = Lists.newArrayList();
        for (Expression e : hash) {
            if (key == null && isCrossSideEquality(e, left)) {
                key = e;
            } else {
                subWhere.add(e);
            }
        }
        if (key == null) {
            for (Expression e : other) {
                if (key == null && isCrossSideEquality(e, left)) {
                    key = e;
                } else {
                    subWhere.add(e);
                }
            }
        }
        if (key == null) {
            throw new UnsupportedOperationException(
                    "SPM decompile NULL_AWARE anti: no IN key equality found");
        }

        // probe belongs to the left (preserved) side, build to the right subquery side
        Expression mk0 = key.child(0);
        Expression mk1 = key.child(1);
        Expression probe;
        Expression build;
        if (exprOnlyUsesColumnsOf(mk0, left) && !exprOnlyUsesColumnsOf(mk1, left)) {
            probe = mk0;
            build = mk1;
        } else if (exprOnlyUsesColumnsOf(mk1, left) && !exprOnlyUsesColumnsOf(mk0, left)) {
            probe = mk1;
            build = mk0;
        } else {
            throw new UnsupportedOperationException(
                    "SPM decompile NULL_AWARE anti: cannot split the IN key sides");
        }

        // every column must be resolvable: preserved columns in the outer scope of the
        // subquery, subquery columns inside it
        Set<ExprId> knownIds = new HashSet<>(left.getColumnNames().keySet());
        knownIds.addAll(right.getColumnNames().keySet());
        for (Expression condition : subWhere) {
            Set<ExprId> used = new HashSet<>();
            collectAllSlotIds(condition, used);
            for (ExprId usedId : used) {
                if (!knownIds.contains(usedId)) {
                    throw new UnsupportedOperationException(
                            "SPM decompile NULL_AWARE anti: condition references an unknown column");
                }
            }
        }
        Set<ExprId> buildIds = new HashSet<>();
        collectAllSlotIds(build, buildIds);
        if (!right.getColumnNames().keySet().containsAll(buildIds)) {
            throw new UnsupportedOperationException(
                    "SPM decompile NULL_AWARE anti: build value not on the right side");
        }

        // inside the NOT IN subquery a bare name binds to the right side first, so a
        // left column sharing a name with a right column must be qualified by the left
        // relation alias to stay resolvable in the outer scope
        boolean nameConflict = intersectsIgnoreCase(
                left.getColumnNames().values(), right.getColumnNames().values());
        String leftQualifier = nameConflict ? left.getRelationAlias() : null;
        if (nameConflict && (leftQualifier == null || leftQualifier.isEmpty())) {
            throw new UnsupportedOperationException(
                    "SPM decompile NULL_AWARE anti: cannot qualify the left side columns");
        }

        // mapping used only to print the probe, the build value and the WHERE clause
        SQLRelation printRelation = new SQLRelation();
        for (Map.Entry<ExprId, String> entry : left.getColumnNames().entrySet()) {
            String value = entry.getValue();
            String ref = value;
            if (leftQualifier != null && ref.indexOf('.') < 0) {
                ref = leftQualifier + "." + ref;
            }
            printRelation.registerRef(entry.getKey(), ref);
        }
        for (Map.Entry<ExprId, String> entry : right.getColumnNames().entrySet()) {
            String value = entry.getValue();
            if (value == null || value.isEmpty()
                    || value.indexOf(' ') >= 0 || value.indexOf('(') >= 0) {
                throw new UnsupportedOperationException(
                        "SPM decompile NULL_AWARE anti: non-trivial right column " + value);
            }
            printRelation.registerRef(entry.getKey(), value);
        }

        String buildSql = exprSqlBuilder.print(build, printRelation);
        String whereSql = subWhere.isEmpty() ? "" : " WHERE " + subWhere.stream()
                .map(e -> exprSqlBuilder.print(e, printRelation))
                .collect(Collectors.joining(" AND "));
        String notInPredicate = "(" + exprSqlBuilder.print(probe, printRelation)
                + ") NOT IN (SELECT " + buildSql + " FROM " + right.toRelationSQL() + whereSql + ")";

        // the anti join keeps the left rows whose NOT IN result is TRUE; the predicate
        // becomes the WHERE clause of this relation, and the left columns are re-exported
        SQLRelation joinRelation = new SQLRelation();
        joinRelation.setFrom(left.toRelationSQL());
        List<Pair<ExprId, String>> selects = new ArrayList<>();
        appendLivePreservedColumns(join, left, joinRelation, selects);
        joinRelation.setSelects(selects);
        joinRelation.setWhere(notInPredicate);
        joinRelation.newAlias();
        return joinRelation;
    }

    /** Re-exports the preserved-side columns of a mark / NULL-AWARE anti / IN-subquery
     * rewrite onto the new join relation, pruned to the join's live output columns (a
     * dead preserved column that no upper layer references is dropped from the SELECT
     * list; the correlated subquery still resolves its own conditions inside the FROM
     * scope). */
    private void appendLivePreservedColumns(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join,
            SQLRelation preserved, SQLRelation joinRelation, List<Pair<ExprId, String>> selects) {
        Set<ExprId> need = neededOutputs.get(join);
        for (Map.Entry<ExprId, String> entry : preserved.getColumnNames().entrySet()) {
            String value = entry.getValue();
            if (isSystemColumnName(value)) {
                continue;
            }
            if (need != null && !need.contains(entry.getKey())) {
                continue;
            }
            selects.add(Pair.of(entry.getKey(), value));
            joinRelation.registerRef(entry.getKey(), value);
        }
    }

    /**
     * Case-insensitive intersection test over column references: Doris binds column
     * identifiers case-insensitively, so a and A are the SAME name and two join sides
     * exposing them must take the qualification / renaming path. A case-sensitive check
     * (Collections.disjoint) would skip it and freeze an unqualified predicate such as
     * ON (a = A), which the re-analysis then rejects as ambiguous. Names are compared
     * with backtick quoting stripped and lower-cased (Locale.ROOT).
     */
    private static boolean intersectsIgnoreCase(Collection<String> left, Collection<String> right) {
        Set<String> normalized = new HashSet<>();
        for (String name : left) {
            normalized.add(normalizeIdentifier(name));
        }
        for (String name : right) {
            if (normalized.contains(normalizeIdentifier(name))) {
                return true;
            }
        }
        return false;
    }

    /** Normalizes a column reference for case-insensitive comparison (strips quoting). */
    private static String normalizeIdentifier(String name) {
        if (name == null) {
            return "";
        }
        String normalized = name;
        if (normalized.length() >= 2 && normalized.charAt(0) == '`'
                && normalized.charAt(normalized.length() - 1) == '`') {
            normalized = normalized.substring(1, normalized.length() - 1).replace("``", "`");
        }
        return normalized.toLowerCase(Locale.ROOT);
    }

    /** True when expr is an equality with exactly one side on the left relation. */
    private static boolean isCrossSideEquality(Expression expr, SQLRelation left) {
        if (!(expr instanceof EqualTo)) {
            return false;
        }
        boolean c0Left = exprOnlyUsesColumnsOf(expr.child(0), left);
        boolean c1Left = exprOnlyUsesColumnsOf(expr.child(1), left);
        return c0Left != c1Left;
    }

    /** True when every slot of expr belongs to the given relation's output columns. */
    private static boolean exprOnlyUsesColumnsOf(Expression expr, SQLRelation relation) {
        Set<ExprId> used = new HashSet<>();
        collectAllSlotIds(expr, used);
        if (used.isEmpty()) {
            return false;
        }
        return relation.getColumnNames().keySet().containsAll(used);
    }

    /** Collects every slot exprId used by an expression. */
    private static void collectAllSlotIds(Expression expr, Set<ExprId> out) {
        if (expr instanceof SlotReference) {
            out.add(((SlotReference) expr).getExprId());
        }
        for (Expression child : expr.children()) {
            collectAllSlotIds(child, out);
        }
    }

    // ==================== Aggregate (local merge / global wrap) ====================

    /**
     * PhysicalHashAggregate:
     *
     * - LOCAL / DISTINCT_LOCAL (partial): a distributed-execution detail with no SQL
     *   equivalent. Its output columns are recorded (local-output-column -> the
     *   aggregated input expression, localAggParams) so the enclosing GLOBAL aggregate
     *   decompiles to a single logical aggregate, then the child is passed through
     *   without nesting and without leaking any partial_* intermediate column.
     * - GLOBAL / DISTINCT_GLOBAL (final): wrap mode - GROUP BY plus the aggregate
     *   function list, each partial reference rewritten back to its input expression.
     */
    @Override
    public SQLRelation visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> agg, Void context) {
        SQLRelation childRelation = process(agg.child(0));
        AggPhase phase = agg.getAggPhase();

        if (phase.isLocal() || isIntermediateAggStage(agg)) {
            // Eliminate this execution-only stage: record every partial buffer output
            // (buffer-slot -> the aggregated input expression, recursively resolved
            // through earlier recorded stages down to the data expression) so the outer
            // final aggregate rewrites its partial references back to the data columns,
            // then pass the child through WITHOUT nesting - an intermediate wrap would
            // otherwise cut off the outer aggregate's argument scoping ("Unknown column
            // 'cs_ext_ship_cost' in table list" for count(DISTINCT)+sum plans).
            recordLocalAggStage(agg);
            return childRelation;
        }

        // ===== wrap mode: global (final) aggregate =====
        SQLRelation aggRelation;
        if (!childRelation.getGroupings().isEmpty()) {
            // GROUPING SETS: PhysicalRepeat has already built the FROM clause and the
            // GROUPING SETS(...) expression; the global aggregate reuses the child and
            // turns it into GROUP BY GROUPING SETS(...) (no extra nesting).
            aggRelation = childRelation;
            aggRelation.setGroupBy(aggRelation.getGroupings());
        } else {
            aggRelation = new SQLRelation();
            aggRelation.setFrom(childRelation.toRelationSQL());
            // GROUP BY columns
            String groupBySql = agg.getGroupByExpressions().stream()
                    .map(g -> exprSqlBuilder.print(g, childRelation))
                    .collect(Collectors.joining(", "));
            aggRelation.setGroupBy(groupBySql);
        }
        // SELECT list = GROUP BY columns + aggregate functions (partial references
        // rewritten to their input expressions, stable aliases for upper references).
        // A DISTINCT_GLOBAL final stage is the merge that completes the user's
        // count(DISTINCT key) (the dedup GROUP BY stages below were eliminated): the
        // count consuming the distinct-dedup buffer renders with DISTINCT, while a
        // plain count/avg riding along the same stage stays plain (see
        // distinctMergeBuffers). When the stage carries no marked buffer (distinct
        // plans built by other shapes), the historical blanket behavior is kept.
        boolean distinctMergeCount = phase == AggPhase.DISTINCT_GLOBAL;
        boolean stageHasDistinctMergeBuffer = distinctMergeCount
                && hasMarkedDistinctMergeArg(agg.getOutputExpressions());
        List<Pair<ExprId, String>> selects = new ArrayList<>();
        for (NamedExpression output : agg.getOutputExpressions()) {
            appendAggSelect(aggRelation, childRelation, output, selects, distinctMergeCount,
                    stageHasDistinctMergeBuffer);
        }
        aggRelation.setSelects(selects);
        // columnNames: expose ONLY the aggregate output columns (group-by keys +
        // aggregate results). The child's full base-table column set must NOT leak:
        // an upper projection pass-through would otherwise reference columns the
        // aggregate subquery does not emit ("Unknown column 'L_DISCOUNT' in table
        // list" over a sum(...) subquery). Group-by keys re-use the child slots so
        // those mappings are kept; aggregate-result mappings were registered by
        // appendAggSelect above.
        aggRelation.getColumnNames().putAll(childRelation.getColumnNames());
        if (aggRelation != childRelation) {
            // not the GROUPING SETS reuse branch (that one keeps the child's columns)
            Set<ExprId> aggOutputIds = new HashSet<>();
            for (NamedExpression output : agg.getOutputExpressions()) {
                aggOutputIds.add(output.getExprId());
            }
            aggRelation.getColumnNames().keySet().removeIf(id -> !aggOutputIds.contains(id));
        }
        aggRelation.newAlias();
        return aggRelation;
    }

    /** Whether the aggregate expression is a partial (product-buffer) local aggregate. */
    private static boolean isPartialAggregate(AggregateExpression aggExpr) {
        return aggExpr.getFunction().getName().startsWith("partial_")
                || aggExpr.getAggregateParam().aggMode.productAggregateBuffer;
    }

    /**
     * Whether this PhysicalHashAggregate stage is an execution-only intermediate stage of
     * a multi-phase aggregate (e.g. count(DISTINCT) mixed with plain aggregates, whose
     * physical plan is LOCAL -> GLOBAL(by the distinct key) -> DISTINCT_LOCAL ->
     * DISTINCT_GLOBAL). Such a stage consumes / produces partial_* buffers only and has
     * no SQL equivalent of its own; the OUTERMOST stage (whose aggregate functions are
     * final user aggregates such as count / sum) emits the single logical aggregate.
     */
    private static boolean isIntermediateAggStage(PhysicalHashAggregate<? extends Plan> agg) {
        // The dedup stage of a single-distinct plan (LOCAL/GLOBAL GROUP BY the distinct
        // key, producing partial_* buffers for the DISTINCT_LOCAL/GLOBAL merge above) is
        // execution-only: the outer final aggregate re-expresses it as count(DISTINCT
        // key). Such a stage emits ONLY slot (group-by key) + partial-buffer columns, so
        // a final user aggregate (plain count/sum/...) makes it a real stage.
        boolean sawAggregate = false;
        for (NamedExpression output : agg.getOutputExpressions()) {
            Expression inner = output instanceof Alias ? ((Alias) output).child() : output;
            if (inner instanceof AggregateExpression) {
                sawAggregate = true;
                if (!isPartialAggregate((AggregateExpression) inner)) {
                    return false;
                }
            } else if (!(inner instanceof SlotReference)) {
                // a non-aggregate expression column: not a pure intermediate stage
                return false;
            }
        }
        // no aggregate at all -> conservative: treat as a real stage
        return sawAggregate;
    }

    /**
     * The input expression aggregated by a local partial aggregate: sum(x) -> x,
     * count(*) -> null (no argument).
     */
    private static Expression extractPartialParam(AggregateExpression aggExpr) {
        List<Expression> fnChildren = aggExpr.getFunction().children();
        if (!fnChildren.isEmpty()) {
            return fnChildren.size() == 1 ? fnChildren.get(0) : null;
        }
        return null;
    }

    /**
     * Whether an aggregate argument is a nested no-argument count - the physical
     * merge-finalize count(*) representation whose local stage is nested as the
     * function's argument, either as a real partial expression (partial_count(*)) or as
     * a buffer slot literally named after it (a scan-level pushAggOp=COUNT has no LOCAL
     * PhysicalHashAggregate node to record, so the enclosing count(*) references the
     * buffer slot whose name is the count-star SQL, e.g. "count()"). A star contributes
     * no data column, so it collapses into a plain count(*) instead of leaking as the
     * invalid count(partial_count(*)) / count(count()).
     */
    private static boolean isNestedNoArgCount(Expression expr) {
        if (expr instanceof SlotReference) {
            String name = ((SlotReference) expr).getName();
            if (name != null) {
                String n = name.trim();
                return n.endsWith("count()") || n.endsWith("count(*)");
            }
            return false;
        }
        if (expr instanceof AggregateExpression) {
            expr = ((AggregateExpression) expr).getFunction();
        }
        if (!(expr instanceof AggregateFunction)) {
            return false;
        }
        AggregateFunction fn = (AggregateFunction) expr;
        String name = fn.getName();
        if (name.startsWith("partial_")) {
            name = name.substring("partial_".length());
        }
        if (!"count".equalsIgnoreCase(name)) {
            return false;
        }
        List<Expression> children = fn.children();
        // count(*) has no children; count(constant) carries a constant that equals a
        // star (count(1) counts rows) and never needs a real column argument either
        return children.isEmpty()
                || (children.size() == 1 && children.get(0).isConstant());
    }

    /** Whether any aggregate output of a DISTINCT_GLOBAL stage consumes a recorded
     * distinct-dedup buffer (see distinctMergeBuffers / isDistinctMergeArg). Applies to
     * EVERY supported aggregate (sum / avg / ...), not only count: SplitAggMultiPhase
     * clears isDistinct on the final DISTINCT_GLOBAL function for all of them. */
    private boolean hasMarkedDistinctMergeArg(List<NamedExpression> outputs) {
        for (NamedExpression output : outputs) {
            Expression inner = output instanceof Alias ? ((Alias) output).child() : output;
            if (!(inner instanceof AggregateExpression)) {
                continue;
            }
            AggregateExpression aggExpr = (AggregateExpression) inner;
            List<Expression> args = aggExpr.getFunction().children().isEmpty()
                    ? new ArrayList<>(aggExpr.children()) : aggExpr.getFunction().children();
            List<Expression> bufferArgs = new ArrayList<>(aggExpr.children());
            if (isDistinctMergeArg(bufferArgs) || isDistinctMergeArg(args)) {
                return true;
            }
        }
        return false;
    }

    /** Whether one of the count arguments consumes the distinct-dedup buffer recorded
     * by recordLocalAggStage - either as the buffer slot itself or nested one level
     * below a partial_* merge function. */
    private boolean isDistinctMergeArg(List<Expression> args) {
        for (Expression arg : args) {
            if (arg instanceof SlotReference
                    && distinctMergeBuffers.contains(((SlotReference) arg).getExprId())) {
                return true;
            }
            if (arg instanceof AggregateExpression) {
                for (Expression child : ((AggregateExpression) arg).children()) {
                    if (child instanceof SlotReference
                            && distinctMergeBuffers.contains(((SlotReference) child).getExprId())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Recursively rewrites partial-buffer slot references (recorded in localAggParams)
     * inside an expression back to their aggregated input expressions. The local buffer
     * column is often simply named after the function ("sum"), so an unreplaced nested
     * reference would leak a bare ambiguous "sum" into the decompiled aggregate argument
     * (e.g. sum(if(day = ?, buffer, buffer))).
     */
    private Expression resolveBufferSlots(Expression expr) {
        if (expr instanceof SlotReference) {
            Expression param = localAggParams.get(((SlotReference) expr).getExprId());
            return param == null ? expr : param;
        }
        if (expr.children().isEmpty()) {
            return expr;
        }
        List<Expression> newChildren = new ArrayList<>();
        boolean changed = false;
        for (Expression childExpr : expr.children()) {
            Expression rewritten = resolveBufferSlots(childExpr);
            newChildren.add(rewritten);
            if (rewritten != childExpr) {
                changed = true;
            }
        }
        return changed ? expr.withChildren(newChildren) : expr;
    }

    /**
     * Appends one global-aggregate SELECT item. Group-by columns pass through by their
     * name; an aggregate function is rewritten (partial references replaced by their
     * input expression, e.g. sum(partial_sum(x)#N) -> sum(x)) and given a stable
     * reference name (the user alias when it is a clean identifier, else a generated
     * {@code c_<seq>}) so upper layers reference the aggregate output column without
     * re-printing the function.
     */
    private void appendAggSelect(SQLRelation relation, SQLRelation child, NamedExpression output,
            List<Pair<ExprId, String>> selects, boolean distinctMergeCount,
            boolean stageHasDistinctMergeBuffer) {
        Expression inner = output instanceof Alias ? ((Alias) output).child() : output;
        if (inner instanceof SlotReference && isSystemColumnName(((SlotReference) inner).getName())) {
            // GROUPING_ID / rowid group-by marker: execution detail of ROLLUP, never a
            // user-visible aggregate output column
            return;
        }
        String sql;
        String ref;
        if (inner instanceof SlotReference) {
            // group-by column pass-through: reference by its (child) name
            sql = exprSqlBuilder.print(output, child);
            ref = sql;
            if (ref != null && ref.startsWith("GROUPING(")) {
                // GROUPING virtual column (ROLLUP): GROUPING(col) is only valid in the
                // SELECT of the GROUP BY GROUPING SETS / ROLLUP aggregate itself. Export
                // it here under a plain alias (c_<seq>) so upper projections reference the
                // alias column instead of re-printing GROUPING(col) in a non-grouping
                // scope ("LOGICAL_PROJECT should not contain grouping expression").
                String alias = stableRef(output);
                sql = ref + " AS " + alias;
                ref = alias;
                relation.registerRef(output.getExprId(), alias);
                relation.registerRef(((SlotReference) inner).getExprId(), alias);
            } else {
                relation.registerRef(output.getExprId(), ref);
            }
        } else if (inner instanceof AggregateExpression) {
            AggregateExpression aggExpr = (AggregateExpression) inner;
            AggregateFunction fn = aggExpr.getFunction();
            if (fn instanceof GroupConcat || fn instanceof MultiDistinctGroupConcat) {
                // Dedicated GROUP_CONCAT grammar: resolve the physical buffer slots first,
                // then render ([DISTINCT] value [ORDER BY ...] [SEPARATOR ...]) - a
                // comma-joined form would print the order keys as extra arguments
                // (group_concat(v, ',', k DESC)) and break after a reload.
                List<Expression> resolvedChildren = new ArrayList<>(fn.children().size());
                for (Expression arg : fn.children()) {
                    resolvedChildren.add(resolveBufferSlots(arg));
                }
                if (!resolvedChildren.equals(fn.children())) {
                    fn = (AggregateFunction) fn.withChildren(resolvedChildren);
                }
                String rendered = exprSqlBuilder.renderGroupConcat(fn, child);
                if (rendered == null) {
                    throw new UnsupportedOperationException(
                            "SPM decompile: group_concat shape is not supported yet");
                }
                String groupConcatRef = stableRef(output);
                if (groupConcatRef.equalsIgnoreCase(fn.getName())) {
                    groupConcatRef = generatedColumnName(output.getExprId());
                }
                relation.registerRef(output.getExprId(), groupConcatRef);
                selects.add(Pair.of(output.getExprId(), rendered + " AS " + groupConcatRef));
                return;
            }
            List<Expression> args = fn.children().isEmpty()
                    ? new ArrayList<>(aggExpr.children()) : fn.children();
            // the aggregate expression's own children are the physical buffer slots it
            // consumes (the function arguments may already be normalized to the data
            // column); they carry the distinct-merge provenance (see distinctMergeBuffers)
            List<Expression> bufferArgs = new ArrayList<>(aggExpr.children());
            List<String> argSqls = new ArrayList<>();
            boolean starCountArg = false;
            for (Expression arg : args) {
                // rewrite every partial-buffer slot reference inside the argument (also
                // nested ones, e.g. sum(if(day = ?, buffer, buffer)) where the local
                // buffer column happens to be named "sum") back to its input expression
                Expression resolved = resolveBufferSlots(arg);
                if (isNestedNoArgCount(resolved)) {
                    // A merge-finalize count(*) is physically represented with its local
                    // count(*) stage nested as the function argument
                    // (count(partial_count(*))); a star carries no data column, so it
                    // must collapse into count(*) and never leak as an argument
                    // (count(partial_count(*)) is not re-parseable).
                    starCountArg = true;
                    continue;
                }
                String argSql = resolved == null ? "" : exprSqlBuilder.print(resolved, child);
                if (!argSql.isEmpty()) {
                    argSqls.add(argSql);
                }
            }
            // strip the partial_* execution prefix (partial_sum -> sum, partial_count
            // -> count) so an intermediate stage fold renders the user aggregate name
            String aggName = fn.getName();
            if (aggName.startsWith("partial_")) {
                aggName = aggName.substring("partial_".length());
            }
            boolean distinct;
            if (fn.isDistinct()) {
                distinct = true;
            } else if (distinctMergeCount) {
                if (stageHasDistinctMergeBuffer) {
                    // DISTINCT_GLOBAL merge stage: SplitAggMultiPhase deliberately cleared
                    // isDistinct on the final function because a lower physical stage
                    // deduplicates its input; the decompiler folds that lower stage away,
                    // so EVERY aggregate whose expression consumes the distinct-dedup
                    // buffer (sum / avg / count / ...) must restore its DISTINCT - only
                    // checking count would freeze sum(DISTINCT x) as sum(x) and return a
                    // different value when x has duplicates. An aggregate riding along
                    // the same stage consumes a merge-chain buffer and stays plain (see
                    // distinctMergeBuffers).
                    distinct = isDistinctMergeArg(bufferArgs);
                } else {
                    // No buffer of the stage is marked (distinct plans built by other
                    // shapes): keep the historical blanket behavior for count and never
                    // invent DISTINCT for the other aggregates.
                    distinct = "count".equalsIgnoreCase(aggName);
                }
            } else {
                distinct = false;
            }
            String distinctSql = distinct ? "DISTINCT " : "";
            if ("count".equalsIgnoreCase(aggName) && starCountArg && argSqls.isEmpty()) {
                sql = aggName + "(" + distinctSql + "*)";
            } else {
                sql = aggName + "(" + distinctSql + String.join(", ", argSqls) + ")";
            }
            ref = stableRef(output);
            if (ref.equalsIgnoreCase(aggName)) {
                // The alias was generated by Doris from the function name (no user
                // alias, e.g. an execution-added pre-aggregate with several "sum"
                // columns). Keeping "sum" would decompile many identical aggregate
                // columns whose bare references are ambiguous on re-parse ("sum is
                // ambiguous: sum#23, sum#24, ..."). Emit a unique c_<seq> instead.
                ref = generatedColumnName(output.getExprId());
            }
            sql = sql + " AS " + ref;
        } else {
            sql = exprSqlBuilder.print(output, child);
            ref = sql;
        }
        relation.registerRef(output.getExprId(), ref);
        selects.add(Pair.of(output.getExprId(), sql));
    }

    /**
     * A clean user alias (e.g. "sum_qty"), otherwise a generated {@code c_<seq>} that is
     * unique within the decompiled SQL (see generatedColumnName).
     */
    private String stableRef(NamedExpression namedExpression) {
        if (namedExpression instanceof Alias) {
            String name = ((Alias) namedExpression).getName();
            if (name != null && name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                return name;
            }
        }
        return generatedColumnName(namedExpression.getExprId());
    }

    /**
     * Assigns (once per ExprId) a compact alias for a generated output column:
     * {@code c_<seq>} where seq grows over the current decompile. Memoized by the
     * ExprId so later references to the same column reuse the alias. The sequence is
     * reset at the start of every toSQL(Plan) call, so the produced SQL only
     * needs the aliases to be unique within itself and the numbers stay small and
     * readable regardless of the analyzer's ExprId values.
     */
    private String generatedColumnName(ExprId exprId) {
        return generatedColumnNames.computeIfAbsent(exprId, id -> "c_" + (++generatedColumnSeq));
    }

    // ==================== Sort / TopN / Limit ====================

    /**
     * PhysicalTopN: wraps as ORDER BY + LIMIT (reuses the child relation when it has no
     * limit/orderBy to avoid extra nesting).
     */
    @Override
    public SQLRelation visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, Void context) {
        Plan childNode = topN.child(0);
        SQLRelation child = process(childNode);

        // A two-phase distributed TopN (LOCAL_SORT stage feeding a MERGE_SORT stage
        // with the same keys/limit) is ONE SQL ORDER BY ... LIMIT ... . The inner
        // stage has already printed the sort keys + limit onto the (unwrapped) child
        // relation, so the outer stage must reuse that relation instead of wrapping
        // another subquery - a redundant inner "LIMIT n) t_x" would truncate the
        // result (e.g. a similar query with LIMIT 101 replaying a LIMIT 100 baseline
        // returns only 100 rows even after mergeLimits fixed the outer limit).
        if (skipPassThrough(childNode) instanceof PhysicalTopN) {
            String orderBySql = topN.getOrderKeys().stream()
                    .map(k -> exprSqlBuilder.print(k.getExpr(), child)
                            + (k.isAsc() ? " ASC" : " DESC")
                            + (k.isNullFirst() ? " NULLS FIRST" : " NULLS LAST"))
                    .collect(Collectors.joining(", "));
            child.setOrderBy(orderBySql);
            child.setLimit(topN.getOffset() > 0 ? topN.getOffset() + ", " : "");
            if (topN.getLimit() != Long.MAX_VALUE) {
                child.setLimit(child.getLimit() + topN.getLimit());
            }
            return child;
        }

        SQLRelation relation;
        if (child.getLimit().isEmpty() && child.getOrderBy().isEmpty()) {
            relation = child;
        } else {
            relation = new SQLRelation();
            relation.setFrom(child.toRelationSQL());
            relation.getColumnNames().putAll(child.getColumnNames());
            relation.newAlias();
        }

        // ORDER BY
        String orderBySql = topN.getOrderKeys().stream()
                .map(k -> exprSqlBuilder.print(k.getExpr(), relation)
                        + (k.isAsc() ? " ASC" : " DESC")
                        + (k.isNullFirst() ? " NULLS FIRST" : " NULLS LAST"))
                .collect(Collectors.joining(", "));
        relation.setOrderBy(orderBySql);
        // LIMIT
        relation.setLimit(topN.getOffset() > 0 ? topN.getOffset() + ", " : "");
        if (topN.getLimit() != Long.MAX_VALUE) {
            relation.setLimit(relation.getLimit() + topN.getLimit());
        }
        return relation;
    }

    /** Walks through execution-only exchange / distribute pass-through nodes so a
     * distributed two-phase TopN (MERGE_SORT -> [Distribute] -> LOCAL_SORT) is seen as
     * two adjacent TopN stages and folded into a single ORDER BY ... LIMIT .... */
    private static Plan skipPassThrough(Plan node) {
        while (node instanceof PhysicalDistribute) {
            Plan child = node.child(0);
            if (child == null) {
                return node;
            }
            node = child;
        }
        return node;
    }

    /**
     * PhysicalQuickSort: ORDER BY over the child relation (reuses the child relation when
     * it has no orderBy/limit to avoid extra nesting).
     */
    @Override
    public SQLRelation visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort, Void context) {
        SQLRelation child = process(sort.child(0));

        SQLRelation relation;
        if (child.getLimit().isEmpty() && child.getOrderBy().isEmpty()) {
            relation = child;
        } else {
            relation = new SQLRelation();
            relation.setFrom(child.toRelationSQL());
            relation.getColumnNames().putAll(child.getColumnNames());
            relation.newAlias();
        }

        // ORDER BY
        String orderBySql = sort.getOrderKeys().stream()
                .map(k -> exprSqlBuilder.print(k.getExpr(), relation)
                        + (k.isAsc() ? " ASC" : " DESC")
                        + (k.isNullFirst() ? " NULLS FIRST" : " NULLS LAST"))
                .collect(Collectors.joining(", "));
        relation.setOrderBy(orderBySql);
        return relation;
    }

    /**
     * PhysicalLimit: fills in LIMIT. Wraps the child when it already has a limit,
     * otherwise reuses it.
     */
    @Override
    public SQLRelation visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, Void context) {
        SQLRelation child = process(limit.child(0));
        SQLRelation limitRelation;
        if (child.getLimit().isEmpty()) {
            limitRelation = child;
        } else {
            limitRelation = new SQLRelation();
            limitRelation.setFrom(child.toRelationSQL());
            limitRelation.getColumnNames().putAll(child.getColumnNames());
            limitRelation.newAlias();
        }
        limitRelation.setLimit(limit.getOffset() > 0 ? limit.getOffset() + ", " : "");
        limitRelation.setLimit(limitRelation.getLimit() + limit.getLimit());
        return limitRelation;
    }

    // ==================== Project (wrap + SELECT list) ====================

    /**
     * PhysicalProject: assembles a SELECT list and wraps. The OUTERMOST projection (the
     * one that feeds the result) prunes its output to the final user-visible columns.
     * Every intermediate projection passes through the child's full output plus its own
     * expressions (each with a stable alias), so an upper filter / join / aggregate /
     * projection always resolves the columns it references - the intermediate column
     * pruning of the physical plan is an execution detail that must not break the
     * decompiled SQL's column scoping.
     */
    @Override
    public SQLRelation visitPhysicalProject(PhysicalProject<? extends Plan> project, Void context) {
        SQLRelation child = process(project.child(0));
        SQLRelation relation = new SQLRelation();
        relation.setFrom(child.toRelationSQL());
        relation.getColumnNames().putAll(child.getColumnNames());

        boolean isFinalProject = outputProjects.contains(project);
        List<Pair<ExprId, String>> selects = new ArrayList<>();
        Set<ExprId> need = neededOutputs.get(project);
        if (isFinalProject) {
            // outermost: prune to the final user-visible projection columns
            for (NamedExpression projectExpr : project.getProjects()) {
                appendProjectSelect(relation, child, projectExpr, selects);
            }
        } else {
            // intermediate: child output plus the projection's own columns, pruned to
            // the live columns (a projection used to re-emit the whole child column set
            // so an upper layer could resolve anything; with the top-down live-column
            // analysis only the columns actually referenced above are re-emitted)
            appendFullChildOutput(relation, child, selects);
            if (need != null) {
                selects.removeIf(p -> !need.contains(p.key()));
            }
            Set<ExprId> emitted = new HashSet<>();
            for (Pair<ExprId, String> select : selects) {
                emitted.add(select.first);
            }
            for (NamedExpression projectExpr : project.getProjects()) {
                if (emitted.contains(projectExpr.getExprId())) {
                    continue;
                }
                if (need != null && !need.contains(projectExpr.getExprId())) {
                    continue;
                }
                appendProjectSelect(relation, child, projectExpr, selects);
                emitted.add(projectExpr.getExprId());
            }
        }
        relation.setSelects(selects);
        relation.newAlias();
        return relation;
    }

    /**
     * Passes through the child relation's full output columns: its SELECT list when it
     * has one (a wrapped subquery), otherwise every registered column of a SELECT *
     * relation (filter / scan / join / ...). Internal system columns (e.g. Doris rowid
     * columns used by some joins / unique tables) are execution details and must never
     * surface in the decompiled SQL.
     *
     * Every SELECT item is passed through by its OUTPUT NAME (the column the child
     * subquery exports), never by re-emitting its defining expression - an expression
     * may reference qualified / inner columns (t_a.X, aggregates) that are out of scope
     * one level up. Expressions are exported as "expr AS alias" by the child, so the
     * parent references the bare alias.
     */
    private void appendFullChildOutput(SQLRelation relation, SQLRelation child,
            List<Pair<ExprId, String>> selects) {
        if (child.getSelects() != null && !child.getSelects().isEmpty()) {
            for (Pair<ExprId, String> select : child.getSelects()) {
                String value = select.value();
                if (isSystemColumnName(value)) {
                    continue;
                }
                // "expr AS alias" / "t_a.X AS c_5" -> reference the exported alias only
                int asIdx = value.toLowerCase().lastIndexOf(" as ");
                String ref = asIdx >= 0 ? value.substring(asIdx + 4).trim() : value;
                if (ref.isEmpty()) {
                    continue;
                }
                selects.add(Pair.of(select.key(), ref));
                relation.registerRef(select.key(), ref);
            }
        } else {
            for (Map.Entry<ExprId, String> entry : child.getColumnNames().entrySet()) {
                String ref = entry.getValue();
                // plain reference names only (skip expressions / qualified names - those
                // relations carry their own SELECT list instead)
                if (ref != null && !ref.contains("(") && !ref.contains(" ") && !ref.contains(".")
                        && !isSystemColumnName(ref)) {
                    selects.add(Pair.of(entry.getKey(), ref));
                }
            }
        }
    }

    /**
     * Doris internal columns that carry no SQL meaning and must be dropped from every
     * decompiled projection. Mirror of the internal-column naming defined in
     * Column: every hidden execution column is named under the
     * Column.HIDDEN_COLUMN_PREFIX "__DORIS_" family (rowid columns, version /
     * delete-sign / sequence columns, ...), plus the lowercase shadow prefix
     * Column.SHADOW_NAME_PREFIX; GROUPING_ID is the rollup execution marker
     * (the user never selects it; ROLLUP is expressed as GROUPING SETS). Any new
     * internal column added to Column under one of these prefixes is covered
     * automatically.
     */
    private static boolean isSystemColumnName(String name) {
        return name != null
                && (name.startsWith(Column.HIDDEN_COLUMN_PREFIX)
                || name.startsWith(Column.SHADOW_NAME_PREFIX)
                || "GROUPING_ID".equals(name));
    }

    /**
     * Appends one projection SELECT item: a plain column pass-through keeps its (child)
     * reference name; an expression is emitted as "expr AS [name]" where the name is the
     * user alias (clean identifier) or a generated {@code c_<seq>}.
     */
    private void appendProjectSelect(SQLRelation relation, SQLRelation child, NamedExpression projectExpr,
            List<Pair<ExprId, String>> selects) {
        Expression inner = projectExpr instanceof Alias ? ((Alias) projectExpr).child() : projectExpr;
        if (inner instanceof SlotReference && isSystemColumnName(((SlotReference) inner).getName())) {
            // internal system column (e.g. rowid used by some joins): execution detail,
            // not part of the user query - drop it from the decompiled projection.
            return;
        }
        String exprSql = exprSqlBuilder.print(projectExpr, child);
        String ref;
        if (inner instanceof SlotReference && !exprSql.contains(" ") && !exprSql.contains("(")
                && !exprSql.contains(".")) {
            // plain column pass-through: no alias, reference by its name
            ref = exprSql;
        } else {
            ref = stableRef(projectExpr);
            exprSql = exprSql + " AS " + ref;
        }
        relation.registerRef(projectExpr.getExprId(), ref);
        selects.add(Pair.of(projectExpr.getExprId(), exprSql));
    }

    // ==================== Window (wrap + OVER clause) ====================

    /**
     * PhysicalWindow: a window computation layer with no SQL equivalent of its own; it
     * must pass through the child's full output (so upper projections can still
     * reference the underlying columns such as group-by keys / plain columns) AND emit
     * the window-function columns. Only emitting the window columns would cut off every
     * other child column for the upper layers ("Unknown column 'i_item_id' in table
     * list" - a window over an aggregate leaves only the window result referenceable).
     */
    @Override
    public SQLRelation visitPhysicalWindow(PhysicalWindow<? extends Plan> window, Void context) {
        SQLRelation child = process(window.child(0));
        SQLRelation relation = new SQLRelation();
        relation.setFrom(child.toRelationSQL());
        relation.getColumnNames().putAll(child.getColumnNames());

        List<Pair<ExprId, String>> selects = Lists.newArrayList();
        appendFullChildOutput(relation, child, selects);
        Set<ExprId> need = neededOutputs.get(window);
        if (need != null) {
            selects.removeIf(p -> !need.contains(p.key()));
        }
        Set<ExprId> emitted = new HashSet<>();
        for (Pair<ExprId, String> select : selects) {
            emitted.add(select.first);
        }
        for (NamedExpression windowExpr : window.getWindowExpressions()) {
            if (emitted.contains(windowExpr.getExprId())) {
                continue;
            }
            if (need != null && !need.contains(windowExpr.getExprId())) {
                continue;
            }
            Expression inner = windowExpr instanceof Alias
                    ? ((Alias) windowExpr).child()
                    : windowExpr;
            String sql;
            if (inner instanceof WindowExpression) {
                sql = exprSqlBuilder.print(inner, relation);
            } else {
                sql = exprSqlBuilder.print(windowExpr, relation);
            }
            // export under a stable alias (user alias when clean, else c_<seq>) so an
            // upper projection can reference the window result column by name
            String ref = stableRef(windowExpr);
            relation.registerRef(windowExpr.getExprId(), ref);
            selects.add(Pair.of(windowExpr.getExprId(), sql + " AS " + ref));
            emitted.add(windowExpr.getExprId());
        }
        relation.setSelects(selects);
        relation.newAlias();
        return relation;
    }

    // ==================== SetOperation (UNION/EXCEPT/INTERSECT) ====================

    /**
     * PhysicalUnion / PhysicalExcept / PhysicalIntersect: join the child queries with
     * the corresponding operator.
     */
    @Override
    public SQLRelation visitPhysicalUnion(PhysicalUnion union, Void context) {
        return visitPhysicalSet(union, "UNION ALL", context);
    }

    @Override
    public SQLRelation visitPhysicalExcept(PhysicalExcept except, Void context) {
        return visitPhysicalSet(except, "EXCEPT", context);
    }

    @Override
    public SQLRelation visitPhysicalIntersect(PhysicalIntersect intersect, Void context) {
        return visitPhysicalSet(intersect, "INTERSECT", context);
    }

    private SQLRelation visitPhysicalSet(PhysicalSetOperation set, String op, Void context) {
        List<List<SlotReference>> childrenOutputs = set.getRegularChildrenOutputs();
        List<? extends Slot> outputs = set.getOutput();
        List<String> branchSqls = Lists.newArrayList();
        for (int i = 0; i < set.children().size(); i++) {
            SQLRelation childRelation = process(set.children().get(i));
            List<SlotReference> childOutputs = childrenOutputs.get(i);
            // Project EXACTLY the positional output of this branch (regularChildrenOutputs)
            // under the set's output names: blindly concatenating the child relation where it
            // stands would emit whatever columns that branch happened to produce (e.g. all 36
            // sales columns), so the names the outer SQL references after the set node would
            // never exist in the frozen planSql.
            SQLRelation branch = new SQLRelation();
            branch.setFrom(childRelation.toRelationSQL());
            branch.newAlias();
            List<Pair<ExprId, String>> selects = new ArrayList<>();
            for (int j = 0; j < childOutputs.size(); j++) {
                SlotReference slot = childOutputs.get(j);
                String columnRef = exprSqlBuilder.print(slot, childRelation);
                String outputName = j < outputs.size() ? outputs.get(j).getName() : slot.getName();
                String item = columnRef.equals(outputName)
                        ? columnRef : columnRef + " AS " + quoteIdentifier(outputName);
                selects.add(Pair.of(slot.getExprId(), item));
            }
            branch.setSelects(selects);
            branchSqls.add(branch.toRelationSQL());
        }
        // PhysicalUnion may carry constant one-row branches that rule
        // MergeOneRowRelationIntoUnion MOVED out of children() into constantExprsList.
        // Emitting only the regular children would silently DROP those rows at replay
        // (e.g. SELECT 1 UNION ALL SELECT x FROM t WHERE y = ? lost the SELECT 1 row;
        // a constant-only UNION rendered an empty body). Emit every constant row as a
        // positional SELECT branch under the set's output names.
        if (set instanceof PhysicalUnion) {
            for (List<NamedExpression> row : ((PhysicalUnion) set).getConstantExprsList()) {
                if (row.size() != outputs.size()) {
                    throw new UnsupportedOperationException(
                            "SPM decompile: union constant branch arity mismatch");
                }
                SQLRelation branch = new SQLRelation();
                List<Pair<ExprId, String>> selects = new ArrayList<>();
                for (int j = 0; j < row.size(); j++) {
                    NamedExpression project = row.get(j);
                    String item = exprSqlBuilder.print(project, branch);
                    String outputName = outputs.get(j).getName();
                    if (!item.equals(outputName)) {
                        item = item + " AS " + quoteIdentifier(outputName);
                    }
                    selects.add(Pair.of(project.getExprId(), item));
                }
                branch.setSelects(selects);
                // a FROM-less branch is a plain SELECT (toRelationSQL would return the
                // empty FROM text for it)
                branchSqls.add(branch.toSQL());
            }
        }
        SQLRelation setRelation = new SQLRelation();
        setRelation.setFrom("(" + String.join(" " + op + " ", branchSqls) + ")");
        setRelation.newAlias();
        // register the set outputs so upper nodes reference the produced column names
        for (int j = 0; j < outputs.size(); j++) {
            setRelation.registerRef(outputs.get(j).getExprId(),
                    quoteIdentifier(outputs.get(j).getName()));
        }
        return setRelation;
    }

    // ==================== GROUPING SETS (PhysicalRepeat) ====================

    /**
     * PhysicalRepeat (GROUPING SETS): builds the GROUPING SETS(...) expression onto the
     * child relation and returns it without wrapping - the parent global aggregate
     * (visitPhysicalHashAggregate) consumes the groupings field and emits
     * GROUP BY GROUPING SETS(...).
     */
    @Override
    public SQLRelation visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, Void context) {
        SQLRelation relation = process(repeat.child(0));

        SQLRelation groupingRelation = new SQLRelation();
        groupingRelation.setFrom(relation.toRelationSQL());
        groupingRelation.getColumnNames().putAll(relation.getColumnNames());

        // Register the GROUPING(...) virtual outputs the repeat emits for the enclosing
        // aggregate ("Grouping(col) AS GROUPING_PREFIX_col"): the aggregate groups by
        // these slots and the user's GROUPING(col) projection references them, so they
        // must decompile back to the GROUPING(col) expression (GROUPING_ID, the other
        // execution-only output, is dropped by isSystemColumnName).
        for (NamedExpression output : repeat.getOutputExpressions()) {
            Expression inner = output instanceof Alias ? ((Alias) output).child() : output;
            if (inner instanceof Grouping) {
                Expression groupArg = ((Grouping) inner).child();
                String groupingSql = "GROUPING("
                        + exprSqlBuilder.print(groupArg, groupingRelation) + ")";
                groupingRelation.registerRef(output.getExprId(), groupingSql);
            }
        }

        List<String> groupings = Lists.newArrayList();
        for (List<Expression> group : repeat.getGroupingSets()) {
            groupings.add("(" + group.stream()
                    .map(e -> exprSqlBuilder.print(e, groupingRelation))
                    .collect(Collectors.joining(", ")) + ")");
        }
        groupingRelation.setGroupings("GROUPING SETS(" + String.join(", ", groupings) + ")");
        // no newAlias: consumed by the parent global aggregate
        return groupingRelation;
    }

    // ==================== ASSERT_ROWS (PhysicalAssertNumRows) ====================

    /**
     * PhysicalAssertNumRows: marks the child relation with assertRows so toRelationSQL()
     * renders ASSERT_ROWS (SELECT ...) t_N (single-row assertion semantics, e.g.
     * SELECT ... WHERE EXISTS).
     */
    @Override
    public SQLRelation visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows, Void context) {
        SQLRelation child = process(assertNumRows.child(0));
        child.setAssertRows(true);
        if (child.getSelects().isEmpty() && child.getColumnNames().size() == 1) {
            Map.Entry<ExprId, String> only = child.getColumnNames().entrySet().iterator().next();
            child.setSelects(Lists.newArrayList(Pair.of(only.getKey(), only.getValue())));
        }
        child.newAlias();
        return child;
    }

    // ==================== helper methods ====================

    /**
     * JOIN type -> SQL keyword.
     */
    private String joinTypeToSql(JoinType joinType) {
        switch (joinType) {
            case INNER_JOIN:
                return "INNER JOIN";
            case LEFT_OUTER_JOIN:
                return "LEFT OUTER JOIN";
            case RIGHT_OUTER_JOIN:
                return "RIGHT OUTER JOIN";
            case FULL_OUTER_JOIN:
                return "FULL OUTER JOIN";
            case LEFT_SEMI_JOIN:
                return "LEFT SEMI JOIN";
            case RIGHT_SEMI_JOIN:
                return "RIGHT SEMI JOIN";
            case LEFT_ANTI_JOIN:
                return "LEFT ANTI JOIN";
            case RIGHT_ANTI_JOIN:
                return "RIGHT ANTI JOIN";
            case CROSS_JOIN:
                return "CROSS JOIN";
            case ASOF_LEFT_OUTER_JOIN:
                return "ASOF LEFT JOIN";
            case ASOF_LEFT_INNER_JOIN:
                return "ASOF INNER JOIN";
            case NULL_AWARE_LEFT_ANTI_JOIN:
                return "LEFT NULL_AWARE ANTI JOIN";
            default:
                // Reaching the default means a future JoinType would silently produce an
                // invalid SQL keyword - fail loudly instead of freezing a broken planSql.
                throw new UnsupportedOperationException(
                        "SPM decompile does not support join type " + joinType);
        }
    }

    /**
     * Infers the JOIN distribution HINT from the data distribution of the children.
     *
     * M1 basic rule: BROADCAST (DistributionSpecReplicated) on the right child gives
     * [BROADCAST]; any Distribution on either side gives [SHUFFLE]; otherwise no HINT.
     * The complete rules (COLOCATE/BUCKET) are added in a later milestone.
     */
    private String getJoinDistributionHints(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join) {
        Plan left = join.left();
        Plan right = join.right();
        if (right instanceof PhysicalDistribute) {
            DistributionSpec spec = ((PhysicalDistribute<?>) right).getDistributionSpec();
            if (spec instanceof DistributionSpecReplicated) {
                return HINT_JOIN_BROADCAST;
            }
            return HINT_JOIN_SHUFFLE;
        }
        if (left instanceof PhysicalDistribute) {
            return HINT_JOIN_SHUFFLE;
        }
        return "";
    }
}
