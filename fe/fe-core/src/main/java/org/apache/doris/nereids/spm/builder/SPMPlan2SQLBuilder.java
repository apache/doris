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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.trees.TableSample;
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
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Grouping;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeTVFScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

    /** Generated output names ("c_" + ExprId) produced by re-export labels and dedupe renames. */
    private static final Pattern GENERATED_NAME_PATTERN = Pattern.compile("\\bc_\\d+\\b");

    /** JOIN distribution HINT prefix constants. */
    private static final String HINT_JOIN_BROADCAST = "BROADCAST";
    private static final String HINT_JOIN_SHUFFLE = "SHUFFLE";

    /**
     * Re-export labels a hoist step created one level below ("X AS c_N" items): maps the
     * generated label back to the ExprId of the item it re-exports, so a later hoist step
     * can re-export the SAME label through one more projection level (tpcds q78: the
     * ORDER BY hoisted out of the 16-item projection references c_14/c_15/c_16, and the
     * 11-item projection above it must therefore re-export them).
     */
    private final Map<String, ExprId> reExportedLabels = new HashMap<>();

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
     * clean user alias it is exported under a generated c_N name (c_1, c_2, ...),
     * assigned in decompile order and memoized by the output ExprId so every reference
     * to the same column prints the same alias. A fresh per-decompile sequence (instead
     * of embedding the analyzer's ExprId) keeps the generated names small, readable and
     * independent of how many internal ExprIds the optimizer allocated.
     */
    private final Map<ExprId, String> generatedColumnNames = new HashMap<>();
    private int generatedColumnSeq = 0;

    /**
     * Normalized names already visible in the CURRENT decompile: user aliases and the
     * preserved source columns of the relations a generated name gets exported next to.
     * A generated c_N must avoid them - the counter alone only keeps the
     * GENERATED names apart, so a source column literally named c_1 (or a group-by
     * item / user alias of that name) could end up next to sum(v) AS c_1 or
     * MARK_SLOT c_1: two ExprIds registered under one visible name make the
     * enclosing projection / result sink read an AMBIGUOUS column from the derived
     * relation after reload. Project / window outputs repair duplicates afterwards
     * (dedupeSelectOutputNames); the join (MARK_SLOT / explicit projection) and aggregate
     * exports do not, so they reserve here instead.
     */
    private final Set<String> reservedOutputNames = new HashSet<>();

    /**
     * Rejects freezing when any expression of the plan carries a
     * SessionVarGuardExpr (see containsSessionVarGuard): the guard holds the
     * alias-UDF DEFINITION's saved session variables and has no SQL rendering. Every
     * expansion of a definition with saved variables retains the guard (see
     * AliasUdfBuilder), including creators whose variables already matched - the frozen
     * text would otherwise re-analyze the arithmetic under the CALLER's settings.
     *
     * @param plan the physical plan about to be decompiled
     */
    public static void rejectSessionVarGuardedExpressions(Plan plan) {
        org.apache.doris.nereids.spm.SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            for (org.apache.doris.nereids.trees.expressions.Expression expr
                    : node.getExpressions()) {
                if (containsSessionVarGuard(expr)) {
                    throw new UnsupportedOperationException("SPM cannot freeze an expression"
                            + " carrying a session-variable guard: " + expr);
                }
            }
        });
    }

    private static boolean containsSessionVarGuard(
            org.apache.doris.nereids.trees.expressions.Expression expr) {
        if (expr instanceof org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr) {
            return true;
        }
        for (org.apache.doris.nereids.trees.expressions.Expression child : expr.children()) {
            if (containsSessionVarGuard(child)) {
                return true;
            }
        }
        return false;
    }

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
        // An alias-UDF expansion computed under the DEFINITION's stored session
        // variables (decimalOverflowScale / enable_decimal256 / ...) carries a
        // SessionVarGuardExpr; the SQL text has no way to express that guard, so
        // printing only its child would replan the arithmetic under the LATER caller's
        // variables and could change its type, scale or value while the same bind SQL
        // still matches. Reject freezing instead (CREATE keeps the user planSql /
        // falls back to the parameterized-plan-tree path).
        rejectSessionVarGuardedExpressions(plan);
        // reset the per-decompile alias / generated-name sequences: t_N and c_N only
        // need to be unique WITHIN the one produced SQL, so numbering restarts here and
        // the decompiled text stays compact across calls
        SQLRelation.resetAliasCounter();
        cteBodies.clear();
        cteAliases.clear();
        cteDefinitions.clear();
        generatedColumnNames.clear();
        generatedColumnSeq = 0;
        reservedOutputNames.clear();
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
        // PhysicalAssertNumRows carries no node of its own in the frozen text: it renders
        // as the ASSERT_ROWS relation prefix wherever its parent references the input
        // relation (visitPhysicalAssertNumRows), and the parser rebuilds the assertion.
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
     * PhysicalStorageLayerAggregate: a scan-shaped shortcut that returns COUNT / MIN / MAX
     * for the wrapped scan from table or footer metadata instead of reading the data.
     * AggregateStrategies keeps the enclosing aggregate (or the constant-only project) on
     * TOP of the shortcut and keeps every expression in terms of the wrapped relation's
     * slots, so decompiling the wrapped relation publishes exactly those slots and the
     * enclosing operators render the same SQL aggregate (count(*) / min(x) / ...) over the
     * same table. Replaying that SQL re-derives an equivalent plan; there is no clause that
     * describes the shortcut itself and none is needed, because the shortcut is an
     * execution strategy of the aggregate rather than a different result.
     */
    @Override
    public SQLRelation visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, Void context) {
        return visitPhysicalRelation(storageLayerAggregate.getRelation(), context);
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
     * PhysicalLazyMaterializeFileScan: a file scan wrapped with lazy column
     * materialization; decompiled as a normal scan (including its scan modifiers -
     * the wrapper subclasses PhysicalFileScan, so the TABLESAMPLE / snapshot / scan
     * parameter rendering applies unchanged).
     */
    @Override
    public SQLRelation visitPhysicalLazyMaterializeFileScan(
            PhysicalLazyMaterializeFileScan scan, Void context) {
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
                // list to the user column order in place (preserving ORDER BY / LIMIT) -
                // UNLESS a re-labelled alias would SHADOW a name the ORDER BY references
                // "c_3 AS b" next to "ORDER BY b" rebinds the sort to the
                // alias although the clause was rendered against the base column b. Then
                // the relabel moves to the wrapper below, where the clause keeps its own
                // query block and the aliases cannot capture it.
                String inPlaceOrderBy = child.getOrderBy();
                if (inPlaceOrderBy.isEmpty()
                        || !orderByShadowedByRelabel(child, inPlaceOrderBy, ordered)) {
                    child.setSelects(ordered);
                    return child;
                }
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
            // A TOP-LEVEL ORDER BY / LIMIT pair is a semantic user clause: matching
            // deliberately ignores its LIMIT value, and mergeLimits adopts the user's value
            // by position - but a limit left INSIDE the derived table can never be reached
            // by that positional merge (the frozen root would have no Limit node next to
            // the user's Limit), so a matched LIMIT 200 query kept the captured 100-row
            // cap. This wrapper is a PURE output relabelling (it neither filters nor
            // aggregates), so hoisting the pair onto it is semantically identical and
            // makes the frozen root a Limit node again.
            // The ORDER BY moves WITH the LIMIT: a derived-table ORDER BY WITHOUT its
            // LIMIT is only a hint the optimizer is free to drop, and the outer SELECT
            // would then return an arbitrary LIMIT slice (a q02-style query replayed as
            // unordered rows). Clear both BEFORE rendering the child text - toSQL()
            // captures them into the string.
            String sinkOrderBy = child.getOrderBy();
            String sinkLimit = child.getLimit();
            if (!sinkOrderBy.isEmpty() || !sinkLimit.isEmpty()) {
                // Independently of the LIMIT: "SELECT a FROM t ORDER BY b + 1"
                // (no LIMIT at all) arrives here as a Sort under the final relabel wrapper,
                // and leaving the ORDER BY inside the derived table was NOT safe - the
                // frozen text is re-planned at replay, and a sort that the user asked for
                // but that sits one level down came back as unordered rows (Nereids drops
                // an inner sort it considers redundant). The wrapper is a pure output
                // relabelling, so the user clause belongs on it. Hidden sort keys the
                // child's SELECT list does not export are added to it first (see
                // hoistableOrderBy); a key that cannot be exported, or that a relabelled
                // wrapper alias would capture, keeps the clause inside the child - its own
                // query block still resolves it. ORDER BY and LIMIT
                // always move TOGETHER: a limit without its order picks arbitrary rows.
                String hoisted = sinkOrderBy.isEmpty() ? ""
                        : hoistableOrderBy(child, sinkOrderBy, ordered, true);
                if (hoisted != null) {
                    outer.setOrderBy(hoisted);
                    outer.setLimit(sinkLimit);
                    child.setOrderBy("");
                    child.setLimit("");
                }
            }
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
     * unquotable identifier: a column named a-b must be emitted as `a-b`,
     * otherwise the frozen projection re-parses as the subtraction a - b. A name that
     * looks plain can still be a RESERVED keyword (a legal quoted column named
     * from must not be emitted bare - the parser tokenizes that as the FROM
     * keyword rather than an identifier), so the keyword check decides as well.
     * Embedded backticks are doubled. Unquotable names stay verbatim, so ordinary
     * schemas keep byte-identical frozen SQL. Also used for result-column labels (the
     * expression text of an un-aliased output column such as
     * round((sun_sales1 / sun_sales2), 2) is wrapped so the frozen planSql can
     * carry the original column header verbatim).
     */
    public static String quoteIdentifier(String name) {
        if (name == null) {
            return null;
        }
        if (name.matches("[A-Za-z_][A-Za-z0-9_]*") && NereidsParser.isValidUnquotedIdentifier(name)) {
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

    /**
     * Fully-qualified name of a table with every COMPONENT quoted separately
     * (catalog.db.table). getNameWithFullQualifiers() FLATTENS a legal quoted
     * component such as `t.a` into "internal.db.t.a", and splitting that on every dot
     * would emit FOUR identifiers instead of the intended three-part name with `t.a`
     * quoted as ONE component - a manually created frozen baseline carrying that broken
     * text then fails re-analysis after a reload, and no raw fallback tree exists for a
     * frozen row. The metadata components are taken structurally, never re-split.
     */
    static String quoteQualifiedTableName(org.apache.doris.catalog.TableIf table) {
        StringBuilder sb = new StringBuilder();
        for (String component : table.getFullQualifiers()) {
            if (sb.length() > 0) {
                sb.append('.');
            }
            sb.append(quoteIdentifier(component));
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
        // A TEMPORARY table's catalog object carries the CREATOR session's internal name
        // (<sessionId>_#TEMP#_<name>), not the text the user typed. Emitting it into the
        // frozen planSql would persist a session-scoped identifier: another session
        // running the same "FROM t" text matches the same bind key (catalog.db.t), and
        // Database.getTableNullable accepts the already-marked internal name unchanged,
        // so the replay would read the CREATOR's (possibly still live) temporary table
        // instead of its own t. Never render it - the GLOBAL create / capture path
        // rejects temporary relations before freezing (SPMPlanner), and a SESSION-scope
        // freeze (where the session's own temp table is the intended target) falls back
        // to the raw user text instead.
        if (catalogRelation.getTable().isTemporary()) {
            throw new UnsupportedOperationException(
                    "SPMPlan2SQLBuilder does not support temporary tables: the physical relation"
                    + " carries the creator session's internal name");
        }
        SQLRelation sqlRelation = new SQLRelation();
        // Emit the fully qualified name (catalog.db.table) so the frozen planSql resolves
        // the same table when it is replayed from a session whose current database (or
        // catalog) differs from the one used at CREATE time (cross-db queries,
        // information_schema, ...). Tables without a database (e.g. FunctionGenTable)
        // keep the bare name. Every COMPONENT is backtick-quoted separately when it is
        // not a plain identifier, so a metadata name containing operators is re-parsed
        // as an identifier instead of an expression, and a legal quoted component such
        // as `t.a` keeps its boundary (see quoteQualifiedTableName). Scan modifiers
        // (partition selection, TABLESAMPLE, snapshot, scan parameters) follow the name
        // in grammar order.
        String table = catalogRelation.getTable().getDatabase() == null
                ? quoteIdentifier(catalogRelation.getTable().getName())
                : quoteQualifiedTableName(catalogRelation.getTable());
        sqlRelation.setFrom(table + renderScanModifiers(relation));
        // Remember WHICH table this scan reads: two occurrences of one table are a self
        // join even when their FROM texts differ (each occurrence may carry its own
        // PARTITION / TABLESAMPLE pin), and the join must wrap both sides then.
        sqlRelation.setRelationIdentity(table);
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
        relation.setFrom(renderTableValuedFunction(tvfRelation.getFunction()));
        for (Slot slot : tvfRelation.getOutput()) {
            relation.registerRef(slot.getExprId(), quoteIdentifier(slot.getName()));
        }
        return relation;
    }

    /**
     * Renders one TVF call safely. TableValuedFunction.computeToSql() concatenates each
     * concrete key/value between apostrophes WITHOUT escaping, so a valid property such
     * as an S3 object key containing an apostrophe or backslash would freeze malformed
     * SQL: with a placeholder-bearing predicate the row is classified as frozen, no
     * fallback tree is rebuilt, and after refresh/restart the baseline silently stops
     * applying. The property map is serialized in a deterministic key order with the
     * same default-mode-safe quoting as every other SPM-emitted string literal (the
     * stored text is always re-parsed under MODE_DEFAULT).
     */
    private static String renderTableValuedFunction(TableValuedFunction function) {
        String args = new TreeMap<>(function.getTVFProperties().getMap()).entrySet().stream()
                .map(kv -> quoteSqlString(kv.getKey()) + " = " + quoteSqlString(kv.getValue()))
                .collect(Collectors.joining(", "));
        return quoteIdentifier(function.getName()) + "(" + args + ")";
    }

    /** PhysicalLazyMaterializeTVFScan: a TVF scan wrapped by lazy materialization. */
    @Override
    public SQLRelation visitPhysicalLazyMaterializeTVFScan(
            PhysicalLazyMaterializeTVFScan scan, Void context) {
        return visitPhysicalTVFRelation(scan, context);
    }

    /**
     * Renders the scan modifiers that the frozen SQL must carry, in the order the grammar
     * accepts them after the table name
     * (optScanParams, materializedViewName, tableSnapshot, specifiedPartition, sample):
     *
     * olap scans: @paramType(...) parameters (binlog reads), the partition list
     *     when the scan reads a strict non-empty subset of the table partitions, and
     *     TABLESAMPLE;
     * file scans: @paramType(...) parameters, FOR VERSION/TIME AS OF
     *     snapshots and TABLESAMPLE.
     *
     * Dropping any of them would silently change what the frozen planSql reads: a
     * "FROM t PARTITION(p1)" baseline would replay over every partition, a snapshot read
     * would run against the moving head of the table, and a sampled scan would return the
     * full table.
     *
     * The partition list is emitted only when the user pinned it (a pruned scan re-derives
     * its selection by replay); a frozen text without the pin simply does not match the
     * pinned user query (a safe miss, never a replay over the wrong partitions).
     *
     * Two scan states are deliberately NOT emitted. The selected index is an optimizer
     * choice (a rollup is a consistent copy, so the choice carries no semantics) that
     * cannot be told apart from a user-written INDEX clause; freezing it would pin the
     * optimization and stop the plain user query from matching. A user-written INDEX
     * pin only survives in the user tree, so such queries never match a frozen text that
     * dropped the pin.
     */
    private static String renderScanModifiers(PhysicalRelation relation) {
        StringBuilder modifiers = new StringBuilder();
        if (relation instanceof PhysicalOlapScan) {
            PhysicalOlapScan scan = (PhysicalOlapScan) relation;
            modifiers.append(renderScanParams(scan.getScanParams()));
            modifiers.append(renderPartitionSelection(scan));
            modifiers.append(renderTabletSelection(scan));
            modifiers.append(renderTableSample(scan.getTableSample()));
        } else if (relation instanceof PhysicalFileScan) {
            PhysicalFileScan scan = (PhysicalFileScan) relation;
            modifiers.append(renderScanParams(scan.getScanParams()));
            modifiers.append(renderTableSnapshot(scan.getTableSnapshot()));
            modifiers.append(renderTableSample(scan.getTableSample()));
        }
        return modifiers.toString();
    }

    /**
     * OLAP partition selection: the ids of a user-written PARTITION(...) /
     * TEMPORARY PARTITION(...) list are frozen as that very clause - including
     * when the pin happens to cover every partition that exists right now (a cardinality
     * test would drop the clause and a later ADD PARTITION would let the pinned query
     * silently read the new partition) and including the temporary namespace (a temp pin
     * replayed as a formal PARTITION(name) binds the wrong partition or fails to bind).
     * Partition pruning also shrinks selectedPartitionIds without any user pin,
     * so only the manual provenance is ever rendered; a pruned scan re-derives its
     * selection by replay. Ids are sorted so the frozen text is deterministic (the
     * matcher compares the selection as a set).
     */
    private static String renderPartitionSelection(PhysicalOlapScan scan) {
        List<Long> pinnedIds = scan.getManuallySpecifiedPartitions();
        if (pinnedIds.isEmpty()) {
            return "";
        }
        OlapTable table = scan.getTable();
        List<Long> sortedIds = new ArrayList<>(pinnedIds);
        Collections.sort(sortedIds);
        boolean temporary = table.isTemporaryPartition(sortedIds.get(0));
        StringBuilder partition = new StringBuilder(temporary ? " TEMPORARY PARTITION(" : " PARTITION(");
        for (int i = 0; i < sortedIds.size(); i++) {
            if (i > 0) {
                partition.append(", ");
            }
            if (table.isTemporaryPartition(sortedIds.get(i)) != temporary) {
                throw new UnsupportedOperationException(
                        "SPM decompile: a partition pin mixing temporary and formal partitions"
                                + " has no single-clause rendering");
            }
            Partition partitionMeta = table.getPartition(sortedIds.get(i));
            partition.append(quoteIdentifier(partitionMeta.getName()));
        }
        return partition.append(')').toString();
    }

    /**
     * OLAP tablet pin: TABLET(id, ...) is frozen when the user wrote it. Bucket
     * pruning also fills selectedTabletIds, so only the manual provenance is
     * rendered; without the clause a replayed scan would read every tablet of the
     * selected partitions and could return rows the captured query excluded. Ids are
     * sorted (the matcher compares the tablet list as a set).
     */
    private static String renderTabletSelection(PhysicalOlapScan scan) {
        List<Long> pinnedIds = scan.getManuallySpecifiedTabletIds();
        if (pinnedIds.isEmpty()) {
            return "";
        }
        List<Long> sortedIds = new ArrayList<>(pinnedIds);
        Collections.sort(sortedIds);
        StringBuilder tablet = new StringBuilder(" TABLET(");
        for (int i = 0; i < sortedIds.size(); i++) {
            if (i > 0) {
                tablet.append(", ");
            }
            tablet.append(sortedIds.get(i));
        }
        return tablet.append(')').toString();
    }

    /**
     * TABLESAMPLE(n PERCENT | n ROWS) [REPEATABLE seed]: both olap and file scans keep the
     * user's sample. Dropping it would replay over the full table and return rows the
     * captured plan never sampled in.
     */
    private static String renderTableSample(Optional<TableSample> tableSample) {
        if (!tableSample.isPresent()) {
            return "";
        }
        TableSample sample = tableSample.get();
        StringBuilder modifiers = new StringBuilder(" TABLESAMPLE(")
                .append(sample.sampleValue)
                .append(sample.isPercent ? " PERCENT)" : " ROWS)");
        if (sample.seek >= 0) {
            modifiers.append(" REPEATABLE ").append(sample.seek);
        }
        return modifiers.toString();
    }

    /**
     * FOR VERSION AS OF / FOR TIME AS OF: a time-travel read must stay pinned to the
     * captured version, otherwise the replay reads the current table contents. A numeric
     * version is emitted as a literal, every other value (and all times) as a string.
     */
    private static String renderTableSnapshot(Optional<TableSnapshot> tableSnapshot) {
        if (!tableSnapshot.isPresent()) {
            return "";
        }
        TableSnapshot snapshot = tableSnapshot.get();
        if (snapshot.getType() == TableSnapshot.VersionType.TIME) {
            return " FOR TIME AS OF " + quoteSqlString(snapshot.getValue());
        }
        String value = snapshot.getValue();
        if (value.matches("[0-9]+")) {
            return " FOR VERSION AS OF " + value;
        }
        return " FOR VERSION AS OF " + quoteSqlString(value);
    }

    /**
     * The @paramType(...) read parameters (incremental / branch / tag / options /
     * snapshot / reset): the map form when the parameters carry key/value pairs, otherwise
     * the bare identifier list form. Dropping them would replay a different (e.g.
     * non-incremental) read than the captured one.
     */
    private static String renderScanParams(Optional<TableScanParams> scanParams) {
        if (!scanParams.isPresent()) {
            return "";
        }
        TableScanParams params = scanParams.get();
        StringBuilder modifiers = new StringBuilder(" @").append(params.getParamType()).append('(');
        Map<String, String> mapParams = params.getMapParams();
        if (!mapParams.isEmpty()) {
            boolean first = true;
            for (Map.Entry<String, String> param : mapParams.entrySet()) {
                if (!first) {
                    modifiers.append(", ");
                }
                first = false;
                modifiers.append(quoteIdentifier(param.getKey()))
                        .append(" = ")
                        .append(quoteSqlString(param.getValue()));
            }
        } else {
            List<String> listParams = params.getListParams();
            for (int i = 0; i < listParams.size(); i++) {
                if (i > 0) {
                    modifiers.append(", ");
                }
                modifiers.append(quoteIdentifier(listParams.get(i)));
            }
        }
        return modifiers.append(')').toString();
    }

    /**
     * A single-quoted SQL string literal: embedded single quotes are doubled AND
     * backslashes are doubled, so the value cannot terminate the literal, change the frozen
     * SQL structure, or decode to a DIFFERENT value. The DEFAULT sql_mode treats a backslash
     * as an escape introducer (a semantic value such as release\next would otherwise
     * reparse as release + newline + ext under that mode and select another external ref),
     * so the literal doubles it; the SPM re-parses pin the DEFAULT mode (see
     * SPMPlanner.parseStoredSelect), which makes the round trip exact under default AND
     * NO_BACKSLASH_ESCAPES sessions alike.
     */
    static String quoteSqlString(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }

    // ==================== Generate (LATERAL VIEW) ====================

    /**
     * PhysicalGenerate: LATERAL VIEW clauses over the child relation - one clause per
     * generator. The parser wraps every user-written LATERAL VIEW ... into its
     * own single-generator node, while MergeGenerates (for two independent
     * views) folds stacked nodes into one node carrying several generators; the
     * executor rolls several functions over each child row, i.e. a cartesian expansion,
     * which is exactly what stacked LATERAL VIEWs express (MergeGenerates only merges
     * when the upper view does not reference the lower view's output). LATERAL VIEW
     * clauses attach to the child's COMPLETE query block: the child's WHERE / GROUP BY /
     * HAVING / ORDER BY / LIMIT clauses must stay inside the lateral-view input (a
     * derived table with LIMIT 10 limits the input, not the exploded rows). The
     * generator output columns are registered on the same relation so parent operators
     * can reference them; the alias is taken from the output slot's qualifier when it
     * survived analysis, otherwise a per-decompile lv_N alias is used. Generate
     * conjuncts (if any) are appended to the relation's WHERE.
     */
    @Override
    public SQLRelation visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, Void context) {
        SQLRelation childRelation = process(generate.child(0));
        List<Function> generators = generate.getGenerators();
        List<Slot> outputs = generate.getGeneratorOutput();
        if (generators.isEmpty() || generators.size() != outputs.size()) {
            // post-binding plans keep exactly one output slot per generator (multi-column
            // generators carry their columns in the expand alias project above)
            throw new UnsupportedOperationException("SPM decompile generate: generator/output arity mismatch "
                    + generators.size() + "/" + outputs.size());
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
        List<String> aliases = new ArrayList<>(outputs.size());
        // The analyzer may name a generator output with an internal column name
        // ("$c$N"); such a name cannot be referenced in SQL, so give it a generated
        // visible name and register the slot under that name for the parent operators.
        // A generator output may also COLLIDE with a name the child already exports
        // (SELECT t.x, lv.x FROM t LATERAL VIEW explode(t.arr) lv AS x is valid: the two
        // slots carry different qualifiers, but the frozen SQL references a derived
        // relation's columns by name) - registering both as bare x made the parent emit
        // SELECT x, x ..., which fails binding as ambiguous after reload. Rename the
        // generator output to a unique visible name and register THAT for the slot.
        Set<String> takenNames = new HashSet<>();
        for (String existing : childRelation.getColumnNames().values()) {
            if (existing != null) {
                takenNames.add(existing.replace("`", ""));
            }
        }
        List<String> columnNames = new ArrayList<>(outputs.size());
        for (Slot slot : outputs) {
            String alias = slot.getQualifier().isEmpty()
                    ? "" : slot.getQualifier().get(slot.getQualifier().size() - 1);
            aliases.add(alias.isEmpty() ? "lv_" + (lateralViewSeq++) : alias);
            String name = slot.getName();
            String visible = name == null || name.startsWith("$c$")
                    ? "lv_col_" + (lateralViewSeq++) : name;
            while (takenNames.contains(visible)) {
                visible = visible + "_";
            }
            takenNames.add(visible);
            columnNames.add(visible);
        }
        StringBuilder from = new StringBuilder(baseSql);
        for (int i = 0; i < generators.size(); i++) {
            from.append(" LATERAL VIEW ")
                    .append(exprSqlBuilder.print(generators.get(i), relation))
                    .append(' ')
                    .append(quoteIdentifier(aliases.get(i)))
                    .append(" AS ")
                    .append(quoteIdentifier(columnNames.get(i)));
        }
        relation.setFrom(from.toString());
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
            // The conjuncts are the generate's original join ON predicates (they reach
            // the TableFunctionNode as expandConjuncts). On an OUTER generator the BE
            // keeps ONE NULL-extended left row when every generated value fails them
            // (LEFT JOIN UNNEST(arr) AS u(x) ON u.x > 0 over arr = [-1] keeps the left
            // row), while the LATERAL VIEW + WHERE rendering below would filter that row
            // away. The frozen text has no rendering for that ON semantics, so decline
            // and let the rewrite replay the parameterized tree.
            for (Function generator : generators) {
                if (isOuterGenerator(generator)) {
                    throw new UnsupportedOperationException("SPM decompile generate: the join ON"
                            + " conjuncts of the outer generator " + generator + " rely on"
                            + " expandConjuncts keeping the NULL-extended row when every generated"
                            + " value fails them; replay the parameterized tree instead");
                }
            }
            String conjuncts = generate.getConjuncts().stream()
                    .map(expr -> exprSqlBuilder.print(expr, relation))
                    .collect(Collectors.joining(" AND "));
            relation.setWhere(relation.getWhere().isEmpty()
                    ? conjuncts
                    : relation.getWhere() + " AND " + conjuncts);
        }
        return relation;
    }

    /**
     * Whether one generator renders its OUTER form - the "xxx_outer" function family
     * (explode_outer / posexplode_outer / ...; the catalog derives the same name for
     * outer lookup) or an Unnest still carrying the outer flag before convertUnnest.
     * Only the outer form keeps the NULL-extended row when the expandConjuncts fail
     * (see visitPhysicalGenerate).
     */
    private static boolean isOuterGenerator(Function generator) {
        if (generator instanceof Unnest) {
            return ((Unnest) generator).isOuter();
        }
        return generator.getName().endsWith("_outer");
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
     * PhysicalCTEConsumer: emits FROM the CTE alias - a reference to the WITH
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
                    // top-level AS only: a quoted name may itself contain " as "
                    if (selectOutputName(select.value()).equals(name)) {
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
     * Rendered as a NULL value cast to the column's type (SELECT ... FROM (SELECT 1) WHERE FALSE)
     * so parent
     * operators can still reference its output columns AND each column keeps its DECLARED
     * type: defaulting every column to INT 1 (a) makes the frozen text fail to analyze once
     * the placeholder substitution types another branch (e.g. DATEV2), and (b) lets a
     * reanalyzed placeholder-bearing UNION widen the branch (INT + DATEV2 -> DATETIMEV2)
     * and change the result metadata. WHERE FALSE still filters the row out, and CAST(NULL
     * AS type) is grammar-parseable for every type.
     */
    @Override
    public SQLRelation visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, Void context) {
        SQLRelation relation = new SQLRelation();
        List<Pair<ExprId, String>> selects = Lists.newArrayList();
        for (NamedExpression project : emptyRelation.getProjects()) {
            ExprId id = project.getExprId();
            String name = generatedColumnName(id);
            selects.add(Pair.of(id, "CAST(NULL AS " + project.getDataType().toSql() + ") AS " + name));
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
        // natively in one of three shapes:
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

        // Materialize BOTH FROM fragments BEFORE any qualified reference is built, but
        // AFTER the same-alias force-wrap below: toRelationSQL() allocates the FINAL
        // alias when the relation needs a wrapper - a TopN / Limit over a set operation
        // carries fromCarriesAlias AND its own ORDER BY / LIMIT block, so it is wrapped
        // one level deeper under a FRESH alias. Building the ON condition (or the
        // explicit projection's qualifiers) through ensureQualifierAlias() first made
        // them reference the INNER set alias (t_0) while the FROM fragment exposed t_1 -
        // the frozen SQL could not bind after reload. toRelationSQL() allocates at most
        // one alias per relation, so the fragments are materialized once and reused.
        SQLRelation joinRelation = new SQLRelation();
        String hints = getJoinDistributionHints(join);
        String hintStr = hints.isEmpty() ? "" : "[" + hints + "]";

        // ===== column-name collision handling =====
        // Computed BEFORE the FROM fragments are materialized: when the columns collide,
        // EVERY column of both sides is referenced QUALIFIED below, and that qualifier can
        // require a wrapper (a composite FROM such as a scan carrying scan modifiers).
        // Allocating the wrapper only while the references were built left the FROM
        // fragment inline while the references used the freshly allocated t_N alias - a
        // frozen text that cannot bind on replay.
        boolean columnConflicts = intersectsIgnoreCase(
                left.getColumnNames().values(), right.getColumnNames().values());
        // same relation alias on both sides (e.g. self join) -> force subquery wrap. The
        // SCAN identity covers the self join whose FROM texts differ (different scan
        // selectors): the older FROM-text comparison missed it and emitted the same table
        // twice without aliases ("Not unique table/alias" on replay).
        if (left.isSameRelation(right)
                || StringUtils.equalsIgnoreCase(left.getRelationAlias(), right.getRelationAlias())) {
            left.newAlias();
            right.newAlias();
        } else if (columnConflicts) {
            // allocate the wrapper aliases NOW: ensureQualifierAlias() wraps a composite
            // FROM (scan modifiers, a TVF call) so the references below and the FROM
            // fragment agree
            left.ensureQualifierAlias();
            right.ensureQualifierAlias();
        }
        String leftSql = left.toRelationSQL();
        String rightSql = right.toRelationSQL();
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

        if (columnConflicts) {
            // qualify every column with its side's alias (needed by the ON clause AND by
            // the explicit projection below)
            for (Map.Entry<ExprId, String> entry : left.getColumnNames().entrySet()) {
                joinRelation.registerRef(entry.getKey(),
                        left.ensureQualifierAlias() + "." + entry.getValue());
            }
            for (Map.Entry<ExprId, String> entry : right.getColumnNames().entrySet()) {
                joinRelation.registerRef(entry.getKey(),
                        right.ensureQualifierAlias() + "." + entry.getValue());
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
            String markName = generatedColumnName(markSlot.getExprId(),
                    scopeNames(joinRelation));
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
        joinRelation.setFrom(leftSql + " " + joinTypeWord + hintStr + " "
                + rightSql + markSpec + matchSql + effectiveOn);
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
                String alias = generatedColumnName(entry.getKey(),
                        scopeNames(joinRelation));
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
        } else {
            // The key was found among the hash conjuncts: the other conjuncts STILL have
            // to become the subquery filter. The old `if (key == null)` guard skipped
            // this loop entirely and silently DROPPED every residual other conjunct -
            // NULL_AWARE_LEFT_ANTI(hash=[l.a = r.b], other=[r.b > 5]) froze as
            // "l.a NOT IN (SELECT r.b FROM r)" without r.b > 5, so with both keys 2 the
            // original anti join kept the row (r.b > 5 is FALSE and the NOT IN is
            // evaluated on the join output) while the replay dropped it.
            subWhere.addAll(other);
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

        // Materialize BOTH fragments BEFORE the qualifier below is taken: toRelationSQL()
        // allocates the FINAL wrapper alias when the relation needs one (a left / right
        // relation carrying its own ORDER BY / LIMIT block, a FROM-less relation), and the
        // qualifier must be the alias that is VISIBLE in these fragments (mirrors
        // visitPhysicalJoin).
        String leftSql = left.toRelationSQL();
        String rightSql = right.toRelationSQL();

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
        String leftQualifier = null;
        if (nameConflict) {
            // The qualifier must be the alias VISIBLE in leftSql. Materialization above
            // already allocated the FINAL wrapper alias for a relation carrying its own
            // ORDER BY / LIMIT block or an aliased set relation, so that alias is read
            // back here. A COMPOSITE FROM that materialized INLINE - a scan carrying
            // PARTITION / TABLESAMPLE (the row form of a pinned left scan), a TVF call -
            // still has to be WRAPPED, and that allocation CHANGES leftSql: the emitted
            // NOT IN probe would reference the fresh t_N while the outer FROM kept the
            // bare scan, so the frozen SQL could not analyze. Re-render it.
            String visibleBefore = left.getRelationAlias();
            leftQualifier = left.ensureQualifierAlias();
            if (leftQualifier == null || leftQualifier.isEmpty()) {
                throw new UnsupportedOperationException(
                        "SPM decompile NULL_AWARE anti: cannot qualify the left side columns");
            }
            if (!leftQualifier.equals(visibleBefore)) {
                leftSql = left.toRelationSQL();
            }
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
                + ") NOT IN (SELECT " + buildSql + " FROM " + rightSql + whereSql + ")";

        // the anti join keeps the left rows whose NOT IN result is TRUE; the predicate
        // becomes the WHERE clause of this relation, and the left columns are re-exported
        SQLRelation joinRelation = new SQLRelation();
        joinRelation.setFrom(leftSql);
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
        // The distinct keys SplitAggMultiPhase put BELOW this stage: the eliminated
        // dedup stages group by (the user's group keys + the DISTINCT arguments), so
        // the extra keys identify an aggregate whose DISTINCT the split CLEARED. See
        // dedupKeysBelow for the reviewer's sum(DISTINCT x) with a non-empty GROUP BY.
        Set<ExprId> dedupKeyIds = distinctMergeCount
                ? dedupKeysBelow(agg.child(0), ownGroupKeyIds(agg)) : Collections.emptySet();
        List<Pair<ExprId, String>> selects = new ArrayList<>();
        for (NamedExpression output : agg.getOutputExpressions()) {
            appendAggSelect(aggRelation, childRelation, output, selects, distinctMergeCount,
                    stageHasDistinctMergeBuffer, dedupKeyIds);
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

    /**
     * Whether the aggregate expression is a partial (product-buffer) local aggregate.
     *
     * The ONLY reliable provenance is the aggregation MODE: the physical plan keeps the
     * USER'S function instance in every stage (a partial_sum buffer is a Sum expression
     * with aggMode.productAggregateBuffer), and the "partial_" text is synthesized
     * when the buffer stage is RENDERED (AggregateExpression#computeToSql()). A
     * name test therefore misclassifies a user UDAF whose own name starts with
     * partial_: its one-phase GLOBAL aggregate looked like an execution-only
     * intermediate stage, the whole aggregate was folded away and the frozen SQL degraded
     * to SELECT * FROM t (wrong cardinality AND columns on every later hit).
     */
    private static boolean isPartialAggregate(AggregateExpression aggExpr) {
        return aggExpr.getAggregateParam().aggMode.productAggregateBuffer;
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
     * an execution buffer slot (a scan-level pushAggOp=COUNT has no LOCAL
     * PhysicalHashAggregate node to record, so the enclosing count(*) references the
     * buffer slot whose name is the count-star SQL). A star contributes no data column,
     * so it collapses into a plain count(*) instead of leaking as the invalid
     * count(partial_count(*)) / count(count()).
     *
     * The slot form is decided by PROVENANCE, never by the name alone: a slot the
     * child relation EXPORTS as a column is a data argument no matter what it is called
     * (a quoted user column named count() - legal under
     * enable_unicode_name_support - froze as count(*) and every baseline hit then counted
     * ROWS where the column is NULL, changing the value). Only a slot the relation does
     * not export (an execution-only buffer) may collapse.
     *
     * @param expr  the aggregate argument (already resolved through localAggParams)
     * @param child the decompiled child relation of the aggregate (null when unknown)
     */
    private static boolean isNestedNoArgCount(Expression expr, SQLRelation child) {
        if (expr instanceof SlotReference) {
            SlotReference slot = (SlotReference) expr;
            if (child != null && child.getColumnNames().containsKey(slot.getExprId())) {
                // a real / derived column of the child relation: a data argument
                return false;
            }
            String name = slot.getName();
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
        // the physical plan keeps the USER'S function instance in every stage (the
        // "partial_" prefix is a rendering of the aggregate mode - see isPartialAggregate),
        // so the count-star buffer function is named exactly "count"; stripping a user
        // function's own prefix here would turn a user UDAF named partial_count into a
        // star as well
        if (!"count".equalsIgnoreCase(fn.getName())) {
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

    /** The ExprIds of the group-by expressions' slots of one aggregate stage. */
    private static Set<ExprId> ownGroupKeyIds(PhysicalHashAggregate<? extends Plan> agg) {
        Set<ExprId> ids = new HashSet<>();
        for (Expression group : agg.getGroupByExpressions()) {
            collectAllSlotIds(group, ids);
        }
        return ids;
    }

    /**
     * The DISTINCT keys SplitAggMultiPhase placed below a final aggregate stage: every
     * ELIMINATED aggregate stage of the child chain groups by (the user's group keys +
     * the DISTINCT arguments), so the keys that are NOT this stage's own are exactly
     * those distinct arguments. SplitAggDistinct rewrites sum(DISTINCT x) into a
     * final sum(x) whose isDistinct is CLEARED (the lower stage's grouping IS the
     * deduplication); the decompiler folds that stage away, so consuming the key directly
     * (no lower partial buffer) is the only remaining evidence of the user's DISTINCT and
     * has to restore it - otherwise the frozen SQL sums the key's multiplicity instead of
     * its distinct values (SELECT k, SUM(DISTINCT x), SUM(y)
     * GROUP BY k with two x = 2 rows froze sum(distinct x) as sum(x) and
     * returned 4 instead of 2). A plain aggregate riding along the same stage consumes
     * the lower stage's partial buffer (see localAggParams), never a bare key, so it
     * stays plain.
     *
     * @param childPlan     the input of the final stage
     * @param ownGroupKeys  the final stage's own group keys (never dedup keys)
     * @return the dedup key ExprIds (empty when no eliminated stage groups by an extra key)
     */
    private static Set<ExprId> dedupKeysBelow(Plan childPlan, Set<ExprId> ownGroupKeys) {
        Set<ExprId> keys = new HashSet<>();
        Plan node = childPlan;
        while (node instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<? extends Plan> stage = (PhysicalHashAggregate<? extends Plan>) node;
            if (!stage.getAggPhase().isLocal() && !isIntermediateAggStage(stage)) {
                break;
            }
            for (Expression group : stage.getGroupByExpressions()) {
                collectAllSlotIds(group, keys);
            }
            node = stage.child(0);
        }
        keys.removeAll(ownGroupKeys);
        return keys;
    }

    /**
     * Whether one aggregate of a DISTINCT_GLOBAL stage consumes a bare (non-buffer) dedup
     * key - the direct-key form of a DISTINCT aggregate whose isDistinct was cleared (see
     * dedupKeysBelow). Arguments that resolve to a LOWER partial buffer are the
     * merge chain of a riding-along / lower-stage aggregate and are never dedup keys.
     */
    private boolean consumesDedupKey(List<Expression> args, Set<ExprId> dedupKeyIds) {
        if (dedupKeyIds.isEmpty()) {
            return false;
        }
        for (Expression arg : args) {
            if (arg instanceof SlotReference
                    && dedupKeyIds.contains(((SlotReference) arg).getExprId())
                    && !localAggParams.containsKey(((SlotReference) arg).getExprId())) {
                return true;
            }
            if (arg instanceof AggregateExpression) {
                for (Expression child : ((AggregateExpression) arg).children()) {
                    if (child instanceof SlotReference
                            && dedupKeyIds.contains(((SlotReference) child).getExprId())
                            && !localAggParams.containsKey(((SlotReference) child).getExprId())) {
                        return true;
                    }
                }
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
     * c_N) so upper layers reference the aggregate output column without
     * re-printing the function.
     */
    private void appendAggSelect(SQLRelation relation, SQLRelation child, NamedExpression output,
            List<Pair<ExprId, String>> selects, boolean distinctMergeCount,
            boolean stageHasDistinctMergeBuffer, Set<ExprId> dedupKeyIds) {
        Expression inner = output instanceof Alias ? ((Alias) output).child() : output;
        if (inner instanceof SlotReference
                && (isSystemColumnName(((SlotReference) inner).getName())
                || isRollupGroupingIdMarker((SlotReference) inner, child))) {
            // GROUPING_ID / rowid group-by marker: execution detail of ROLLUP, never a
            // user-visible aggregate output column. A user column of that NAME is
            // exported by the child relation and stays (see isRollupGroupingIdMarker)
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
                String alias = stableRef(output, scopeNames(child, relation));
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
                String groupConcatRef = stableRef(output, scopeNames(child, relation));
                if (groupConcatRef.equalsIgnoreCase(fn.getName())) {
                    groupConcatRef = generatedColumnName(output.getExprId(),
                            scopeNames(child, relation));
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
                if (isNestedNoArgCount(resolved, child)) {
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
            // Keep the USER'S aggregate name as-is: the physical plan never renames a
            // function to partial_* (that prefix exists only in the RENDERED text of a
            // buffer stage - see isPartialAggregate), so the historical "strip the
            // partial_ prefix" rewrite could only ever damage a user function whose own
            // name carries it (partial_myagg -> myagg). A UDAF keeps its DATABASE
            // QUALIFIER (JavaUdaf / PythonUdaf carry dbName): freezing db1.f(v) as f(v)
            // let FunctionRegistry resolve the replay under ANOTHER default database to
            // a same-signature db2.f with a different implementation (the UDF
            // fingerprint cannot tell them apart).
            String aggName = SPMExprSqlBuilder.functionName(fn);
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
                    distinct = isDistinctMergeArg(bufferArgs) || isDistinctMergeArg(args);
                } else {
                    // No buffer of the stage is marked (distinct plans built by other
                    // shapes): keep the historical blanket behavior for count and never
                    // invent DISTINCT for the other aggregates.
                    distinct = "count".equalsIgnoreCase(aggName);
                }
                // The GROUPED distinct shape (... SUM(DISTINCT x), SUM(y) GROUP BY
                // k): the split's dedup stage groups by (k, x) and the final SUM consumes
                // the key x DIRECTLY with no lower buffer) leaves no buffer provenance at
                // all: the dedup key below the stage is the only evidence (see
                // dedupKeysBelow). A riding-along aggregate consumes a lower partial
                // buffer, never a bare key, so it stays plain.
                distinct = distinct || consumesDedupKey(args, dedupKeyIds)
                        || consumesDedupKey(bufferArgs, dedupKeyIds);
            } else {
                distinct = false;
            }
            String distinctSql = distinct ? "DISTINCT " : "";
            if ("count".equalsIgnoreCase(aggName) && starCountArg && argSqls.isEmpty()) {
                sql = aggName + "(" + distinctSql + "*)";
            } else {
                sql = aggName + "(" + distinctSql + String.join(", ", argSqls) + ")";
            }
            ref = stableRef(output, scopeNames(child, relation));
            if (ref.equalsIgnoreCase(aggName)) {
                // The alias was generated by Doris from the function name (no user
                // alias, e.g. an execution-added pre-aggregate with several "sum"
                // columns). Keeping "sum" would decompile many identical aggregate
                // columns whose bare references are ambiguous on re-parse ("sum is
                // ambiguous: sum#23, sum#24, ..."). Emit a unique c_<seq> instead.
                ref = generatedColumnName(output.getExprId(), scopeNames(child, relation));
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
     * A clean user alias (e.g. "sum_qty"), otherwise a generated c_N that is
     * unique within the decompiled SQL (see generatedColumnName).
     */
    private String stableRef(NamedExpression namedExpression) {
        return stableRef(namedExpression, null);
    }

    /**
     * Same as stableRef(NamedExpression) with the in-scope exported names the
     * generated fallback must avoid (a preserved source column named c_1).
     */
    private String stableRef(NamedExpression namedExpression, Collection<String> scope) {
        if (namedExpression instanceof Alias) {
            String name = ((Alias) namedExpression).getName();
            if (name != null && name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                // reserve the user alias too: a later generated c_N must not shadow it
                reservedOutputNames.add(normalizeIdentifier(name));
                // A RESERVED word (from, select, ...) is a legal QUOTED identifier but not
                // an unquoted one: freezing "sum(v) AS from" produced text the parser
                // rejects, and a persisted frozen row has no plan-tree fallback - the
                // baseline could never replay. Keep the user's label quoted instead.
                return NereidsParser.isValidUnquotedIdentifier(name) ? name : quoteIdentifier(name);
            }
        }
        return generatedColumnName(namedExpression.getExprId(), scope);
    }

    /**
     * Assigns (once per ExprId) a compact alias for a generated output column:
     * c_N where N grows over the current decompile. Memoized by the
     * ExprId so later references to the same column reuse the alias. The sequence is
     * reset at the start of every toSQL(Plan) call, so the produced SQL only
     * needs the aliases to be unique within itself and the numbers stay small and
     * readable regardless of the analyzer's ExprId values.
     */
    private String generatedColumnName(ExprId exprId) {
        return generatedColumnName(exprId, null);
    }

    /**
     * Same as generatedColumnName(ExprId) with the in-scope exported names of
     * the target relation: candidates colliding with a preserved source column / user
     * alias are skipped (see reservedOutputNames).
     */
    private String generatedColumnName(ExprId exprId, Collection<String> scope) {
        String existing = generatedColumnNames.get(exprId);
        if (existing != null) {
            return existing;
        }
        reserveOutputNames(scope);
        String candidate;
        do {
            candidate = "c_" + (++generatedColumnSeq);
        } while (reservedOutputNames.contains(normalizeIdentifier(candidate)));
        reservedOutputNames.add(normalizeIdentifier(candidate));
        generatedColumnNames.put(exprId, candidate);
        return candidate;
    }

    /** Reserves the (normalized) exported names of the given relations. */
    private void reserveOutputNames(Collection<String> names) {
        if (names == null) {
            return;
        }
        for (String name : names) {
            if (name != null && !name.isEmpty()) {
                reservedOutputNames.add(normalizeIdentifier(name));
            }
        }
    }

    /** In-scope exported names of the given relations (a generated c_N must avoid them). */
    private static Collection<String> scopeNames(SQLRelation... relations) {
        List<String> names = new ArrayList<>();
        for (SQLRelation relation : relations) {
            if (relation != null) {
                names.addAll(relation.getColumnNames().values());
            }
        }
        return names;
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
        Plan innerTopNNode = skipPassThrough(childNode);
        if (innerTopNNode instanceof PhysicalTopN) {
            PhysicalTopN<?> innerTopN = (PhysicalTopN<?>) innerTopNNode;
            String orderBySql = renderOrderKeys(topN, child);
            if (isSameTopNContinuation(topN, innerTopN, child, orderBySql)) {
                child.setOrderBy(orderBySql);
                child.setLimit(topN.getOffset() > 0 ? topN.getOffset() + ", " : "");
                if (topN.getLimit() != Long.MAX_VALUE) {
                    child.setLimit(child.getLimit() + topN.getLimit());
                }
                return child;
            }
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

    /** Renders one TopN's ORDER BY list against the given relation. */
    private String renderOrderKeys(PhysicalTopN<? extends Plan> topN, SQLRelation relation) {
        return topN.getOrderKeys().stream()
                .map(k -> exprSqlBuilder.print(k.getExpr(), relation)
                        + (k.isAsc() ? " ASC" : " DESC")
                        + (k.isNullFirst() ? " NULLS FIRST" : " NULLS LAST"))
                .collect(Collectors.joining(", "));
    }

    /**
     * Whether outer is the merge stage of the same distributed TopN as
     * inner (the local stage), so that its ORDER BY / LIMIT may be written onto
     * the same relation. Nereids builds MERGE(limit=L, offset=O) -> [Distribute] ->
     * LOCAL(limit=L+O, offset=0), so the local stage must have kept EXACTLY L+O rows:
     * the old inner.limit <= outer.limit test was false for every positive
     * offset (the pair was then serialized as a semantic inner LIMIT L+O and outer
     * LIMIT L OFFSET O, and a later replay - SPM matching ignores the top-level values -
     * stayed capped at L+O inputs), and it was true for unrelated SEMANTIC pairs
     * (inner LIMIT 2, outer LIMIT 5), whose fold would return 5 rows instead of 2. Only
     * the exact identity holds. Both stages must also carry IDENTICAL sort keys (checked
     * on the rendered text, since the merge stage sorts by the local stage's output
     * slots).
     */
    private static boolean isSameTopNContinuation(PhysicalTopN<?> outer, PhysicalTopN<?> inner,
            SQLRelation child, String outerOrderBySql) {
        return inner.getSortPhase().isLocal()
                && (outer.getSortPhase().isMerge() || outer.getSortPhase().isGather())
                && inner.getOffset() == 0
                && outerOrderBySql.equals(child.getOrderBy())
                && isExactTopNContinuationLimit(outer.getLimit(), outer.getOffset(),
                        inner.getLimit());
    }

    /**
     * Overflow-safe identity of a distributed TopN continuation:
     * inner.limit == outer.limit + outer.offset. The sum must not be computed
     * when it would overflow - an unlimited stage carries Long.MAX_VALUE, and any
     * addition to it wraps negative and could match a garbage local limit.
     *
     * @param outerLimit  the merge stage's limit
     * @param outerOffset the merge stage's offset
     * @param innerLimit  the local stage's limit
     * @return whether the local stage kept exactly the outer stage's demanded slice
     */
    @VisibleForTesting
    public static boolean isExactTopNContinuationLimit(long outerLimit, long outerOffset, long innerLimit) {
        if (outerLimit == Long.MAX_VALUE) {
            // an unlimited merge stage keeps the local stage unlimited as well; a positive
            // OFFSET would demand MAX_VALUE+offset rows locally, which cannot be
            // represented - do not fold (conservative)
            return outerOffset == 0 && innerLimit == Long.MAX_VALUE;
        }
        if (outerOffset > Long.MAX_VALUE - outerLimit) {
            return false;
        }
        return innerLimit == outerLimit + outerOffset;
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
     *
     * Two-phase LIMIT (SplitLimit: GLOBAL(l, o) -> LOCAL(l + o, 0)) is COLLAPSED into
     * one semantic block: the pair encodes exactly one user LIMIT ... OFFSET, and
     * serializing both phases as query blocks freezes an inner and an outer LIMIT. The
     * rewrite-time LIMIT merge only reaches the node with a user-tree counterpart (the
     * outer one), so a captured order-free LIMIT 10 kept returning 10 rows when a
     * matching user query asked for LIMIT 20 with an offset. The local phase is a pure
     * execution detail (it only bounds how many rows the local side must produce), so
     * dropping it while keeping the global (limit, offset) preserves the semantics.
     */
    @Override
    public SQLRelation visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, Void context) {
        SQLRelation child;
        if (limit.isGlobal() && limit.child(0) instanceof PhysicalLimit
                && ((PhysicalLimit<?>) limit.child(0)).getPhase() == LimitPhase.LOCAL
                && ((PhysicalLimit<?>) limit.child(0)).getOffset() == 0) {
            // collapse the pair: process the LOCAL node's child and emit ONE block with
            // the global (limit, offset)
            child = process(limit.child(0).child(0));
        } else {
            child = process(limit.child(0));
        }
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
        // A top-level ORDER BY / LIMIT pair must not end up INSIDE the derived table of this
        // projection. NormalizeSort makes "SELECT a FROM t ORDER BY b + 1" a (final) Project
        // over Sort, so inlining the child relation - which carries the clause as its own
        // query block and is therefore wrapped by toRelationSQL() - buried the user's
        // ORDER BY one level down: the replay planner is free to drop an ORDER BY that is
        // not paired with a LIMIT, and for "ORDER BY ... LIMIT 20" the buried cap also hid
        // the Limit from mergeLimitNode (the frozen root was a Project, so a matching
        // LIMIT 20 could not replace the captured value and the query kept the old cap).
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
        dedupeSelectOutputNames(selects, relation);
        // A top-level ORDER BY / LIMIT pair must not end up INSIDE the derived table of
        // this projection. NormalizeSort makes "SELECT a FROM t ORDER BY b + 1" a (final)
        // Project over Sort, so inlining the child relation - which carries the clause as
        // its own query block and is therefore wrapped by toRelationSQL() - buried the
        // user's ORDER BY one level down: the replay planner is free to drop an ORDER BY
        // that is not paired with a LIMIT, and for "ORDER BY ... LIMIT 20" the buried cap
        // also hid the Limit from mergeLimitNode (the frozen root was a Project, so a
        // matching LIMIT 20 could not replace the captured value and the query kept the
        // old cap). This projection is row-wise with a 1:1 output relation, so keeping
        // the pair on the OUTER query is semantically identical and preserves the user's
        // clauses at the root. It runs AFTER the SELECT list is composed: the hoist must
        // dodge any reference the wrapper's own output aliases would capture, and that
        // needs their final names (see hoistableOrderBy).
        String childOrderBy = child.getOrderBy();
        String childLimit = child.getLimit();
        if (!childOrderBy.isEmpty() || !childLimit.isEmpty()) {
            String hoisted = childOrderBy.isEmpty() ? ""
                    : hoistableOrderBy(child, childOrderBy, selects, embedsAsSubquery(child));
            if (hoisted != null) {
                child.setOrderBy("");
                child.setLimit("");
                relation.setOrderBy(hoisted);
                relation.setLimit(childLimit);
            }
            // else: a hidden key cannot be re-exported / the wrapper would shadow it -
            // the clause keeps its own block inside the child, where it stays resolvable.
            // (The pair stays together either way: a limit without its order picks
            // arbitrary rows.)
        }
        relation.setFrom(child.toRelationSQL());
        relation.setSelects(selects);
        relation.newAlias();
        return relation;
    }

    /**
     * Two SELECT items can claim the SAME output name: a pass-through column kept for a
     * still-referenced intermediate slot next to a projection alias that derives from it
     * (e.g. the intermediate "profit" column and the branch's final
     * "(profit - profit_loss) AS profit"). The derived table would then export two
     * "profit" columns and EVERY upper reference ("sum(profit)") resolves ambiguously
     * at replay - the frozen plan fails analysis. The explicit alias belongs to the
     * FINAL output (upper layers resolve against it), so the pass-through item is
     * renamed to its unique c_ reference and re-registered here; upper references use
     * the registered name and stay consistent.
     *
     * @param selects  the projection list of the relation (mutated in place)
     * @param relation the relation being built (its column registry is updated)
     */
    private static void dedupeSelectOutputNames(List<Pair<ExprId, String>> selects,
            SQLRelation relation) {
        // Names compare CASE-INSENSITIVELY: a derived table that exported both `x` and `X`
        // for two distinct ExprIds made every upper reference ambiguous after reload
        // (Doris resolves column names case-insensitively), so the pass-through item must
        // be renamed whenever the shared name collides in ANY case.
        Set<String> usedNames = new HashSet<>();
        for (Pair<ExprId, String> select : selects) {
            usedNames.add(selectOutputName(select.value()).toLowerCase(java.util.Locale.ROOT));
        }
        Map<String, Integer> firstOwner = new HashMap<>();
        for (int i = 0; i < selects.size(); i++) {
            Pair<ExprId, String> select = selects.get(i);
            String name = selectOutputName(select.value()).toLowerCase(java.util.Locale.ROOT);
            Integer previous = firstOwner.putIfAbsent(name, i);
            if (previous == null) {
                continue;
            }
            // keep the LATER item (the explicit projection alias upper layers resolve
            // against) under the shared name; re-alias the EARLIER pass-through reference
            // to a fresh unique output name. The pass-through value stays the child-side
            // reference, only its EXPORTED name changes, so the upper references of that
            // intermediate slot (its registered name is updated here) stay resolvable.
            Pair<ExprId, String> first = selects.get(previous);
            // Works for COMPUTED items too: "k + 1 AS x" and "v + 1 AS x" for two
            // distinct ExprIds (a derived table with ORDER BY ... LIMIT consumed by an
            // upper star/join) both exported x, and a parent selecting x, x from the
            // two x columns failed as ambiguous after reload. The earlier item keeps
            // its expression (a previous alias is stripped first) and gains a unique
            // exported name; the later item keeps the shared name upper layers use.
            String value = first.value();
            int asIdx = topLevelAsIndex(value);
            String core = asIdx >= 0 ? value.substring(0, asIdx).trim() : value;
            if (core.isEmpty()) {
                continue;
            }
            String unique = "c_" + first.key();
            while (usedNames.contains(unique.toLowerCase(java.util.Locale.ROOT))) {
                unique = unique + "_";
            }
            usedNames.add(unique.toLowerCase(java.util.Locale.ROOT));
            selects.set(previous, Pair.of(first.key(), core + " AS " + unique));
            relation.registerRef(first.key(), unique);
        }
    }

    /**
     * The output name of one SELECT item: the alias of a TOP-LEVEL " AS " token, or the
     * item itself. " AS " inside a backtick-quoted identifier, inside a single-quoted
     * literal or inside nested parentheses is NOT the alias separator: a legal column
     * named `a as b` exported by an inner layer was read as "b`" by the naive
     * lastIndexOf, and the next frozen layer referenced an identifier that no longer
     * parses.
     */
    private static String selectOutputName(String item) {
        int asIdx = topLevelAsIndex(item);
        String name = asIdx >= 0 ? item.substring(asIdx + 4).trim() : item;
        return name.replace("`", "");
    }

    /**
     * Index of the LAST top-level " AS " token of one SELECT item, or -1: outside
     * backticks / single-quoted literals and at parenthesis depth zero.
     */
    private static int topLevelAsIndex(String item) {
        String lower = item.toLowerCase(java.util.Locale.ROOT);
        int depth = 0;
        int found = -1;
        boolean backtick = false;
        boolean quote = false;
        for (int i = 0; i < item.length(); i++) {
            char c = item.charAt(i);
            if (backtick) {
                if (c == '`') {
                    backtick = false;
                }
                continue;
            }
            if (quote) {
                if (c == '\'') {
                    if (i + 1 < item.length() && item.charAt(i + 1) == '\'') {
                        i++; // '' escape inside a literal
                    } else {
                        quote = false;
                    }
                }
                continue;
            }
            if (c == '`') {
                backtick = true;
            } else if (c == '\'') {
                quote = true;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth = Math.max(0, depth - 1);
            } else if (depth == 0 && c == ' ' && lower.startsWith(" as ", i)) {
                found = i;
                i += 3;
            }
        }
        return found;
    }

    /**
     * Whether the child relation is embedded as a (SELECT ...) subquery (toRelationSQL).
     */
    private static boolean embedsAsSubquery(SQLRelation child) {
        if (child.getRelationName() == null) {
            return child.getFrom().isEmpty() || child.hasOwnBlock();
        }
        return !(child.fromCarriesAlias() && !child.hasOwnBlock());
    }

    /**
     * Prepares the child's ORDER BY for a move onto the wrapper, or refuses the hoist.
     *
     * The clause text was rendered against the CHILD scope; moving it up must keep every
     * reference resolvable (a fully QUOTED column name `a b` is a
     * perfectly resolvable reference and must not force the clause to stay buried inside a
     * derived table, where a no-LIMIT sort is re-planned as a droppable hint and the replay
     * returns unordered rows) and must keep every reference bound to the SAME column:
     *
     *   a reference the child's SELECT list does not export is exported from the
     *       child, keeping its own name;
     *   a reference whose name the child exports for a DIFFERENT expression, or that a
     *       wrapper output alias shadows with a different expression, is exported under a
     *       FRESH name and rewritten in the clause: an ORDER BY b
     *       hoisted above SELECT a AS b would otherwise bind to that alias - the
     *       original sorts by the base column b, the replay by a, and the two rows differ.
     *       The normalised comparison keeps an already-exported quoted key
     *       (a-b) from being appended a second time, which exposed two identical
     *       columns and made every outer reference ambiguous.
     *
     * @param child          the relation whose ORDER BY is hoisted
     * @param orderBy        the rendered ORDER BY text
     * @param wrapperOutputs the wrapper's SELECT items (their output names shadow the
     *                       child's exports at the outer level), or null when unknown
     * @param canAugment     whether the child's SELECT list can still receive an export
     *                       (the child renders as its own subquery block)
     * @return the clause text to place on the wrapper (possibly rewritten), or null when
     *         the clause cannot be hoisted safely - the caller keeps it inside the child
     *         then, where its own query block resolves every reference
     */
    private String hoistableOrderBy(SQLRelation child, String orderBy,
            List<Pair<ExprId, String>> wrapperOutputs, boolean canAugment) {
        if (orderBy.isEmpty()) {
            return "";
        }
        boolean starChild = child.getSelects() == null || child.getSelects().isEmpty();
        Map<String, ExprId> exported = new HashMap<>();
        if (!starChild) {
            for (Pair<ExprId, String> select : child.getSelects()) {
                String outputName = selectOutputName(select.value());
                if (outputName != null && !outputName.isEmpty()) {
                    exported.putIfAbsent(normalizeOutputName(outputName), select.key());
                }
            }
        }
        Map<String, ExprId> shadowed = new HashMap<>();
        if (wrapperOutputs != null) {
            for (Pair<ExprId, String> select : wrapperOutputs) {
                String outputName = selectOutputName(select.value());
                if (outputName != null && !outputName.isEmpty()) {
                    shadowed.putIfAbsent(normalizeOutputName(outputName), select.key());
                }
            }
        }
        String rewritten = orderBy;
        List<Pair<ExprId, String>> additions = new ArrayList<>();
        Set<String> handled = new HashSet<>();
        for (Map.Entry<ExprId, String> entry : child.getColumnNames().entrySet()) {
            String name = entry.getValue();
            if (name == null || !referencesName(orderBy, name)) {
                continue;
            }
            if (!isPassThroughReference(name)) {
                // a qualified / expression reference only the child scope resolves
                LOG.info("SPM order-by hoist refused: '{}' references '{}' (non pass-through name)",
                        orderBy, name);
                return null;
            }
            String normalized = normalizeOutputName(name);
            if (!handled.add(normalized)) {
                continue;
            }
            ExprId shadowOwner = shadowed.get(normalized);
            if (shadowOwner != null && !shadowOwner.equals(entry.getKey())) {
                // the wrapper's own output alias would capture the reference: dodge it
                if (starChild || !canAugment) {
                    LOG.info("SPM order-by hoist refused: '{}' name '{}' shadowed by the wrapper and"
                            + " nothing can be re-exported", orderBy, name);
                    return null; // nothing to re-export the column from
                }
                String fresh = freshExportName(child, orderBy, exported, shadowed);
                additions.add(Pair.of(entry.getKey(), name + " AS " + fresh));
                exported.put(normalizeOutputName(fresh), entry.getKey());
                reExportedLabels.put(fresh, entry.getKey());
                rewritten = replaceStandaloneReference(rewritten, name, fresh);
                continue;
            }
            if (starChild) {
                // a SELECT * relation exports every registered column under its own name
                continue;
            }
            ExprId owner = exported.get(normalized);
            if (entry.getKey().equals(owner)) {
                continue; // already resolvable as the same column
            }
            if (owner == null) {
                // not exported yet: export it under its own name
                if (!canAugment) {
                    LOG.info("SPM order-by hoist refused: '{}' name '{}' not exported by the child"
                            + " and the child cannot be augmented", orderBy, name);
                    return null;
                }
                additions.add(Pair.of(entry.getKey(), name));
                exported.put(normalized, entry.getKey());
                continue;
            }
            // the child exports this NAME for a DIFFERENT expression: the child scope
            // itself already shadows the reference - re-point it at a fresh export
            if (!canAugment) {
                LOG.info("SPM order-by hoist refused: '{}' name '{}' belongs to a different"
                        + " expression in the child scope and the child cannot be augmented",
                        orderBy, name);
                return null;
            }
            String fresh = freshExportName(child, orderBy, exported, shadowed);
            additions.add(Pair.of(entry.getKey(), name + " AS " + fresh));
            exported.put(normalizeOutputName(fresh), entry.getKey());
            reExportedLabels.put(fresh, entry.getKey());
            rewritten = replaceStandaloneReference(rewritten, name, fresh);
        }
        // A clause the previous hoist steps already REWROTE carries output names ("c_N"
        // re-export labels) that no child-map entry mentions, so the loop above never
        // visits them: hoisting one level further past the scope that owns them froze an
        // unresolvable reference (tpcds q78: "Unknown column 'c_14' ... in 'SORT'").
        // Every generated name the rewritten clause references must be exported by the
        // scope the clause lands in - the child below it, or the wrapper's own outputs.
        // A label a previous hoist step created one level below resolved against the
        // child's own scope there, so the child can re-export it with a bare item;
        // without that the clause could only stay buried inside the child's block, and
        // a buried ORDER BY / LIMIT is dropped by the replay planner (the baseline then
        // fails the caller's ORDER BY / LIMIT contract).
        for (String referenced : generatedNameReferences(rewritten)) {
            String normalized = normalizeOutputName(referenced);
            boolean resolvable;
            if (starChild) {
                // "SELECT *" exports every registered column under its own name
                resolvable = normalizedIn(child.getColumnNames().values(), normalized);
            } else {
                resolvable = exported.containsKey(normalized)
                        || wrapperExportsReference(wrapperOutputs, normalized);
            }
            if (resolvable) {
                continue;
            }
            ExprId owner = starChild ? null : reExportedLabels.get(referenced);
            if (owner == null) {
                LOG.info("SPM order-by hoist refused: '{}' keeps referencing '{}' which the"
                        + " wrapper scope does not re-export", orderBy, referenced);
                return null;
            }
            additions.add(Pair.of(owner, referenced));
            exported.put(normalized, owner);
        }
        if (!additions.isEmpty()) {
            child.getSelects().addAll(additions);
        }
        return rewritten;
    }

    /** Generated "c_N" output names referenced as standalone tokens in a clause. */
    private static List<String> generatedNameReferences(String clause) {
        List<String> names = new ArrayList<>();
        Matcher matcher = GENERATED_NAME_PATTERN.matcher(clause);
        while (matcher.find()) {
            names.add(matcher.group());
        }
        return names;
    }

    /** Whether the wrapper's own SELECT list exports the given normalized name. */
    private static boolean wrapperExportsReference(List<Pair<ExprId, String>> wrapperOutputs,
            String normalized) {
        if (wrapperOutputs == null) {
            return false;
        }
        for (Pair<ExprId, String> select : wrapperOutputs) {
            String outputName = selectOutputName(select.value());
            if (outputName != null && normalized.equals(normalizeOutputName(outputName))) {
                return true;
            }
        }
        return false;
    }

    private static boolean normalizedIn(Collection<String> names, String normalized) {
        for (String name : names) {
            if (name != null && normalized.equals(normalizeOutputName(name))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether any reference of the clause would be captured by a RE-LABELLED alias of the
     * same query block: the in-place sink rewrite renames an output to the caller label
     * (c_3 AS b), and a same-block ORDER BY b would then bind to that alias
     * instead of the column the clause was rendered against.
     */
    private static boolean orderByShadowedByRelabel(SQLRelation child, String orderBy,
            List<Pair<ExprId, String>> relabelledOutputs) {
        for (Map.Entry<ExprId, String> entry : child.getColumnNames().entrySet()) {
            String name = entry.getValue();
            if (name == null || !referencesName(orderBy, name)) {
                continue;
            }
            for (Pair<ExprId, String> output : relabelledOutputs) {
                String outputName = selectOutputName(output.value());
                if (outputName == null || outputName.isEmpty()) {
                    continue;
                }
                if (normalizeOutputName(outputName).equals(normalizeOutputName(name))
                        && !output.key().equals(entry.getKey())) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Output names compare case-insensitively with backticks stripped (see the aliasing
     * note in hoistableOrderBy). */
    private static String normalizeOutputName(String name) {
        return name.replace("`", "").toLowerCase(java.util.Locale.ROOT);
    }

    /** A fresh output name for a re-pointed ORDER BY key: not already exported, not
     * shadowed, not a registered child column and not present in the clause itself. */
    private static String freshExportName(SQLRelation child, String orderBy,
            Map<String, ExprId> exported, Map<String, ExprId> shadowed) {
        for (long i = 1; ; i++) {
            String candidate = "c_" + i;
            if (exported.containsKey(candidate) || shadowed.containsKey(candidate)
                    || referencesName(orderBy, candidate)
                    || childNamesContain(child, candidate)) {
                continue;
            }
            return candidate;
        }
    }

    private static boolean childNamesContain(SQLRelation child, String normalized) {
        for (String name : child.getColumnNames().values()) {
            if (name != null && normalizeOutputName(name).equals(normalized)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Replaces every STANDALONE occurrence of one column reference in a rendered clause
     * with another identifier, skipping single-quoted literals and other backtick-quoted
     * identifiers: used when a hoisted ORDER BY key must be re-pointed at a fresh export
     * because its own name would bind to a descending output alias of the wrapper
     */
    private static String replaceStandaloneReference(String sql, String name, String replacement) {
        boolean quotedName = name.startsWith("`") && name.endsWith("`") && name.length() >= 2;
        StringBuilder sb = new StringBuilder(sql.length() + 8);
        boolean inString = false;
        boolean inBacktick = false;
        for (int i = 0; i < sql.length();) {
            char c = sql.charAt(i);
            if (inString) {
                sb.append(c);
                if (c == '\'') {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                        sb.append(sql.charAt(i + 1));
                        i += 2;
                        continue;
                    }
                    inString = false;
                }
                i++;
                continue;
            }
            if (inBacktick) {
                sb.append(c);
                if (c == '`') {
                    inBacktick = false;
                }
                i++;
                continue;
            }
            if (c == '\'') {
                inString = true;
                sb.append(c);
                i++;
                continue;
            }
            if (c == '`' && !quotedName) {
                inBacktick = true;
                sb.append(c);
                i++;
                continue;
            }
            if (sql.startsWith(name, i)) {
                char prev = i == 0 ? ' ' : sql.charAt(i - 1);
                int end = i + name.length();
                char next = end >= sql.length() ? ' ' : sql.charAt(end);
                if (!isIdentifierChar(prev) && !isIdentifierChar(next)) {
                    sb.append(replacement);
                    i = end;
                    continue;
                }
            }
            sb.append(c);
            i++;
            if (c == '`') {
                inBacktick = true;
            }
        }
        return sb.toString();
    }

    private static boolean isIdentifierChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    /** Whether an ORDER BY text references the name as a standalone identifier. */
    private static boolean referencesName(String sql, String name) {
        return java.util.regex.Pattern
                .compile("(?<![A-Za-z0-9_`])" + java.util.regex.Pattern.quote(name)
                        + "(?![A-Za-z0-9_])")
                .matcher(sql)
                .find();
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
                int asIdx = topLevelAsIndex(value);
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
                // relations carry their own SELECT list instead). A FULLY QUOTED
                // reference IS pass-through safe: a legal column named "a b" / "a.b"
                // re-parses as ONE name when emitted backtick-quoted, while the raw
                // space / dot test dropped it - the parent then referenced a column the
                // derived table did not export and the frozen SQL failed binding after a
                // reload (e.g. a Window over a quoted column under a parameterized
                // filter).
                if (isPassThroughReference(ref) && !isSystemColumnName(ref)) {
                    selects.add(Pair.of(entry.getKey(), ref));
                }
            }
        }
    }

    /**
     * Whether a registered reference can be re-emitted as a bare SELECT item: a plain
     * identifier, or a fully backtick-quoted reference (quoteIdentifier output - the
     * quoting is what keeps spaces / dots / keywords inside ONE name). Expressions,
     * qualified / literal forms stay excluded.
     */
    private static boolean isPassThroughReference(String ref) {
        if (ref == null || ref.isEmpty()) {
            return false;
        }
        if (ref.startsWith("`") && ref.endsWith("`") && ref.length() >= 2) {
            return true;
        }
        return !ref.contains("(") && !ref.contains(" ") && !ref.contains(".")
                && !ref.contains("'");
    }

    /**
     * Doris internal columns that carry no SQL meaning and must be dropped from every
     * decompiled projection. Mirror of the internal-column naming defined in
     * Column: every hidden execution column is named under the
     * Column.HIDDEN_COLUMN_PREFIX "__DORIS_" family (rowid columns, version /
     * delete-sign / sequence columns, ...), plus the lowercase shadow prefix
     * Column.SHADOW_NAME_PREFIX. Any new internal column added to Column under one of
     * these prefixes is covered automatically.
     *
     * GROUPING_ID is deliberately NOT part of this name family: it is a legal USER
     * column name, and the rollup execution marker is told apart by provenance instead
     * (see isRollupGroupingIdMarker).
     */
    private static boolean isSystemColumnName(String name) {
        return name != null
                && (name.startsWith(Column.HIDDEN_COLUMN_PREFIX)
                || name.startsWith(Column.SHADOW_NAME_PREFIX));
    }

    /**
     * Whether a slot is the ROLLUP execution marker rather than a user column: the marker
     * is a synthetic GROUPING_ID slot (Repeat.COL_GROUPING_ID) that NO relation
     * exports, while a table column of that name is registered by its scan (see
     * visitPhysicalRelation). Skipping the marker by name alone dropped a real
     * column named GROUPING_ID from the frozen child SELECT while the outer projection
     * still referenced it, so the baseline failed to bind
     * ("Unknown column 'GROUPING_ID' in 'table list'") on every replay.
     *
     * @param slot  the slot of a projection / group-by list
     * @param owner the relation that must export the slot when it is a real column
     */
    private static boolean isRollupGroupingIdMarker(SlotReference slot, SQLRelation owner) {
        return "GROUPING_ID".equals(slot.getName())
                && (owner == null || !owner.getColumnNames().containsKey(slot.getExprId()));
    }

    /**
     * Appends one projection SELECT item: a plain column pass-through keeps its (child)
     * reference name; an expression is emitted as "expr AS [name]" where the name is the
     * user alias (clean identifier) or a generated c_N.
     */
    private void appendProjectSelect(SQLRelation relation, SQLRelation child, NamedExpression projectExpr,
            List<Pair<ExprId, String>> selects) {
        Expression inner = projectExpr instanceof Alias ? ((Alias) projectExpr).child() : projectExpr;
        if (inner instanceof SlotReference
                && (isSystemColumnName(((SlotReference) inner).getName())
                || isRollupGroupingIdMarker((SlotReference) inner, child))) {
            // internal system column (e.g. rowid used by some joins) or the ROLLUP
            // GROUPING_ID marker: execution detail, not part of the user query - drop it
            // from the decompiled projection. A USER column named GROUPING_ID is
            // registered by the child relation and stays (see isRollupGroupingIdMarker)
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
        // An input column and a window OUTPUT can share a visible name
        // (SELECT t.x, row_number() OVER (...) AS x FROM t): both would export bare x
        // and the parent's SELECT x, x fails as ambiguous after reload. Unlike the
        // intermediate Project this visitor had no duplicate repair - run the same one
        // here: the earlier pass-through keeps its value under a unique c_ reference,
        // the window output keeps the shared name upper layers resolve against.
        dedupeSelectOutputNames(selects, relation);
        relation.setSelects(selects);
        relation.newAlias();
        return relation;
    }

    // ==================== SetOperation (UNION/EXCEPT/INTERSECT) ====================

    /**
     * PhysicalUnion / PhysicalExcept / PhysicalIntersect: join the child queries with
     * the corresponding operator and the quantifier of the PHYSICAL node (see
     * setOperationKeyword).
     */
    @Override
    public SQLRelation visitPhysicalUnion(PhysicalUnion union, Void context) {
        return visitPhysicalSet(union, setOperationKeyword("UNION", union.getQualifier()), context);
    }

    @Override
    public SQLRelation visitPhysicalExcept(PhysicalExcept except, Void context) {
        return visitPhysicalSet(except, setOperationKeyword("EXCEPT", except.getQualifier()), context);
    }

    @Override
    public SQLRelation visitPhysicalIntersect(PhysicalIntersect intersect, Void context) {
        return visitPhysicalSet(intersect,
                setOperationKeyword("INTERSECT", intersect.getQualifier()), context);
    }

    /**
     * Keyword of a set operation for the frozen SQL. The parser maps an OMITTED
     * quantifier (and an explicit DISTINCT) to Qualifier.DISTINCT, so DISTINCT must be
     * emitted WITHOUT ALL: a DISTINCT union frozen as UNION ALL would return duplicate
     * rows at replay, while dropping ALL from EXCEPT ALL / INTERSECT ALL silently
     * de-duplicates the multiplicity of the branches.
     */
    private static String setOperationKeyword(String keyword, Qualifier qualifier) {
        return qualifier == Qualifier.ALL ? keyword + " ALL" : keyword;
    }

    private SQLRelation visitPhysicalSet(PhysicalSetOperation set, String op, Void context) {
        List<List<SlotReference>> childrenOutputs = set.getRegularChildrenOutputs();
        List<? extends Slot> outputs = set.getOutput();
        // Output names that appear MORE THAN ONCE on the set: the analyzer's default
        // expression-derived names make a rollup union over per-branch aggregates carry
        // several columns LITERALLY named "sum". A bare name is AMBIGUOUS in every
        // downstream reference (sum(sum) over the set hits all of them - the analyzer
        // rejects the replayed plan with "sum is ambiguous"), while the originating SQL
        // could never reference such a column by name either. Such outputs are aliased
        // to their positional reference (c_<output id>) instead; every branch exposes
        // that reference locally, so no further renaming is needed.
        // Case-INSENSITIVE counting: Doris identifiers are case-insensitive, so `a` and
        // `A` are the SAME output name - two raw names each counted once left both
        // branches exporting a / A and the result sink's SELECT a, A ... was rejected
        // as ambiguous after reload. Every duplicate (by normalized name) is aliased to
        // its positional reference instead.
        Map<String, Integer> outputNameCounts = new HashMap<>();
        for (int j = 0; j < outputs.size(); j++) {
            outputNameCounts.merge(normalizedOutputName(outputs.get(j).getName()), 1,
                    Integer::sum);
        }
        // Positional references must be UNIQUE across ALL set outputs: an output already
        // named c_1 (a user column) next to a duplicate whose ExprId is 1 produced
        // (c_1, c_2, c_1) - both branches and the result sink then referenced an AMBIGUOUS
        // c_1 as soon as the frozen SQL was re-parsed. Trailing underscores resolve the
        // collision (same rule as the other dedupe helpers).
        Set<String> usedOutputNames = new HashSet<>();
        for (Slot output : outputs) {
            usedOutputNames.add(normalizedOutputName(output.getName()));
        }
        List<String> registeredOutputNames = new ArrayList<>();
        for (int j = 0; j < outputs.size(); j++) {
            Slot output = outputs.get(j);
            if (outputNameCounts.getOrDefault(normalizedOutputName(output.getName()), 0) > 1) {
                String unique = "c_" + output.getExprId();
                while (usedOutputNames.contains(normalizedOutputName(unique))) {
                    unique = unique + "_";
                }
                usedOutputNames.add(normalizedOutputName(unique));
                registeredOutputNames.add(quoteIdentifier(unique));
            } else {
                registeredOutputNames.add(quoteIdentifier(output.getName()));
            }
        }
        List<String> branchSqls = Lists.newArrayList();
        for (int i = 0; i < set.children().size(); i++) {
            SQLRelation childRelation = process(set.children().get(i));
            List<SlotReference> childOutputs = childrenOutputs.get(i);
            // Project EXACTLY the positional output of this branch (regularChildrenOutputs)
            // under the set's output names: blindly concatenating the child relation where it
            // stands would emit whatever columns that branch happened to produce (e.g. all 36
            // sales columns), so the names the outer SQL references after the set node would
            // never exist in the frozen planSql.
            // NO newAlias() on the branch: its SQL is placed directly on either side of
            // UNION / EXCEPT / INTERSECT, where DorisParser accepts a parenthesized query
            // term but NO trailing alias - "(SELECT ...) t_3" is not a legal set operand
            // (aliases are legal only in relation position, i.e. under FROM). Creation
            // never re-parses the decompiled text, so the invalid fragment was invisible
            // in-memory; a later refresh / restart could not rebuild the persisted frozen
            // baseline at all. The alias is allocated further below, on the COMPLETED set
            // relation, where parent nodes reference it.
            SQLRelation branch = new SQLRelation();
            branch.setFrom(childRelation.toRelationSQL());
            List<Pair<ExprId, String>> selects = new ArrayList<>();
            for (int j = 0; j < childOutputs.size(); j++) {
                SlotReference slot = childOutputs.get(j);
                String columnRef = exprSqlBuilder.print(slot, childRelation);
                String outputName = j < outputs.size() ? outputs.get(j).getName() : slot.getName();
                String item;
                if (j < outputs.size()
                        && outputNameCounts.getOrDefault(normalizedOutputName(outputName), 0) > 1) {
                    // duplicated output name: alias the branch to the UNIQUE positional
                    // reference registered for the set (see above)
                    item = columnRef + " AS " + registeredOutputNames.get(j);
                } else {
                    item = columnRef.equals(outputName)
                            ? columnRef : columnRef + " AS " + quoteIdentifier(outputName);
                }
                selects.add(Pair.of(slot.getExprId(), item));
            }
            branch.setSelects(selects);
            // parenthesized query term: a set operand carrying its own ORDER BY / LIMIT
            // must be grouped so the clause binds to the operand, not to the set chain
            branchSqls.add("(" + branch.toSQL() + ")");
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
                    if (outputNameCounts.getOrDefault(normalizedOutputName(outputName), 0) > 1) {
                        // duplicated output name: use the UNIQUE positional reference the
                        // set registered (the result sink selects THAT name from the
                        // derived union; emitting `AS x` twice left its c_<ExprId>
                        // references pointing at nonexistent columns)
                        item = item + " AS " + registeredOutputNames.get(j);
                    } else if (!item.equals(outputName)) {
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
        // A set operation is a DERIVED TABLE: the alias must sit inside the FROM fragment
        // itself - "SELECT * FROM ((a) UNION (b))" is a parse error (every derived table
        // needs its own alias), while rendering the alias through the generic subquery
        // wrapper would nest the whole set block one level deeper. The alias is
        // registered for column qualification either way.
        String setAlias = setRelation.newAlias();
        setRelation.setFrom("(" + String.join(" " + op + " ", branchSqls) + ") " + setAlias);
        setRelation.markFromCarriesAlias();
        // register the set outputs so upper nodes reference the produced column names
        for (int j = 0; j < outputs.size(); j++) {
            setRelation.registerRef(outputs.get(j).getExprId(), registeredOutputNames.get(j));
        }
        return setRelation;
    }

    /** Normalized key of one set-output name: identifiers are case-insensitive. */
    private static String normalizedOutputName(String name) {
        return name == null ? "" : name.toLowerCase(java.util.Locale.ROOT);
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
     * PhysicalAssertNumRows (the "at most one row" assertion of a scalar-subquery
     * unnest, e.g. the inner side of the INNER join ScalarApplyToJoin builds): render it
     * as the ASSERT_ROWS relation prefix, "ASSERT_ROWS (SELECT ...) t_N". The Nereids
     * grammar accepts that relation and the parser rebuilds the LogicalAssertNumRows on
     * replay, so the frozen text keeps the single-row contract instead of falling back
     * to the user planSql.
     */
    @Override
    public SQLRelation visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows, Void context) {
        SQLRelation relation = process(assertNumRows.child(0));
        relation.setAssertRows(true);
        if (relation.getRelationName() == null) {
            // The parent references this input as "ASSERT_ROWS (...) t_N": an inline
            // fragment (e.g. a bare scan) carries no alias to append to that text.
            relation.newAlias();
        }
        return relation;
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
