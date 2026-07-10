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

package org.apache.doris.statistics.query;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Records column-level query-hit and filter-hit statistics from the Nereids physical plan.
 * Only OlapTable scans are recorded; DML, EXPLAIN, and internal queries are skipped.
 *
 * <p>{@code exprIdToScan} holds only base scan-backed ExprIds; every derived slot (alias,
 * aggregate output, CTE consumer, set-op output) is an edge in {@code derivedSlotInputs}
 * pointing at its own inputs. {@link #resolveBaseScanExprIds} expands that edge map
 * recursively, however many computed hops deep a slot's provenance chain is.
 */
public class QueryStatsRecorder {
    private static final Logger LOG = LogManager.getLogger(QueryStatsRecorder.class);

    private QueryStatsRecorder() {}

    public static void record(PhysicalPlan plan, StatementContext stmtContext) {
        if (!shouldRecord(stmtContext)) {
            return;
        }
        if (stmtContext.isQueryStatsRecorded()) {
            return;
        }
        // Set the latch before the work so a partial-failure retry does not double-count.
        stmtContext.markQueryStatsRecorded();
        try {
            Map<String, StatsDelta> deltas = collectDeltas(plan);
            for (StatsDelta delta : deltas.values()) {
                if (!delta.empty()) {
                    try {
                        Env.getCurrentEnv().getQueryStats().addStats(delta);
                    } catch (Exception e) {
                        ConnectContext cc = stmtContext.getConnectContext();
                        String queryId = (cc != null && cc.queryId() != null)
                                ? cc.queryId().toString() : "unknown";
                        LOG.warn("Failed to record query stats for query={}", queryId, e);
                    }
                }
            }
        } catch (Throwable t) {
            // Best-effort telemetry: catch Errors too (e.g. StackOverflowError), never break the query.
            ConnectContext cc = stmtContext.getConnectContext();
            String queryId = (cc != null && cc.queryId() != null)
                    ? cc.queryId().toString() : "unknown";
            LOG.warn("Failed to build query stats deltas for query={}", queryId, t);
        }
    }

    /**
     * Builds the per-table StatsDelta map from the physical plan.
     * Package-private so unit tests can verify recording logic without touching Env.
     */
    static Map<String, StatsDelta> collectDeltas(PhysicalPlan plan) {
        Map<ExprId, PhysicalOlapScan> exprIdToScan = new HashMap<>();
        Map<ExprId, String> exprIdToColName = new HashMap<>();
        Map<String, StatsDelta> deltas = new HashMap<>();
        // ExprId -> input slots for any derived slot (alias, aggregate output, CTE consumer
        // slot, set-operation output). resolveBaseScanExprIds expands this recursively.
        Map<ExprId, Set<Slot>> derivedSlotInputs = new HashMap<>();
        walkPlan(plan, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
        if (exprIdToScan.isEmpty()) {
            return deltas;
        }
        // queryHit: use getProjects() for PhysicalProject so Alias nodes are visible to unwrapAlias.
        Iterable<? extends NamedExpression> rootExprs = (plan instanceof PhysicalProject)
                ? ((PhysicalProject<?>) plan).getProjects()
                : plan.getOutput();
        for (NamedExpression ne : rootExprs) {
            SlotReference sr = unwrapAlias(ne);
            ExprId lookupId = (sr != null) ? sr.getExprId() : ne.getExprId();
            for (ExprId baseId : resolveBaseScanExprIds(lookupId, exprIdToScan, derivedSlotInputs)) {
                recordExprIdAsQueryHit(baseId, exprIdToScan, exprIdToColName, deltas);
            }
        }
        return deltas;
    }

    // Package-private for testing.
    static boolean shouldRecord(StatementContext ctx) {
        if (!Config.enable_query_hit_stats) {
            return false;
        }
        ConnectContext connectContext = ctx.getConnectContext();
        if (connectContext != null && connectContext.getState().isInternal()) {
            return false;
        }
        StatementBase stmt = ctx.getParsedStatement();
        if (stmt == null || stmt.isExplain()) {
            return false;
        }
        // isInsert guards INSERT INTO … SELECT: parsedStmt may be the SELECT sub-plan,
        // not the INSERT Command, when NereidsPlanner re-enters for execution planning.
        if (ctx.isInsert()) {
            return false;
        }
        if (stmt instanceof LogicalPlanAdapter
                && ((LogicalPlanAdapter) stmt).getLogicalPlan() instanceof Command) {
            return false;
        }
        return true;
    }

    /**
     * Single-pass tree walk: registers scan slots, records filterHit for WHERE/JOIN conditions,
     * and queryHit for GROUP BY / ORDER BY / window keys and aggregate inputs.
     */
    private static void walkPlan(Plan plan,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, String> exprIdToColName,
            Map<String, StatsDelta> deltas,
            Map<ExprId, Set<Slot>> derivedSlotInputs) {
        if (plan instanceof PhysicalStorageLayerAggregate) {
            // COUNT(*)/MIN/MAX pushdown — the aggregate wraps the real scan but has no children.
            PhysicalRelation inner = ((PhysicalStorageLayerAggregate) plan).getRelation();
            if (inner instanceof PhysicalOlapScan) {
                PhysicalOlapScan scan = (PhysicalOlapScan) inner;
                for (Slot slot : scan.getOutput()) {
                    exprIdToScan.put(slot.getExprId(), scan);
                    if (slot instanceof SlotReference) {
                        registerColName(exprIdToColName, slot.getExprId(), (SlotReference) slot);
                    }
                }
            }
            return;
        }
        if (plan instanceof PhysicalLazyMaterializeOlapScan) {
            PhysicalOlapScan inner =
                    ((PhysicalLazyMaterializeOlapScan) plan).getScan();
            for (Slot slot : plan.getOutput()) {
                exprIdToScan.put(slot.getExprId(), inner);
                if (slot instanceof SlotReference) {
                    registerColName(exprIdToColName, slot.getExprId(), (SlotReference) slot);
                }
            }
            return;
        }
        if (plan instanceof PhysicalOlapScan) {
            PhysicalOlapScan scan = (PhysicalOlapScan) plan;
            for (Slot slot : scan.getOutput()) {
                exprIdToScan.put(slot.getExprId(), scan);
                if (slot instanceof SlotReference) {
                    registerColName(exprIdToColName, slot.getExprId(), (SlotReference) slot);
                }
            }
            return;
        }
        // PhysicalCTEProducer: walk its single child so the producer scan is registered
        // before any PhysicalCTEConsumer nodes are encountered during the tree walk.
        // Uses plan.children() for consistency with the rest of walkPlan.
        if (plan instanceof PhysicalCTEProducer) {
            walkPlan(plan.children().get(0),
                    exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            return;
        }
        // PhysicalCTEConsumer: link consumer slots to producer slots; resolveBaseScanExprIds
        // expands through this whether the producer column is direct or itself computed.
        if (plan instanceof PhysicalCTEConsumer) {
            PhysicalCTEConsumer cteConsumer = (PhysicalCTEConsumer) plan;
            for (Slot consumerSlot : cteConsumer.getOutput()) {
                Slot producerSlot = cteConsumer.getProducerSlot(consumerSlot);
                derivedSlotInputs.put(consumerSlot.getExprId(), ImmutableSet.of(producerSlot));
            }
        }
        for (Plan child : plan.children()) {
            walkPlan(child, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
        }
        if (plan instanceof PhysicalFilter) {
            PhysicalFilter<?> filter = (PhysicalFilter<?>) plan;
            for (Expression conjunct : filter.getConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
        }
        // Lazy columns are absent from PhysicalLazyMaterializeOlapScan.getOutput(); the parent
        // PhysicalLazyMaterialize exposes them via getLazySlots(), linked by row-id to the scan.
        if (plan instanceof PhysicalLazyMaterialize) {
            PhysicalLazyMaterialize<?> lazy = (PhysicalLazyMaterialize<?>) plan;
            List<Relation> rels = lazy.getRelations();
            List<Slot> rowIds = lazy.getRowIds();
            for (int i = 0; i < rels.size() && i < rowIds.size(); i++) {
                PhysicalOlapScan sourceScan = exprIdToScan.get(rowIds.get(i).getExprId());
                if (sourceScan == null) {
                    continue;
                }
                for (Slot lazySlot : lazy.getLazySlots(rels.get(i))) {
                    exprIdToScan.put(lazySlot.getExprId(), sourceScan);
                    if (lazySlot instanceof SlotReference) {
                        registerColName(exprIdToColName, lazySlot.getExprId(),
                                (SlotReference) lazySlot);
                    }
                }
            }
        }
        if (plan instanceof Aggregate) {
            Aggregate<?> agg = (Aggregate<?>) plan;
            // GROUP BY keys
            for (Expression expr : agg.getGroupByExpressions()) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
            // Columns consumed by aggregate functions (e.g. k2 in SUM(k2))
            for (NamedExpression expr : agg.getOutputExpressions()) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
            // HAVING: link each non-scan-backed aggregate output to its own inputs, however
            // computed those already are (e.g. HAVING SUM(x) where x is itself computed).
            for (NamedExpression ne : agg.getOutputExpressions()) {
                if (exprIdToScan.containsKey(ne.getExprId()) || ne instanceof Slot) {
                    // Bare pass-through output (e.g. two-phase DISTINCT's merge phase reusing the
                    // local phase's slot) — already linked there; getInputSlots() on a bare Slot
                    // includes itself, which would self-loop here.
                    continue;
                }
                Set<Slot> inputSlots = ne.getInputSlots();
                if (!inputSlots.isEmpty()) {
                    derivedSlotInputs.put(ne.getExprId(), inputSlots);
                }
            }
        }
        if (plan instanceof AbstractPhysicalSort) {
            for (OrderKey orderKey : ((AbstractPhysicalSort<?>) plan).getOrderKeys()) {
                recordInputSlotsAsQueryHit(orderKey.getExpr(), exprIdToScan, exprIdToColName, deltas,
                        derivedSlotInputs);
            }
        }
        // PhysicalPartitionTopN does not extend AbstractPhysicalSort but also has ORDER BY and
        // partition keys (used for row_number() / rank() per partition).
        if (plan instanceof PhysicalPartitionTopN) {
            PhysicalPartitionTopN<?> ptn = (PhysicalPartitionTopN<?>) plan;
            for (Expression partKey : ptn.getPartitionKeys()) {
                recordInputSlotsAsQueryHit(partKey, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
            for (OrderKey orderKey : ptn.getOrderKeys()) {
                recordInputSlotsAsQueryHit(orderKey.getExpr(), exprIdToScan, exprIdToColName, deltas,
                        derivedSlotInputs);
            }
        }
        // PhysicalRepeat handles ROLLUP/CUBE: group sets are like GROUP BY keys.
        if (plan instanceof PhysicalRepeat) {
            PhysicalRepeat<?> repeat = (PhysicalRepeat<?>) plan;
            for (List<Expression> groupSet : repeat.getGroupingSets()) {
                for (Expression expr : groupSet) {
                    recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
                }
            }
            for (NamedExpression expr : repeat.getOutputExpressions()) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
        }
        if (plan instanceof PhysicalWindow) {
            WindowFrameGroup wfg = ((PhysicalWindow<?>) plan).getWindowFrameGroup();
            Set<Expression> partitionKeys = wfg.getPartitionKeys();
            for (Expression expr : partitionKeys) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
            for (OrderExpression orderExpr : wfg.getOrderKeys()) {
                recordInputSlotsAsQueryHit(orderExpr.child(), exprIdToScan, exprIdToColName, deltas,
                        derivedSlotInputs);
            }
            // queryHit for window value columns (e.g. k2 in SUM(k2) OVER (...)), and link the
            // alias to those inputs for a QUALIFY-style filter above; positional functions
            // like ROW_NUMBER have no column arguments, so nothing gets linked for those.
            for (NamedExpression windowAlias : wfg.getGroups()) {
                Expression windowExpr = windowAlias.child(0);
                if (windowExpr instanceof WindowExpression) {
                    Expression function = ((WindowExpression) windowExpr).getFunction();
                    recordInputSlotsAsQueryHit(function, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
                    if (!exprIdToScan.containsKey(windowAlias.getExprId())) {
                        Set<Slot> inputSlots = function.getInputSlots();
                        if (!inputSlots.isEmpty()) {
                            derivedSlotInputs.put(windowAlias.getExprId(), inputSlots);
                        }
                    }
                }
            }
        }
        // filterHit for all JOIN ON conditions; mark conjuncts are a separate field not included
        // in hashJoinConjuncts or otherJoinConjuncts (IN/EXISTS subquery correlation columns).
        if (plan instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin<?, ?> join = (AbstractPhysicalJoin<?, ?>) plan;
            for (Expression conjunct : join.getHashJoinConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
            for (Expression conjunct : join.getOtherJoinConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
            for (Expression conjunct : join.getMarkJoinConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
        }
        // UNION / INTERSECT / EXCEPT: queryHit per branch, plus link the set op's own output
        // slots to those branch slots so a filter kept above the set op (e.g. a volatile
        // predicate PushDownFilterThroughSetOperation can't push down) still resolves.
        if (plan instanceof PhysicalSetOperation) {
            PhysicalSetOperation setOp = (PhysicalSetOperation) plan;
            recordSetOpChildrenOutputs(setOp.getOutput(), setOp.getRegularChildrenOutputs(),
                    exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
        }
        // PhysicalRecursiveUnion extends PhysicalBinary (not PhysicalSetOperation); handle its
        // getRegularChildrenOutputs() explicitly. Recursive-case (WorkTableReference) slots skipped.
        if (plan instanceof PhysicalRecursiveUnion) {
            PhysicalRecursiveUnion<?, ?> recursiveUnion = (PhysicalRecursiveUnion<?, ?>) plan;
            recordSetOpChildrenOutputs(recursiveUnion.getOutput(), recursiveUnion.getRegularChildrenOutputs(),
                    exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
        }
        // LATERAL VIEW / EXPLODE: queryHit for generator inputs; filterHit for the generator's
        // own ON predicate (e.g. table-function join), sent straight to TableFunctionNode.
        if (plan instanceof PhysicalGenerate) {
            PhysicalGenerate<?> generate = (PhysicalGenerate<?>) plan;
            for (Function generator : generate.getGenerators()) {
                recordInputSlotsAsQueryHit(generator, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
            for (Expression conjunct : generate.getConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, exprIdToColName, deltas, derivedSlotInputs);
            }
        }
        // Link alias ExprIds to their input slots so parent output resolves back to the scan.
        if (plan instanceof PhysicalProject) {
            for (NamedExpression ne : ((PhysicalProject<?>) plan).getProjects()) {
                if (exprIdToScan.containsKey(ne.getExprId())) {
                    continue; // plain slot pass-through — already registered by child scan
                }
                SlotReference underlying = unwrapAlias(ne);
                if (underlying != null && !underlying.getExprId().equals(ne.getExprId())) {
                    // Simple alias: Alias(SlotRef) — one input slot, same identity chain.
                    derivedSlotInputs.put(ne.getExprId(), ImmutableSet.of(underlying));
                } else if (underlying == null && ne instanceof Alias) {
                    // Computed alias: Alias(k1+k2), Alias(Cast(k1)) join-key cast, etc.
                    Set<Slot> inputSlots = ((Alias) ne).child().getInputSlots();
                    if (!inputSlots.isEmpty()) {
                        derivedSlotInputs.put(ne.getExprId(), inputSlots);
                    }
                }
            }
        }
    }

    /**
     * Resolves an ExprId down to the base scan-backed ExprIds it derives from, recursing
     * through derivedSlotInputs for any non-scan slot. Shared by every call site.
     */
    private static Set<ExprId> resolveBaseScanExprIds(ExprId exprId,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, Set<Slot>> derivedSlotInputs) {
        return resolveBaseScanExprIds(exprId, exprIdToScan, derivedSlotInputs, new HashSet<>());
    }

    // visiting guards against a cyclic derivedSlotInputs entry causing unbounded recursion.
    private static Set<ExprId> resolveBaseScanExprIds(ExprId exprId,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, Set<Slot>> derivedSlotInputs,
            Set<ExprId> visiting) {
        if (exprIdToScan.containsKey(exprId)) {
            return ImmutableSet.of(exprId);
        }
        if (!visiting.add(exprId)) {
            return ImmutableSet.of();
        }
        Set<Slot> inputSlots = derivedSlotInputs.get(exprId);
        if (inputSlots == null) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<ExprId> result = ImmutableSet.builder();
        for (Slot slot : inputSlots) {
            if (slot instanceof SlotReference) {
                result.addAll(resolveBaseScanExprIds(slot.getExprId(), exprIdToScan, derivedSlotInputs, visiting));
            }
        }
        return result.build();
    }

    private static void recordExprIdAsQueryHit(ExprId baseExprId,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, String> exprIdToColName,
            Map<String, StatsDelta> deltas) {
        PhysicalOlapScan scan = exprIdToScan.get(baseExprId);
        if (scan == null) {
            return;
        }
        StatsDelta delta = getOrCreateDelta(deltas, scan);
        if (delta == null) {
            return;
        }
        String colName = exprIdToColName.get(baseExprId);
        if (colName != null) {
            delta.addQueryStats(colName);
        }
    }

    private static void recordExprIdAsFilterHit(ExprId baseExprId,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, String> exprIdToColName,
            Map<String, StatsDelta> deltas) {
        PhysicalOlapScan scan = exprIdToScan.get(baseExprId);
        if (scan == null) {
            return;
        }
        StatsDelta delta = getOrCreateDelta(deltas, scan);
        if (delta == null) {
            return;
        }
        String colName = exprIdToColName.get(baseExprId);
        if (colName != null) {
            delta.addFilterStats(colName);
        }
    }

    /** Shared by PhysicalSetOperation and PhysicalRecursiveUnion — both expose the same
     *  getOutput()/getRegularChildrenOutputs() contract and need identical recording logic. */
    private static void recordSetOpChildrenOutputs(
            List<Slot> setOpOutput,
            List<List<SlotReference>> childrenOutputs,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, String> exprIdToColName,
            Map<String, StatsDelta> deltas,
            Map<ExprId, Set<Slot>> derivedSlotInputs) {
        for (List<SlotReference> childOutput : childrenOutputs) {
            for (int i = 0; i < childOutput.size(); i++) {
                SlotReference branchSlot = childOutput.get(i);
                for (ExprId baseId : resolveBaseScanExprIds(branchSlot.getExprId(), exprIdToScan,
                        derivedSlotInputs)) {
                    recordExprIdAsQueryHit(baseId, exprIdToScan, exprIdToColName, deltas);
                }
                if (i >= setOpOutput.size()) {
                    continue;
                }
                Slot outputSlot = setOpOutput.get(i);
                if (outputSlot instanceof SlotReference && !exprIdToScan.containsKey(outputSlot.getExprId())
                        && !outputSlot.getExprId().equals(branchSlot.getExprId())) {
                    // Skip self-reuse (output slot IS the branch slot) — would self-loop; the
                    // branch's queryHit is already recorded above regardless.
                    derivedSlotInputs.computeIfAbsent(outputSlot.getExprId(), k -> new HashSet<>())
                            .add(branchSlot);
                }
            }
        }
    }

    private static void recordInputSlotsAsQueryHit(Expression expr,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, String> exprIdToColName,
            Map<String, StatsDelta> deltas,
            Map<ExprId, Set<Slot>> derivedSlotInputs) {
        for (Slot slot : expr.getInputSlots()) {
            if (!(slot instanceof SlotReference)) {
                continue;
            }
            for (ExprId baseId : resolveBaseScanExprIds(slot.getExprId(), exprIdToScan, derivedSlotInputs)) {
                recordExprIdAsQueryHit(baseId, exprIdToScan, exprIdToColName, deltas);
            }
        }
    }

    private static void recordInputSlotsAsFilterHit(Expression expr,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, String> exprIdToColName,
            Map<String, StatsDelta> deltas,
            Map<ExprId, Set<Slot>> derivedSlotInputs) {
        for (Slot slot : expr.getInputSlots()) {
            if (!(slot instanceof SlotReference)) {
                continue;
            }
            for (ExprId baseId : resolveBaseScanExprIds(slot.getExprId(), exprIdToScan, derivedSlotInputs)) {
                recordExprIdAsFilterHit(baseId, exprIdToScan, exprIdToColName, deltas);
            }
        }
    }

    /** Registers a slot's column name into exprIdToColName for later fallback lookup. */
    private static void registerColName(Map<ExprId, String> exprIdToColName,
            ExprId exprId, SlotReference slot) {
        String name = slot.getOriginalColumn().map(col -> col.getName()).orElse(slot.getName());
        if (name != null) {
            exprIdToColName.put(exprId, name);
        }
    }

    /** Unwraps Alias chains to reach the underlying SlotReference; null for computed expressions. */
    static SlotReference unwrapAlias(Expression expr) {
        if (expr instanceof SlotReference) {
            return (SlotReference) expr;
        }
        if (expr instanceof Alias) {
            return unwrapAlias(((Alias) expr).child());
        }
        return null;
    }

    private static StatsDelta getOrCreateDelta(Map<String, StatsDelta> deltas,
            PhysicalOlapScan scan) {
        OlapTable t = scan.getTable();
        DatabaseIf<?> db = scan.getDatabase();
        if (t == null || db == null) {
            return null;
        }
        String key = t.getCatalogId() + "_" + db.getId() + "_" + t.getId()
                + "_" + scan.getSelectedIndexId();
        return deltas.computeIfAbsent(key, k ->
                new StatsDelta(t.getCatalogId(), db.getId(), t.getId(), scan.getSelectedIndexId()));
    }
}
