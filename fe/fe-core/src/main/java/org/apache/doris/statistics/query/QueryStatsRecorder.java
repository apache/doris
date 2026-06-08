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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Records column-level query-hit and filter-hit statistics from the Nereids physical plan.
 * Only OlapTable scans are recorded; DML, EXPLAIN, and internal queries are skipped.
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
        } catch (Exception e) {
            ConnectContext cc = stmtContext.getConnectContext();
            String queryId = (cc != null && cc.queryId() != null)
                    ? cc.queryId().toString() : "unknown";
            LOG.warn("Failed to build query stats deltas for query={}", queryId, e);
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
        // ExprId → input slots for multi-input computed expressions (agg outputs, project aliases).
        // Used to expand filterHit in parent conjuncts and queryHit in the root output loop.
        Map<ExprId, Set<Slot>> aggOutputToInputSlots = new HashMap<>();
        walkPlan(plan, exprIdToScan, exprIdToColName, deltas, aggOutputToInputSlots);
        if (exprIdToScan.isEmpty()) {
            return deltas;
        }
        // queryHit: use getProjects() for PhysicalProject so Alias nodes are visible to unwrapAlias.
        Iterable<? extends NamedExpression> rootExprs = (plan instanceof PhysicalProject)
                ? ((PhysicalProject<?>) plan).getProjects()
                : plan.getOutput();
        for (NamedExpression ne : rootExprs) {
            SlotReference sr = unwrapAlias(ne);
            if (sr != null) {
                PhysicalOlapScan sourceScan = exprIdToScan.get(sr.getExprId());
                if (sourceScan != null) {
                    StatsDelta delta = getOrCreateDelta(deltas, sourceScan);
                    if (delta != null) {
                        String colName = sr.getOriginalColumn().map(col -> col.getName())
                                .orElseGet(() -> exprIdToColName.get(sr.getExprId()));
                        if (colName != null) {
                            delta.addQueryStats(colName);
                        }
                    }
                    continue;
                }
            }
            // Slot from a computed alias, or a complex root alias (Alias(a+b)): expand via map.
            ExprId lookupId = (sr != null) ? sr.getExprId() : ne.getExprId();
            Set<Slot> inputSlots = aggOutputToInputSlots.get(lookupId);
            if (inputSlots == null) {
                continue;
            }
            for (Slot slot : inputSlots) {
                if (!(slot instanceof SlotReference)) {
                    continue;
                }
                SlotReference inputSr = (SlotReference) slot;
                PhysicalOlapScan sourceScan = exprIdToScan.get(inputSr.getExprId());
                if (sourceScan == null) {
                    continue;
                }
                StatsDelta delta = getOrCreateDelta(deltas, sourceScan);
                if (delta != null) {
                    String colName = inputSr.getOriginalColumn().map(col -> col.getName())
                            .orElseGet(() -> exprIdToColName.get(inputSr.getExprId()));
                    if (colName != null) {
                        delta.addQueryStats(colName);
                    }
                }
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
            Map<ExprId, Set<Slot>> aggOutputToInputSlots) {
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
                    exprIdToScan, exprIdToColName, deltas, aggOutputToInputSlots);
            return;
        }
        // PhysicalCTEConsumer: map consumer slots to producer scan slots so parent
        // plan nodes can resolve CTE column references correctly.
        // Mapping is done before the children() loop so it is populated regardless of
        // whether the consumer has children — consistent with how scan handlers work.
        if (plan instanceof PhysicalCTEConsumer) {
            PhysicalCTEConsumer cteConsumer = (PhysicalCTEConsumer) plan;
            for (Slot consumerSlot : cteConsumer.getOutput()) {
                Slot producerSlot = cteConsumer.getProducerSlot(consumerSlot);
                PhysicalOlapScan sourceScan = exprIdToScan.get(producerSlot.getExprId());
                if (sourceScan != null) {
                    exprIdToScan.put(consumerSlot.getExprId(), sourceScan);
                    String colName = exprIdToColName.get(producerSlot.getExprId());
                    if (colName != null) {
                        exprIdToColName.put(consumerSlot.getExprId(), colName);
                    }
                }
            }
        }
        for (Plan child : plan.children()) {
            walkPlan(child, exprIdToScan, exprIdToColName, deltas, aggOutputToInputSlots);
        }
        if (plan instanceof PhysicalFilter) {
            PhysicalFilter<?> filter = (PhysicalFilter<?>) plan;
            for (Expression conjunct : filter.getConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, exprIdToColName, deltas, aggOutputToInputSlots);
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
                recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas);
            }
            // Columns consumed by aggregate functions (e.g. k2 in SUM(k2))
            for (NamedExpression expr : agg.getOutputExpressions()) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas);
            }
            // HAVING: map single-input aggregate ExprIds (SUM(k2)) to the scan for filterHit;
            // multi-input (SUM(k2+k3)) go into aggOutputToInputSlots for expansion by the filter.
            for (NamedExpression ne : agg.getOutputExpressions()) {
                if (exprIdToScan.containsKey(ne.getExprId())) {
                    continue;
                }
                Set<Slot> inputSlots = ne.getInputSlots();
                if (inputSlots.size() == 1) {
                    Slot inputSlot = inputSlots.iterator().next();
                    PhysicalOlapScan scan = exprIdToScan.get(inputSlot.getExprId());
                    if (scan != null) {
                        exprIdToScan.put(ne.getExprId(), scan);
                        String colName = exprIdToColName.get(inputSlot.getExprId());
                        if (colName != null) {
                            exprIdToColName.put(ne.getExprId(), colName);
                        }
                    }
                } else if (inputSlots.size() > 1) {
                    aggOutputToInputSlots.put(ne.getExprId(), inputSlots);
                }
            }
        }
        if (plan instanceof AbstractPhysicalSort) {
            for (OrderKey orderKey : ((AbstractPhysicalSort<?>) plan).getOrderKeys()) {
                recordInputSlotsAsQueryHit(orderKey.getExpr(), exprIdToScan, exprIdToColName, deltas);
            }
        }
        // PhysicalPartitionTopN does not extend AbstractPhysicalSort but also has ORDER BY and
        // partition keys (used for row_number() / rank() per partition).
        if (plan instanceof PhysicalPartitionTopN) {
            PhysicalPartitionTopN<?> ptn = (PhysicalPartitionTopN<?>) plan;
            for (Expression partKey : ptn.getPartitionKeys()) {
                recordInputSlotsAsQueryHit(partKey, exprIdToScan, exprIdToColName, deltas);
            }
            for (OrderKey orderKey : ptn.getOrderKeys()) {
                recordInputSlotsAsQueryHit(orderKey.getExpr(), exprIdToScan, exprIdToColName, deltas);
            }
        }
        // PhysicalRepeat handles ROLLUP/CUBE: group sets are like GROUP BY keys.
        if (plan instanceof PhysicalRepeat) {
            PhysicalRepeat<?> repeat = (PhysicalRepeat<?>) plan;
            for (List<Expression> groupSet : repeat.getGroupingSets()) {
                for (Expression expr : groupSet) {
                    recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas);
                }
            }
            for (NamedExpression expr : repeat.getOutputExpressions()) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas);
            }
        }
        if (plan instanceof PhysicalWindow) {
            WindowFrameGroup wfg = ((PhysicalWindow<?>) plan).getWindowFrameGroup();
            Set<Expression> partitionKeys = wfg.getPartitionKeys();
            for (Expression expr : partitionKeys) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, exprIdToColName, deltas);
            }
            for (OrderExpression orderExpr : wfg.getOrderKeys()) {
                recordInputSlotsAsQueryHit(orderExpr.child(), exprIdToScan, exprIdToColName, deltas);
            }
            // queryHit for the window function value columns (e.g. k2 in SUM(k2) OVER (...)).
            for (NamedExpression windowAlias : wfg.getGroups()) {
                Expression windowExpr = windowAlias.child(0);
                if (windowExpr instanceof WindowExpression) {
                    recordInputSlotsAsQueryHit(
                            ((WindowExpression) windowExpr).getFunction(),
                            exprIdToScan, exprIdToColName, deltas);
                }
            }
        }
        // filterHit for all JOIN ON conditions; mark conjuncts are a separate field not included
        // in hashJoinConjuncts or otherJoinConjuncts (IN/EXISTS subquery semi-join predicates).
        if (plan instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin<?, ?> join = (AbstractPhysicalJoin<?, ?>) plan;
            for (Expression conjunct : join.getHashJoinConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, exprIdToColName, deltas, aggOutputToInputSlots);
            }
            for (Expression conjunct : join.getOtherJoinConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, exprIdToColName, deltas, aggOutputToInputSlots);
            }
            for (Expression conjunct : join.getMarkJoinConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, exprIdToColName, deltas, aggOutputToInputSlots);
            }
        }
        // UNION / INTERSECT / EXCEPT: record queryHit for each child's contributing columns.
        if (plan instanceof PhysicalSetOperation) {
            recordSetOpChildrenOutputs(
                    ((PhysicalSetOperation) plan).getRegularChildrenOutputs(),
                    exprIdToScan, exprIdToColName, deltas);
        }
        // PhysicalRecursiveUnion extends PhysicalBinary (not PhysicalSetOperation); handle its
        // getRegularChildrenOutputs() explicitly. Recursive-case (WorkTableReference) slots skipped.
        if (plan instanceof PhysicalRecursiveUnion) {
            recordSetOpChildrenOutputs(
                    ((PhysicalRecursiveUnion<?, ?>) plan).getRegularChildrenOutputs(),
                    exprIdToScan, exprIdToColName, deltas);
        }
        // LATERAL VIEW / EXPLODE: record queryHit for the generator input columns (e.g. the
        // array column passed to EXPLODE). Generated output slots are synthetic and skipped.
        if (plan instanceof PhysicalGenerate) {
            for (Function generator : ((PhysicalGenerate<?>) plan).getGenerators()) {
                recordInputSlotsAsQueryHit(generator, exprIdToScan, exprIdToColName, deltas);
            }
        }
        // Propagate alias ExprIds for intermediate PhysicalProject nodes so that parent
        // plan output slots (derived from aliases) resolve back to the original scan.
        if (plan instanceof PhysicalProject) {
            for (NamedExpression ne : ((PhysicalProject<?>) plan).getProjects()) {
                if (exprIdToScan.containsKey(ne.getExprId())) {
                    continue; // plain slot pass-through — already registered by child scan
                }
                SlotReference underlying = unwrapAlias(ne);
                if (underlying != null && !underlying.getExprId().equals(ne.getExprId())) {
                    // Simple alias: Alias(SlotRef) — propagate scan and column name.
                    PhysicalOlapScan scan = exprIdToScan.get(underlying.getExprId());
                    if (scan != null) {
                        exprIdToScan.put(ne.getExprId(), scan);
                        String colName = exprIdToColName.get(underlying.getExprId());
                        if (colName != null) {
                            exprIdToColName.put(ne.getExprId(), colName);
                        }
                    }
                } else if (underlying == null && ne instanceof Alias) {
                    // Complex alias (Alias(Cast(k1)) join-key or Alias(k1+k2) computed SELECT):
                    // single-input → propagate ExprId to scan; multi-input → defer to expansion map.
                    Set<Slot> inputSlots = ((Alias) ne).child().getInputSlots();
                    if (inputSlots.size() == 1) {
                        Slot inputSlot = inputSlots.iterator().next();
                        PhysicalOlapScan scan = exprIdToScan.get(inputSlot.getExprId());
                        if (scan != null) {
                            exprIdToScan.put(ne.getExprId(), scan);
                            String colName = exprIdToColName.get(inputSlot.getExprId());
                            if (colName != null) {
                                exprIdToColName.put(ne.getExprId(), colName);
                            }
                        }
                    } else if (inputSlots.size() > 1) {
                        // Defer to avoid misclassifying PushDownExpressionsInHashCondition
                        // join-key projects as queryHit; parent filter/join expands as filterHit,
                        // root output loop expands as queryHit.
                        aggOutputToInputSlots.put(ne.getExprId(), inputSlots);
                    }
                }
            }
        }
    }

    /** Shared by PhysicalSetOperation and PhysicalRecursiveUnion — both expose the same
     *  getRegularChildrenOutputs() contract and need identical queryHit recording logic. */
    private static void recordSetOpChildrenOutputs(
            List<List<SlotReference>> childrenOutputs,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, String> exprIdToColName,
            Map<String, StatsDelta> deltas) {
        for (List<SlotReference> childOutput : childrenOutputs) {
            for (SlotReference sr : childOutput) {
                PhysicalOlapScan sourceScan = exprIdToScan.get(sr.getExprId());
                if (sourceScan == null) {
                    continue;
                }
                StatsDelta delta = getOrCreateDelta(deltas, sourceScan);
                if (delta != null) {
                    String colName = sr.getOriginalColumn().map(col -> col.getName())
                            .orElseGet(() -> exprIdToColName.get(sr.getExprId()));
                    if (colName != null) {
                        delta.addQueryStats(colName);
                    }
                }
            }
        }
    }

    private static void recordInputSlotsAsQueryHit(Expression expr,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, String> exprIdToColName,
            Map<String, StatsDelta> deltas) {
        for (Slot slot : expr.getInputSlots()) {
            if (!(slot instanceof SlotReference)) {
                continue;
            }
            SlotReference sr = (SlotReference) slot;
            PhysicalOlapScan sourceScan = exprIdToScan.get(sr.getExprId());
            if (sourceScan == null) {
                continue;
            }
            StatsDelta delta = getOrCreateDelta(deltas, sourceScan);
            if (delta != null) {
                String colName = sr.getOriginalColumn().map(col -> col.getName())
                        .orElseGet(() -> exprIdToColName.get(sr.getExprId()));
                if (colName != null) {
                    delta.addQueryStats(colName);
                }
            }
        }
    }

    private static void recordInputSlotsAsFilterHit(Expression expr,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<ExprId, String> exprIdToColName,
            Map<String, StatsDelta> deltas,
            Map<ExprId, Set<Slot>> aggOutputToInputSlots) {
        for (Slot slot : expr.getInputSlots()) {
            if (!(slot instanceof SlotReference)) {
                continue;
            }
            SlotReference sr = (SlotReference) slot;
            PhysicalOlapScan sourceScan = exprIdToScan.get(sr.getExprId());
            if (sourceScan != null) {
                StatsDelta delta = getOrCreateDelta(deltas, sourceScan);
                if (delta != null) {
                    String colName = sr.getOriginalColumn().map(col -> col.getName())
                            .orElseGet(() -> exprIdToColName.get(sr.getExprId()));
                    if (colName != null) {
                        delta.addFilterStats(colName);
                    }
                }
            } else {
                // Slot not from a scan — check if it is a multi-input aggregate output
                // (e.g. SUM(k2+k3)) and expand to its contributing input slots.
                Set<Slot> inputSlots = aggOutputToInputSlots.get(sr.getExprId());
                if (inputSlots != null) {
                    for (Slot inputSlot : inputSlots) {
                        if (!(inputSlot instanceof SlotReference)) {
                            continue;
                        }
                        SlotReference inputSr = (SlotReference) inputSlot;
                        PhysicalOlapScan inputScan = exprIdToScan.get(inputSr.getExprId());
                        if (inputScan == null) {
                            continue;
                        }
                        StatsDelta delta = getOrCreateDelta(deltas, inputScan);
                        if (delta != null) {
                            String colName = inputSr.getOriginalColumn()
                                    .map(col -> col.getName())
                                    .orElseGet(() -> exprIdToColName.get(inputSr.getExprId()));
                            if (colName != null) {
                                delta.addFilterStats(colName);
                            }
                        }
                    }
                }
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
