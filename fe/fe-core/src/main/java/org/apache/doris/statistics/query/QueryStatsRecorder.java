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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
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
 * Called once per query in NereidsPlanner after plan translation.
 *
 * <p>queryHit: SELECT output columns (alias-unwrapped), GROUP BY keys,
 * ORDER BY keys, window PARTITION BY / ORDER BY keys, aggregate input columns.
 * filterHit: WHERE predicate columns.
 * Only OlapTable scans are recorded. DML, EXPLAIN, and internal queries are skipped.
 * Per query each table's count is incremented at most once regardless of scan count.
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
        Map<String, StatsDelta> deltas = new HashMap<>();
        walkPlan(plan, exprIdToScan, deltas);
        if (exprIdToScan.isEmpty()) {
            return deltas;
        }
        // queryHit: root SELECT output, unwrapping Alias to reach the base column.
        for (Slot slot : plan.getOutput()) {
            SlotReference sr = unwrapAlias(slot);
            if (sr == null) {
                continue;
            }
            PhysicalOlapScan sourceScan = exprIdToScan.get(sr.getExprId());
            if (sourceScan == null) {
                continue;
            }
            StatsDelta delta = getOrCreateDelta(deltas, sourceScan);
            if (delta != null) {
                sr.getOriginalColumn().ifPresent(col -> delta.addQueryStats(col.getName()));
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
     * Single-pass tree walk: registers scan output slots into exprIdToScan,
     * records filterHit for WHERE conjuncts, and records queryHit for
     * GROUP BY / ORDER BY / window keys and aggregate input columns.
     * Children are visited before the current node so scans are registered first.
     * PhysicalLazyMaterializeOlapScan is checked before PhysicalOlapScan
     * because it is a subclass; the inner scan's metadata must be used.
     */
    private static void walkPlan(Plan plan,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<String, StatsDelta> deltas) {
        if (plan instanceof PhysicalStorageLayerAggregate) {
            // COUNT(*)/MIN/MAX pushdown — the aggregate wraps the real scan but has no children.
            PhysicalRelation inner = ((PhysicalStorageLayerAggregate) plan).getRelation();
            if (inner instanceof PhysicalOlapScan) {
                PhysicalOlapScan scan = (PhysicalOlapScan) inner;
                for (Slot slot : scan.getOutput()) {
                    exprIdToScan.put(slot.getExprId(), scan);
                }
            }
            return;
        }
        if (plan instanceof PhysicalLazyMaterializeOlapScan) {
            PhysicalOlapScan inner =
                    ((PhysicalLazyMaterializeOlapScan) plan).getScan();
            for (Slot slot : plan.getOutput()) {
                exprIdToScan.put(slot.getExprId(), inner);
            }
            return;
        }
        if (plan instanceof PhysicalOlapScan) {
            PhysicalOlapScan scan = (PhysicalOlapScan) plan;
            for (Slot slot : scan.getOutput()) {
                exprIdToScan.put(slot.getExprId(), scan);
            }
            return;
        }
        // TODO: PhysicalCTEConsumer slots use consumer-side ExprIds that differ from the producer
        // scan's ExprIds, so CTE column stats are silently missed. Fix requires mapping consumer
        // slots back to producer slots via StatementContext.getConsumerToProducerSlotMap().
        for (Plan child : plan.children()) {
            walkPlan(child, exprIdToScan, deltas);
        }
        if (plan instanceof PhysicalFilter) {
            PhysicalFilter<?> filter = (PhysicalFilter<?>) plan;
            for (Expression conjunct : filter.getConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, deltas);
            }
        }
        // Lazy-materialized columns are not in PhysicalLazyMaterializeOlapScan.getOutput()
        // (only operative slots + row-id are). The parent PhysicalLazyMaterialize exposes
        // the lazy slots via getLazySlots(). Use each row-id — already registered by the
        // child scan branch — to look up the source scan and register the lazy ExprIds.
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
                }
            }
        }
        if (plan instanceof Aggregate) {
            Aggregate<?> agg = (Aggregate<?>) plan;
            // GROUP BY keys
            for (Expression expr : agg.getGroupByExpressions()) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, deltas);
            }
            // Columns consumed by aggregate functions (e.g. k2 in SUM(k2))
            for (NamedExpression expr : agg.getOutputExpressions()) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, deltas);
            }
        }
        if (plan instanceof AbstractPhysicalSort) {
            for (OrderKey orderKey : ((AbstractPhysicalSort<?>) plan).getOrderKeys()) {
                recordInputSlotsAsQueryHit(orderKey.getExpr(), exprIdToScan, deltas);
            }
        }
        // PhysicalPartitionTopN does not extend AbstractPhysicalSort but also has ORDER BY and
        // partition keys (used for row_number() / rank() per partition).
        if (plan instanceof PhysicalPartitionTopN) {
            PhysicalPartitionTopN<?> ptn = (PhysicalPartitionTopN<?>) plan;
            for (Expression partKey : ptn.getPartitionKeys()) {
                recordInputSlotsAsQueryHit(partKey, exprIdToScan, deltas);
            }
            for (OrderKey orderKey : ptn.getOrderKeys()) {
                recordInputSlotsAsQueryHit(orderKey.getExpr(), exprIdToScan, deltas);
            }
        }
        // PhysicalRepeat handles ROLLUP/CUBE: group sets are like GROUP BY keys.
        if (plan instanceof PhysicalRepeat) {
            PhysicalRepeat<?> repeat = (PhysicalRepeat<?>) plan;
            for (List<Expression> groupSet : repeat.getGroupingSets()) {
                for (Expression expr : groupSet) {
                    recordInputSlotsAsQueryHit(expr, exprIdToScan, deltas);
                }
            }
            for (NamedExpression expr : repeat.getOutputExpressions()) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, deltas);
            }
        }
        if (plan instanceof PhysicalWindow) {
            WindowFrameGroup wfg = ((PhysicalWindow<?>) plan).getWindowFrameGroup();
            Set<Expression> partitionKeys = wfg.getPartitionKeys();
            for (Expression expr : partitionKeys) {
                recordInputSlotsAsQueryHit(expr, exprIdToScan, deltas);
            }
            for (OrderExpression orderExpr : wfg.getOrderKeys()) {
                recordInputSlotsAsQueryHit(orderExpr.child(), exprIdToScan, deltas);
            }
            // queryHit for the window function value columns (e.g. k2 in SUM(k2) OVER (...)).
            for (NamedExpression windowAlias : wfg.getGroups()) {
                Expression windowExpr = windowAlias.child(0);
                if (windowExpr instanceof WindowExpression) {
                    recordInputSlotsAsQueryHit(
                            ((WindowExpression) windowExpr).getFunction(), exprIdToScan, deltas);
                }
            }
        }
        // filterHit for JOIN ON conditions (hash equality and non-equality predicates).
        if (plan instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin<?, ?> join = (AbstractPhysicalJoin<?, ?>) plan;
            for (Expression conjunct : join.getHashJoinConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, deltas);
            }
            for (Expression conjunct : join.getOtherJoinConjuncts()) {
                recordInputSlotsAsFilterHit(conjunct, exprIdToScan, deltas);
            }
        }
    }

    private static void recordInputSlotsAsQueryHit(Expression expr,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
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
                sr.getOriginalColumn().ifPresent(col -> delta.addQueryStats(col.getName()));
            }
        }
    }

    private static void recordInputSlotsAsFilterHit(Expression expr,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
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
                sr.getOriginalColumn().ifPresent(col -> delta.addFilterStats(col.getName()));
            }
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
