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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Records column-level query-hit and filter-hit statistics from the Nereids physical plan.
 * Called once per query in NereidsPlanner after plan translation.
 *
 * <p>Scope (Part 1):
 * <ul>
 *   <li>queryHit: base SELECT columns whose ExprId flows straight through to the root
 *       plan's output without rewriting. Columns hidden by an alias, an expression,
 *       or an aggregate function are NOT recorded yet (Part 2).</li>
 *   <li>filterHit: columns referenced in WHERE predicate conjuncts.</li>
 *   <li>Only OlapTable scans are recorded; external tables (Hive, Iceberg, JDBC, …) are not.</li>
 *   <li>DML, EXPLAIN, and internal queries (e.g. auto-analyze) are skipped.</li>
 *   <li>Per query, each table's count is incremented at most once regardless of scan count.</li>
 * </ul>
 * GROUP BY, ORDER BY, window, JOIN, and aliased/projected columns are deferred to Part 2.
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
        for (Slot slot : plan.getOutput()) {
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
     * and records filterHit for PhysicalFilter conjuncts.
     * Children are visited before the current node so scans are registered
     * before parent filters look them up.
     * PhysicalLazyMaterializeOlapScan is checked before PhysicalOlapScan
     * because it is a subclass; the inner scan's metadata must be used.
     */
    private static void walkPlan(Plan plan,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<String, StatsDelta> deltas) {
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
        for (Plan child : plan.children()) {
            walkPlan(child, exprIdToScan, deltas);
        }
        if (plan instanceof PhysicalFilter) {
            PhysicalFilter<?> filter = (PhysicalFilter<?>) plan;
            for (Expression conjunct : filter.getConjuncts()) {
                conjunct.getInputSlots().forEach(slot -> {
                    if (!(slot instanceof SlotReference)) {
                        return;
                    }
                    SlotReference sr = (SlotReference) slot;
                    PhysicalOlapScan sourceScan = exprIdToScan.get(sr.getExprId());
                    if (sourceScan == null) {
                        return;
                    }
                    StatsDelta delta = getOrCreateDelta(deltas, sourceScan);
                    if (delta != null) {
                        sr.getOriginalColumn().ifPresent(col -> delta.addFilterStats(col.getName()));
                    }
                });
            }
        }
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
