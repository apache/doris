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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Records column-level query-hit and filter-hit stats from the Nereids physical plan.
 * Called once per query in NereidsPlanner after plan translation.
 * Stats are recorded at plan time, not execution time; identical cached queries count once.
 */
public class QueryStatsRecorder {
    private static final Logger LOG = LogManager.getLogger(QueryStatsRecorder.class);

    private QueryStatsRecorder() {}

    /** Entry point — no-ops if config gate is off, EXPLAIN, or DML. */
    public static void record(PhysicalPlan plan, StatementContext stmtContext) {
        if (!shouldRecord(stmtContext)) {
            return;
        }
        try {
            Map<ExprId, PhysicalOlapScan> exprIdToScan = new HashMap<>();
            // Key: "catalogId_dbId_tableId_indexId" — one delta per table even on self-joins.
            Map<String, StatsDelta> deltas = new LinkedHashMap<>();
            walkPlan(plan, exprIdToScan, deltas);
            // No OlapScans found — no stats to record (implies deltas is also empty).
            if (exprIdToScan.isEmpty()) {
                return;
            }
            // queryHit: top-level SELECT columns
            for (Slot slot : plan.getOutput()) {
                SlotReference sr = unwrapSlotRef(slot);
                if (sr == null) {
                    continue;
                }
                PhysicalOlapScan sourceScan = exprIdToScan.get(sr.getExprId());
                if (sourceScan == null) {
                    continue;
                }
                StatsDelta delta = getOrCreateDelta(deltas, sourceScan);
                if (delta == null) {
                    continue;
                }
                sr.getOriginalColumn().ifPresent(col -> {
                    if (!col.getName().isEmpty()) {
                        delta.addQueryStats(col.getName());
                    }
                });
            }
            for (StatsDelta delta : deltas.values()) {
                if (!delta.empty()) {
                    Env.getCurrentEnv().getQueryStats().addStats(delta);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to record query stats", e);
        }
    }

    // Package-private to allow unit testing of guard logic.
    static boolean shouldRecord(StatementContext ctx) {
        if (!Config.enable_query_hit_stats) {
            return false;
        }
        StatementBase stmt = ctx.getParsedStatement();
        if (stmt == null || stmt.isExplain()) {
            return false;
        }
        // Skip DML: full-row pass-through projections produce misleading stats.
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
     */
    private static void walkPlan(Plan plan,
            Map<ExprId, PhysicalOlapScan> exprIdToScan,
            Map<String, StatsDelta> deltas) {
        if (plan instanceof PhysicalOlapScan) {
            PhysicalOlapScan scan = (PhysicalOlapScan) plan;
            for (Slot slot : scan.getOutput()) {
                exprIdToScan.put(slot.getExprId(), scan);
            }
            return;
        }
        if (plan instanceof PhysicalDeferMaterializeOlapScan) {
            PhysicalOlapScan inner =
                    ((PhysicalDeferMaterializeOlapScan) plan).getPhysicalOlapScan();
            for (Slot slot : plan.getOutput()) {
                exprIdToScan.put(slot.getExprId(), inner);
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
                    if (delta == null) {
                        return;
                    }
                    sr.getOriginalColumn().ifPresent(col -> {
                        if (!col.getName().isEmpty()) {
                            delta.addFilterStats(col.getName());
                        }
                    });
                });
            }
        }
    }

    /** Unwraps Alias to reach the underlying SlotReference; null for computed expressions. */
    private static SlotReference unwrapSlotRef(Expression expr) {
        if (expr instanceof SlotReference) {
            return (SlotReference) expr;
        }
        if (expr instanceof Alias) {
            return unwrapSlotRef(((Alias) expr).child());
        }
        return null;
    }

    /**
     * Returns or creates a StatsDelta keyed by catalog/db/table/index — safe for self-joins.
     * Returns null if the scan's table or database is unexpectedly unresolved.
     */
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
