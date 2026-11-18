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

package org.apache.doris.nereids.spm;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.spm.manager.SessionBaselineStore;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializes enabled SESSION baselines for a forwarded statement and rebuilds them in the receiving context. Query
 * statements carry candidates selected by hash and digest; other statements carry all enabled rows. Imported rows
 * are request-context state, not GLOBAL baselines.
 */
public final class SPMForwardedSession {

    /**
     * Character budget for the serialized baseline payload. SessionBaselineStore applies the same accounting when
     * admitting rows, including disabled rows that may later be enabled. This character limit is not a byte count
     * for the enclosing transport request.
     */
    public static final int MAX_PAYLOAD_CHARS = 8 * 1024 * 1024;

    /** Characters used by the JSON array enclosure, included in both admission and serialization accounting. */
    public static final int PAYLOAD_ENCLOSURE_CHARS = 2;

    private static final Logger LOG = LogManager.getLogger(SPMForwardedSession.class);

    private SPMForwardedSession() {
    }

    /**
     * Conservative payload charge for one row: its JSON length plus one separator character. An array with rows
     * needs one fewer separator than the sum of these charges.
     */
    public static int payloadRowChars(BaselinePlan plan) {
        return new Gson().toJson(toPayloadRow(plan)).length() + 1;
    }

    /**
     * Serializes the ENABLED rows of the session store. The store rejects a creation that
     * would exceed MAX_PAYLOAD_CHARS (see SessionBaselineStore), so every
     * enabled row is carried and the rewrite context is IDENTICAL on this FE and on the
     * master that receives the forwarded statement. Reaching the budget here means the
     * store invariant broke (a row was registered without the check): fail the statement
     * loudly instead of silently planning it without a baseline the session owns.
     *
     * Parameter store: the connection's session store (may be null)
     * Returns: the JSON payload, or "" when there is nothing to carry
     * Throws IllegalStateException: when the enabled rows exceed the payload budget
     */
    public static String serialize(SessionBaselineStore store) {
        if (store == null || store.isEmpty()) {
            return "";
        }
        return serializeEnabled(store.getAllBaselines(), null);
    }

    /**
     * Serializes enabled rows with the statement match key when queryMatchKey can compute one. If no key is
     * available, serializes all enabled rows. Disabled rows are always omitted, including for management
     * statements.
     */
    public static String serializeForStatement(SessionBaselineStore store, ConnectContext ctx,
            String sql) {
        if (store == null || store.isEmpty()) {
            return "";
        }
        Pair<String, Long> matchKey = SPMPlanner.queryMatchKey(ctx, sql);
        if (matchKey == null) {
            return serialize(store);
        }
        return serializeEnabled(store.getAllBaselines(), matchKey);
    }

    /**
     * The JSON of the ENABLED rows, restricted to the given match key when one is
     * supplied (a row can only be matched on its (hash, digest) pair, so the restriction
     * preserves the rewrite context of the statement while leaving every non-matching
     * row out of the master's reconstruction). The payload budget is enforced on the
     * selected rows alone: a subset of what serialize would carry can only be smaller.
     */
    private static String serializeEnabled(List<BaselinePlan> plans, Pair<String, Long> matchKey) {
        Gson gson = new Gson();
        List<Map<String, String>> rows = new ArrayList<>();
        long payloadChars = PAYLOAD_ENCLOSURE_CHARS; // the enclosing []
        for (BaselinePlan plan : plans) {
            if (plan.getStatus() != BaselineStatus.ENABLED) {
                continue;
            }
            if (matchKey != null
                    && (matchKey.second.longValue() != plan.getBindSqlHash()
                            || !matchKey.first.equals(plan.getBindSqlDigest()))) {
                continue;
            }
            Map<String, String> row = toPayloadRow(plan);
            payloadChars += gson.toJson(row).length() + 1;
            rows.add(row);
        }
        if (payloadChars > MAX_PAYLOAD_CHARS) {
            LOG.error("The session holds SPM baselines whose forwarded payload needs {} of"
                            + " {} characters; the store must have rejected that creation",
                    payloadChars, MAX_PAYLOAD_CHARS);
            throw new IllegalStateException("SPM cannot forward this connection's SESSION"
                    + " baselines: the payload exceeds " + MAX_PAYLOAD_CHARS + " characters");
        }
        return rows.isEmpty() ? "" : gson.toJson(rows);
    }

    /** One carried row (the payload's field set for a baseline). */
    private static Map<String, String> toPayloadRow(BaselinePlan plan) {
        Map<String, String> row = new HashMap<>();
        row.put("id", String.valueOf(plan.getId()));
        row.put("bindSql", plan.getBindSql() == null ? "" : plan.getBindSql());
        row.put("planSql", plan.getPlanSql() == null ? "" : plan.getPlanSql());
        row.put("bindSqlDigest",
                plan.getBindSqlDigest() == null ? "" : plan.getBindSqlDigest());
        if (plan.getPlanSqlDigest() != null) {
            row.put("planSqlDigest", plan.getPlanSqlDigest());
        }
        row.put("bindSqlHash", String.valueOf(plan.getBindSqlHash()));
        row.put("creatorSqlMode", String.valueOf(plan.getCreatorSqlMode()));
        row.put("planSqlMode",
                plan.getPlanSqlMode() == null ? "" : String.valueOf(plan.getPlanSqlMode()));
        row.put("planFrozen",
                plan.getPlanFrozen() == null ? "" : plan.getPlanFrozen().toString());
        row.put("schemaFingerprint",
                plan.getSchemaFingerprint() == null ? "" : plan.getSchemaFingerprint());
        row.put("queryId", plan.getQueryId() == null ? "" : plan.getQueryId());
        row.put("cost", String.valueOf(plan.getCost()));
        row.put("queryTimeMs", String.valueOf(plan.getQueryTimeMs()));
        return row;
    }

    /**
     * Clears the receiving context store and imports replayable rows when rewriting is enabled. A null context,
     * null payload or missing store leaves state unchanged. Empty payloads and disabled rewriting leave the store
     * empty. Rows with missing SQL or unusable reconstructed trees are skipped. A runtime failure while decoding
     * or importing logs a warning and stops the remaining import; rows already imported remain.
     */
    public static void importInto(ConnectContext ctx, String payload) {
        if (ctx == null || payload == null) {
            return;
        }
        SessionBaselineStore store = ctx.getSessionBaselineStore();
        if (store == null) {
            return;
        }
        store.clear();
        if (payload.isEmpty()) {
            return;
        }
        if (!ctx.getSessionVariable().isEnableSpmRewrite()) {
            // A statement with rewrite disabled can never consult a baseline, and this
            // import runs BEFORE StmtExecutor even starts the (timeout-bounded) rewrite
            // path: restoring every enabled SESSION row - re-parsing each bind AND plan
            // SQL - would add unbounded work to EVERY forwarded statement of such a
            // connection, including the many statements that never touch SPM. The store
            // was cleared above, so the skipped import leaves exactly the empty state a
            // non-rewriting context is entitled to.
            return;
        }
        try {
            List<Map<String, String>> rows = new Gson().fromJson(payload,
                    new TypeToken<List<Map<String, String>>>() { }.getType());
            if (rows == null) {
                return;
            }
            for (Map<String, String> row : rows) {
                BaselinePlan plan = fromPayload(row);
                if (plan != null) {
                    // import under the ORIGINAL id: the forwarded EXPLAIN must report the
                    // id the connection itself uses for SHOW / ALTER / DROP
                    store.importBaseline(plan);
                }
            }
        } catch (RuntimeException e) {
            LOG.warn("SPM forwarded session baselines could not be restored: {}", e.getMessage());
        }
    }

    /** One carried row (null when it cannot be turned into a replayable baseline). */
    private static BaselinePlan fromPayload(Map<String, String> row) {
        String bindSql = row.get("bindSql");
        String planSql = row.get("planSql");
        if (bindSql == null || bindSql.isEmpty() || planSql == null || planSql.isEmpty()) {
            return null;
        }
        BaselinePlan plan = new BaselinePlan();
        plan.setId(parseLong(row.get("id")));
        plan.setBindSql(bindSql);
        plan.setPlanSql(planSql);
        plan.setBindSqlDigest(row.getOrDefault("bindSqlDigest", ""));
        plan.setPlanSqlDigest(row.get("planSqlDigest"));
        plan.setBindSqlHash(parseLong(row.get("bindSqlHash")));
        plan.setCreatorSqlMode(parseLong(row.get("creatorSqlMode")));
        plan.setPlanSqlMode(parseLongOrNull(row.get("planSqlMode")));
        plan.setPlanFrozen(parseBooleanOrNull(row.get("planFrozen")));
        plan.setSchemaFingerprint(row.getOrDefault("schemaFingerprint", ""));
        plan.setQueryId(row.getOrDefault("queryId", ""));
        plan.setCost(parseDouble(row.get("cost")));
        plan.setQueryTimeMs(parseLong(row.get("queryTimeMs")));
        plan.setSource(BaselineSource.USER);
        plan.setStatus(BaselineStatus.ENABLED);
        plan.setScope(BaselineScope.SESSION);
        boolean frozen = Boolean.TRUE.equals(plan.getPlanFrozen())
                || (plan.getPlanFrozen() == null
                        && SPMPlanner.isFrozenPlanSql(plan.getPlanSql(), null));
        long planSqlMode = plan.getPlanSqlMode() == null
                ? SqlModeHelper.MODE_DEFAULT : plan.getPlanSqlMode();
        Pair<LogicalPlan, LogicalPlan> trees = SPMPlanner.rebuildParameterizedTrees(
                plan.getBindSql(), frozen ? null : plan.getPlanSql(),
                plan.getCreatorSqlMode(), planSqlMode);
        if (trees.first == null) {
            return null; // bindSql cannot be parsed: no matching can ever use the row
        }
        if (!frozen && trees.second == null) {
            return null; // a non-frozen row needs its parameterized plan tree
        }
        plan.setParameterizedBindPlan(trees.first);
        if (!frozen) {
            plan.setParameterizedPlanPlan(trees.second);
        }
        return plan;
    }

    private static long parseLong(String text) {
        try {
            return text == null || text.isEmpty() ? 0L : Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static Long parseLongOrNull(String text) {
        try {
            return text == null || text.isEmpty() ? null : Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static double parseDouble(String text) {
        try {
            return text == null || text.isEmpty() ? 0.0 : Double.parseDouble(text.trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static Boolean parseBooleanOrNull(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }
        if ("true".equalsIgnoreCase(text.trim())) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(text.trim())) {
            return Boolean.FALSE;
        }
        return null;
    }
}
