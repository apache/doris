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
 * Carries the enabled SESSION-scope baselines of a forwarding connection to the master FE.
 *
 * A SESSION baseline is created on the FE the connection is attached to
 * (CREATE SESSION BASELINE PLAN never forwards) and lives only in that
 * SessionBaselineStore. A SELECT that must be FORWARDED (the observer cannot read,
 * or forwarding is forced) returns before local SPM matching, and the master executes it in
 * a fresh ConnectContext with an EMPTY session store - so the very baseline the
 * connection created was never used. The observer therefore attaches the rows the
 * statement could MATCH (see serializeForStatement; the full store travels for
 * statements that address rows by id, like the baseline management commands) to the
 * forwarded request (see FEOpExecutor#fillForwardRequest) and the master rebuilds them
 * into its context's store before the statement is planned
 * (ConnectProcessor#proxyExecute).
 *
 * The payload always comes from the store right before forwarding (the variable is
 * overwritten there), so a client cannot inject rows with a plain SET, and the
 * ADMIN-authorized CREATE remains the only way a row is created.
 */
public final class SPMForwardedSession {

    /**
     * The forward payload's CHARACTER budget (the JSON text carried in the session variable
     * of the forwarded statement). The payload is part of a Thrift
     * TMasterOpRequest whose default message limit is 100 MiB - an unbounded
     * serialization of a session holding enough ADMIN-created baselines (each with BOTH SQL
     * texts) made EVERY forwarded statement on that connection fail with a transport error.
     *
     * The budget is enforced where the rows are CREATED
     * (SessionBaselineStore#createBaseline rejects a row that would exceed it), not
     * while serializing: a row that was silently DROPPED here still participates in local
     * matching, so the same connection rewrote the statement locally with its SESSION
     * baseline yet planned the FORWARDED statement without it (falling through to a GLOBAL
     * baseline or to none) - the rewrite context silently changed with the forwarding
     * decision. 8 MiB is generous for any realistic session (thousands of multi-KB
     * baselines) yet far below the transport limit even after JSON escaping doubles the
     * text.
     */
    public static final int MAX_PAYLOAD_CHARS = 8 * 1024 * 1024;

    /**
     * Characters the JSON ARRAY enclosure ([]) adds to the payload.
     * The session store's admission check AND serialize must account for it with
     * the SAME number: a store that admitted rows summing exactly to the row budget used
     * to serialize two characters MORE than the budget and every rewrite-enabled
     * statement forwarded from that connection failed in serialize.
     */
    public static final int PAYLOAD_ENCLOSURE_CHARS = 2;

    private static final Logger LOG = LogManager.getLogger(SPMForwardedSession.class);

    private SPMForwardedSession() {
    }

    /**
     * The exact number of payload characters ONE row contributes (see serialize):
     * the JSON of toPayloadRow plus the array separator. Used by the session
     * store to bound what it accepts, so serialization can always carry every row.
     *
     * @param plan the baseline
     * @return the row's payload size in characters
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
     * @param store the connection's session store (may be null)
     * @return the JSON payload, or "" when there is nothing to carry
     * @throws IllegalStateException when the enabled rows exceed the payload budget
     */
    public static String serialize(SessionBaselineStore store) {
        if (store == null || store.isEmpty()) {
            return "";
        }
        return serializeEnabled(store.getAllBaselines(), null);
    }

    /**
     * Serializes the session baselines the forwarded statement could actually USE: the
     * ENABLED rows whose (structural hash, canonical digest) equal the statement's own
     * match key - the very contract SPMPlanner#tryRewritePlan looks candidates up with
     * (see SPMPlanner#queryMatchKey). The master imports the payload BEFORE classifying
     * the statement (ConnectProcessor#proxyExecute) and REBUILDS every carried row (both
     * SQL texts of each are re-parsed): forwarding the whole store made a connection
     * with a large store pay that reconstruction on every forwarded statement -
     * including statements that can never consult a baseline - while
     * spm_rewrite_timeout_ms only bounds the planner side of the work.
     *
     * Statements that are NOT plan-rewritable queries (commands / DDL, statement
     * batches, unparsable text) keep the FULL payload: the baseline management commands
     * executed on the master ADDRESS ROWS BY ID against this context's store, so they
     * must see every row of the connection.
     *
     * @param store the connection's session store (may be null)
     * @param ctx   the forwarding statement's context (may be null in tests)
     * @param sql   the forwarded statement text
     * @return the payload for the statement (see serialize)
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
     * Rebuilds the carried rows into the given context's session store (the transient
     * parameterized trees are re-parsed exactly like the on-disk reload does). The payload
     * is the forwarding connection's store restricted to what the statement can use - or
     * the full store for a statement that addresses rows by id (see
     * serializeForStatement) - so the import REPLACES this context's content: a row the
     * payload deliberately left out must not survive on the master either.
     *
     * Malformed payloads / unparsable rows are skipped with a warning - SPM must never
     * break the statement it is attached to.
     *
     * @param ctx     the master-side context of the forwarded statement
     * @param payload the payload of the SPM_FORWARDED_SESSION_BASELINES session variable
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
