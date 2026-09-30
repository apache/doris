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
import org.apache.doris.qe.SessionVariable;
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
 * <p>A SESSION baseline is created on the FE the connection is attached to
 * ({@code CREATE SESSION BASELINE PLAN} never forwards) and lives only in that
 * {@link SessionBaselineStore}. A SELECT that must be FORWARDED (the observer cannot read,
 * or forwarding is forced) returns before local SPM matching, and the master executes it in
 * a fresh {@link ConnectContext} with an EMPTY session store - so the very baseline the
 * connection created was never used. The observer therefore attaches the rows to the
 * forwarded request (see {@code FEOpExecutor#fillForwardRequest}) and the master rebuilds
 * them into its context's store before the statement is planned
 * ({@code ConnectProcessor#proxyExecute}).
 *
 * <p>The payload always comes from the store right before forwarding (the variable is
 * overwritten there), so a client cannot inject rows with a plain {@code SET}, and the
 * ADMIN-authorized CREATE remains the only way a row is created.
 */
public final class SPMForwardedSession {

    private static final Logger LOG = LogManager.getLogger(SPMForwardedSession.class);

    private SPMForwardedSession() {
    }

    /**
     * Serializes the ENABLED rows of the session store.
     *
     * @param store the connection's session store (may be null)
     * @return the JSON payload, or "" when there is nothing to carry
     */
    public static String serialize(SessionBaselineStore store) {
        if (store == null || store.isEmpty()) {
            return "";
        }
        List<Map<String, String>> rows = new ArrayList<>();
        for (BaselinePlan plan : store.getAllBaselines()) {
            if (plan.getStatus() != BaselineStatus.ENABLED) {
                continue;
            }
            Map<String, String> row = new HashMap<>();
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
            rows.add(row);
        }
        return rows.isEmpty() ? "" : new Gson().toJson(rows);
    }

    /**
     * Rebuilds the carried rows into the given context's session store (the transient
     * parameterized trees are re-parsed exactly like the on-disk reload does). The payload
     * is the CURRENT observer store, so the import REPLACES this context's content: a row
     * dropped on the observer must not survive on the master.
     *
     * <p>Malformed payloads / unparsable rows are skipped with a warning - SPM must never
     * break the statement it is attached to.
     *
     * @param ctx     the master-side context of the forwarded statement
     * @param payload the payload of {@link SessionVariable#SPM_FORWARDED_SESSION_BASELINES}
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
        try {
            List<Map<String, String>> rows = new Gson().fromJson(payload,
                    new TypeToken<List<Map<String, String>>>() { }.getType());
            if (rows == null) {
                return;
            }
            for (Map<String, String> row : rows) {
                BaselinePlan plan = fromPayload(row);
                if (plan != null) {
                    store.createBaseline(plan);
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
