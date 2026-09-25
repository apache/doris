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

import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

/**
 * BaselinePlan - SPM baseline data model.
 *
 * Corresponds to design doc sections 6.1 and 6.14 (the spm_baselines table). A baseline
 * describes:
 *
 * - bindSql: the binding SQL used for matching (contains literals, shown by SHOW)
 * - bindSqlDigest: the parameterized binding SQL (used for exact digest matching, Level 2)
 * - bindSqlHash: a 64-bit structural hash (used for fast in-memory index filtering, Level 1)
 * - planSql: the frozen plan SQL (contains HINTs and placeholders, executed during query
 *   rewrite)
 * - queryId: the audit_log identity of the statement that produced this baseline.
 *   CREATE BASELINE PLAN stores the queryId of the CREATE statement itself and auto
 *   capture the queryId of the captured query, so the audit_log row (db / user / timing /
 *   the full stmt text) of the originating statement can be looked up through
 *   {@code SELECT ... FROM __internal_schema.audit_log WHERE query_id = '<query_id>'}.
 *   For auto-captured baselines the audit row carries the very stmt text that was
 *   stored here as bindSql (the capture pipeline reads bindSql from audit_log verbatim),
 *   so the two texts can be compared directly.
 */
public class BaselinePlan {

    /** Globally unique id (auto-increment). */
    private long id;

    /** The original binding SQL (contains literals, used for SHOW). */
    private String bindSql;

    /** The parameterized binding SQL (used for exact digest matching). */
    private String bindSqlDigest;

    /** A 64-bit structural hash (used for fast in-memory index filtering). */
    private long bindSqlHash;

    /** The frozen plan SQL (contains HINTs and placeholder functions). */
    private String planSql;

    /**
     * The audit_log query id (DebugUtil.printId format, e.g. "8c1afc857d724b74-aa87f48b9de66193")
     * of the statement that produced this baseline: the CREATE BASELINE PLAN statement for
     * source = USER, the captured query for source = CAPTURE. Persisted as the `query_id`
     * column (see design doc 6.14.1) so the corresponding audit_log row - and through it the
     * full statement text, db / user and execution statistics - can be located by query id.
     * Empty when unknown (e.g. an internal context without a query id).
     */
    private String queryId = "";

    /** The CBO estimated cost. */
    private double cost;

    /** Actual execution time in milliseconds (-1 means unknown; only filled for auto capture). */
    private long queryTimeMs = -1;

    /** Source: USER (manual) / CAPTURE (auto capture). */
    private BaselineSource source = BaselineSource.USER;

    /** Status: ENABLED / DISABLED. */
    private volatile BaselineStatus status = BaselineStatus.ENABLED;

    /**
     * Storage scope: GLOBAL (persisted in the shared spm_baselines internal table, the
     * default) or SESSION (connection-local memory only). Transient: the scope is implied
     * by the owning store (BaselineManager -> GLOBAL, SessionBaselineStore -> SESSION) and
     * is not a column of the internal table, so rows loaded from the table keep the GLOBAL
     * default. Surfaced by the SHOW BASELINE PLANS scope column and the EXPLAIN banner.
     */
    private transient BaselineScope scope = BaselineScope.GLOBAL;

    /** Creation time (epoch millis). */
    private long createTime;

    /** Last update time (epoch millis). */
    private volatile long updateTime;

    /**
     * The parameterized whole-query bind plan tree (transient, not persisted): the
     * parsed bindSql with EVERY literal replaced by a placeholder (SpmConstVar /
     * SpmConstList), including literals inside subqueries. Produced by
     * SPMPlaceholderBuilder over the whole tree, with a single shared builder so
     * placeholder ids are globally unique and stay aligned with
     * parameterizedPlanPlan. Used for the Level 3 structural comparison against
     * the user query (value extraction).
     */
    private transient LogicalPlan parameterizedBindPlan;

    /**
     * The parameterized whole-query plan plan tree (transient, not persisted): the
     * parsed planSql with the same placeholders as parameterizedBindPlan.
     * During a rewrite the placeholders are substituted with the user's actual values
     * and the resulting tree is planned normally.
     *
     * Not stored in the internal table; rebuilt at startup load / periodic refresh from
     * the persisted bindSql + planSql texts with ONE shared builder in the CREATE order
     * (see SPMPlanner#rebuildParameterizedTrees and
     * BaselineManager#readPersistedSnapshot), unless the planSql is a frozen
     * (placeholder-carrying) text - such baselines replay the frozen text directly and
     * keep this field null.
     */
    private transient LogicalPlan parameterizedPlanPlan;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getBindSql() {
        return bindSql;
    }

    public void setBindSql(String bindSql) {
        this.bindSql = bindSql;
    }

    public String getBindSqlDigest() {
        return bindSqlDigest;
    }

    public void setBindSqlDigest(String bindSqlDigest) {
        this.bindSqlDigest = bindSqlDigest;
    }

    public long getBindSqlHash() {
        return bindSqlHash;
    }

    public void setBindSqlHash(long bindSqlHash) {
        this.bindSqlHash = bindSqlHash;
    }

    public String getPlanSql() {
        return planSql;
    }

    public void setPlanSql(String planSql) {
        this.planSql = planSql;
    }

    /**
     * Returns the audit_log query id of the statement that produced this baseline (see the
     * field javadoc); empty when unknown.
     *
     * @return the audit query id
     */
    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId == null ? "" : queryId;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public long getQueryTimeMs() {
        return queryTimeMs;
    }

    public void setQueryTimeMs(long queryTimeMs) {
        this.queryTimeMs = queryTimeMs;
    }

    public BaselineSource getSource() {
        return source;
    }

    public void setSource(BaselineSource source) {
        this.source = source;
    }

    public BaselineStatus getStatus() {
        return status;
    }

    public void setStatus(BaselineStatus status) {
        this.status = status;
    }

    public BaselineScope getScope() {
        return scope;
    }

    public void setScope(BaselineScope scope) {
        this.scope = scope;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    /**
     * Returns the parameterized whole-query bind plan tree (may be null when the
     * baseline was built without a parsed bind plan).
     *
     * @return the parameterized bind plan tree
     */
    public LogicalPlan getParameterizedBindPlan() {
        return parameterizedBindPlan;
    }

    public void setParameterizedBindPlan(LogicalPlan parameterizedBindPlan) {
        this.parameterizedBindPlan = parameterizedBindPlan;
    }

    /**
     * Returns the parameterized whole-query plan plan tree (may be null when the
     * baseline was built without a parsed plan plan).
     *
     * @return the parameterized plan plan tree
     */
    public LogicalPlan getParameterizedPlanPlan() {
        return parameterizedPlanPlan;
    }

    public void setParameterizedPlanPlan(LogicalPlan parameterizedPlanPlan) {
        this.parameterizedPlanPlan = parameterizedPlanPlan;
    }

    @Override
    public String toString() {
        return "BaselinePlan{id=" + id
                + ", digest=" + bindSqlDigest
                + ", hash=" + bindSqlHash
                + ", source=" + source
                + ", status=" + status
                + ", cost=" + cost
                + ", queryTimeMs=" + queryTimeMs + "}";
    }
}
