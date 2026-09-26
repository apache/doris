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
import org.apache.doris.qe.SqlModeHelper;

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

    /**
     * The parser-relevant sql_mode bits of the CREATING session (persisted in the
     * `sql_mode` column). The stored bindSql is USER-authored text: re-parsing it under
     * MODE_DEFAULT could change its meaning - under PIPES_AS_CONCAT {@code a || b} is
     * concat(a, b), while a default-mode parse produces a boolean Or - so the reloaded
     * bind tree would still be found by the stored digest but fail Level-3 structural
     * matching against every CONCAT-mode query. 0 / missing (pre-column rows) means
     * MODE_DEFAULT.
     */
    private long creatorSqlMode = SqlModeHelper.MODE_DEFAULT;

    /**
     * The parser mode for the STORED planSql (Null for pre-column rows = mode default).
     * The planSql can be one of two kinds: the SPM decompiled rendering of the frozen
     * physical plan (always emitted for MODE_DEFAULT) or the user's raw planSql kept as
     * the fallback when the physical plan cannot be decompiled (e.g.
     * PhysicalAssertNumRows). The fallback text is USER-authored and must be re-parsed
     * with the CREATOR's mode - under MODE_DEFAULT a PIPES_AS_CONCAT clause reloads as a
     * boolean Or, so the bind tree still matches while the plan tree replays different
     * projection semantics.
     */
    private Long planSqlMode;

    /**
     * Whether the stored planSql is the SPM decompiled, placeholder-carrying text that
     * is replayed as FROZEN SQL. Null for pre-column rows = classify by parsing.
     * Persisting the provenance explicitly keeps a raw-fallback text that merely
     * CONTAINS a placeholder-like call (e.g. a real UDF named {@code db._spm_const_var})
     * from being misclassified as frozen after a reload: such a row keeps its
     * parameterized fallback tree, and the replacer would otherwise substitute the
     * caller's literal for the real function call.
     */
    private Boolean planFrozen;

    /**
     * Fingerprint of the schema identity of the base tables referenced by the bind
     * query at CREATE time (empty / null = pre-column rows). Matching validates it
     * against the current session's tables before replaying the frozen plan: the bind
     * key is built from the still-unbound query, so without the fingerprint an ALTER
     * TABLE ... ADD COLUMN keeps matching while the frozen result sink still emits the
     * creator-time output columns.
     */
    private String schemaFingerprint;

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
     * Returns the parser-relevant sql_mode bits of the creating session (see the field
     * javadoc); {@link SqlModeHelper#MODE_DEFAULT} when unknown.
     *
     * @return the creation sql_mode bits
     */
    public long getCreatorSqlMode() {
        return creatorSqlMode;
    }

    public void setCreatorSqlMode(long creatorSqlMode) {
        this.creatorSqlMode = creatorSqlMode;
    }

    /**
     * Returns the parser mode the stored planSql must be re-parsed with (see the field
     * javadoc); null for pre-column rows / when unknown (callers pin MODE_DEFAULT).
     *
     * @return the stored planSql parse mode, or null
     */
    public Long getPlanSqlMode() {
        return planSqlMode;
    }

    public void setPlanSqlMode(Long planSqlMode) {
        this.planSqlMode = planSqlMode;
    }

    /**
     * Returns whether the stored planSql is SPM's decompiled, placeholder-carrying
     * frozen text (see the field javadoc); null for pre-column rows (classify by
     * parsing) or when unknown.
     *
     * @return the persisted frozen provenance, or null
     */
    public Boolean getPlanFrozen() {
        return planFrozen;
    }

    public void setPlanFrozen(Boolean planFrozen) {
        this.planFrozen = planFrozen;
    }

    /**
     * Returns the CREATE-time schema fingerprint of the referenced base tables (see the
     * field javadoc); null / empty for pre-column rows and table-less queries (no
     * validation possible / needed).
     *
     * @return the schema fingerprint, or null
     */
    public String getSchemaFingerprint() {
        return schemaFingerprint;
    }

    public void setSchemaFingerprint(String schemaFingerprint) {
        this.schemaFingerprint = schemaFingerprint;
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
