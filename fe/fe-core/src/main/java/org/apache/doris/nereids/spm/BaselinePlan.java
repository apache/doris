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
 * Baseline metadata and transient trees used to match and replay a query plan. The bind digest is a canonical
 * structural key. The stored plan SQL is either a decompiled frozen plan or the original plan SQL retained for
 * fallback planning.
 */
public class BaselinePlan {

    /** Store-assigned identifier. Its range distinguishes GLOBAL and SESSION scope. */
    private long id;

    /** The original binding SQL (contains literals, used for SHOW). */
    private String bindSql;

    /** Canonical structural digest of the parameterized bind query, used for exact digest matching. */
    private String bindSqlDigest;

    /** A 64-bit structural hash (used for fast in-memory index filtering). */
    private long bindSqlHash;

    /** Stored plan SQL, either decompiled frozen SQL or the original fallback SQL. */
    private String planSql;

    /** Query id of the statement that created the baseline; empty when unavailable. */
    private String queryId = "";

    /** The CBO estimated cost. */
    private double cost;

    /** Recorded execution time in milliseconds; a negative value means unknown. */
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

    /** Creation time in epoch microseconds. */
    private long createTime;

    /**
     * The parser-relevant sql_mode bits of the CREATING session (persisted in the
     * sql_mode column). The stored bindSql is USER-authored text: re-parsing it under
     * MODE_DEFAULT could change its meaning - under PIPES_AS_CONCAT a || b is
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
     * CONTAINS a placeholder-like call (e.g. a real UDF named db._spm_const_var)
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
     * TABLE... ADD COLUMN keeps matching while the frozen result sink still emits the
     * creator-time output columns.
     */
    private String schemaFingerprint;

    /**
     * Canonical digest of the submitted plan SQL, used to confirm the identity of a forwarded CREATE independently
     * of the stored plan rendering.
     */
    private String planSqlDigest;

    /** Last update time in epoch microseconds. */
    private volatile long updateTime;

    /**
     * Transient bind tree with supported literal positions replaced by scalar or list markers. Used for structural
     * matching and extraction of the current query expressions. Marker ids are allocated within the builder, with
     * corresponding bind and plan occurrences sharing ids.
     */
    private transient LogicalPlan parameterizedBindPlan;

    /**
     * Transient parameterized fallback plan tree. Matching supplies replacements for its markers before normal
     * planning. Rebuilt lazily from stored SQL when needed; frozen SQL is replayed directly and does not require
     * this tree.
     */
    private transient LogicalPlan parameterizedPlanPlan;

    /**
     * Whether the transient trees of this row are still PENDING: the shallow snapshot
     * decode (see BaselineManager.shallowPersistedRow) reads only the persisted SCALARS -
     * a load / refresh snapshot serves the hash index, the mutation-fence settlement, the
     * tombstone filter, the duplicate collapse and the id watermark, none of which parses
     * SQL - and marks the row so its trees are rebuilt LAZILY, exactly when the row enters
     * the per-bindSql match cache (see BaselineManager.deepenPersistedRow).
     *
     * A row whose trees were never rebuilt must never serve a match / rewrite: the
     * placeholder substitution would run against a null tree. Not persisted, transient
     * like the trees it describes.
     */
    private transient boolean treesPending;

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
     * Returns: the audit query id
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
     * javadoc); SqlModeHelper.MODE_DEFAULT when unknown.
     *
     * Returns: the creation sql_mode bits
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
     * Returns: the stored planSql parse mode, or null
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
     * Returns: the persisted frozen provenance, or null
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
     * Returns: the schema fingerprint, or null
     */
    public String getSchemaFingerprint() {
        return schemaFingerprint;
    }

    public void setSchemaFingerprint(String schemaFingerprint) {
        this.schemaFingerprint = schemaFingerprint;
    }

    /**
     * Returns the canonical digest of the SUBMITTED plan SQL (see the field javadoc);
     * null / empty for pre-column rows, in which case only the bind digest can
     * identify the row.
     *
     * Returns: the canonical plan digest, or null
     */
    public String getPlanSqlDigest() {
        return planSqlDigest;
    }

    public void setPlanSqlDigest(String planSqlDigest) {
        this.planSqlDigest = planSqlDigest;
    }

    /**
     * Copies stored scalar metadata and the store-supplied scope. Transient plan trees and their pending-rebuild
     * flag are not copied.
     */
    public BaselinePlan copyPersistedScalars() {
        BaselinePlan copy = new BaselinePlan();
        copy.id = id;
        copy.bindSql = bindSql;
        copy.bindSqlDigest = bindSqlDigest;
        copy.bindSqlHash = bindSqlHash;
        copy.planSql = planSql;
        copy.queryId = queryId;
        copy.cost = cost;
        copy.queryTimeMs = queryTimeMs;
        copy.source = source;
        copy.status = status;
        copy.scope = scope;
        copy.createTime = createTime;
        copy.creatorSqlMode = creatorSqlMode;
        copy.planSqlMode = planSqlMode;
        copy.planFrozen = planFrozen;
        copy.schemaFingerprint = schemaFingerprint;
        copy.planSqlDigest = planSqlDigest;
        copy.updateTime = updateTime;
        return copy;
    }

    /**
     * Returns the parameterized whole-query bind plan tree (may be null when the
     * baseline was built without a parsed bind plan).
     *
     * Returns: the parameterized bind plan tree
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
     * Returns: the parameterized plan plan tree
     */
    public LogicalPlan getParameterizedPlanPlan() {
        return parameterizedPlanPlan;
    }

    public void setParameterizedPlanPlan(LogicalPlan parameterizedPlanPlan) {
        this.parameterizedPlanPlan = parameterizedPlanPlan;
    }

    /**
     * Whether the transient trees still have to be rebuilt (see treesPending): a shallow
     * snapshot row is deepened before it enters the match cache.
     *
     * Returns: whether the trees are pending
     */
    public boolean isTreesPending() {
        return treesPending;
    }

    /**
     * Marks (or clears) the pending state of the transient trees (see treesPending).
     *
     * Parameter treesPending: whether the trees have to be rebuilt
     */
    public void setTreesPending(boolean treesPending) {
        this.treesPending = treesPending;
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
