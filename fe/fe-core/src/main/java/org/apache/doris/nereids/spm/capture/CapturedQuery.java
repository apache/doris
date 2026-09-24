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

package org.apache.doris.nereids.spm.capture;

import org.apache.doris.plugin.AuditEvent;

/**
 * CapturedQuery - a lightweight view of one audit_log row used by SPM auto capture
 * (Phase 2, design doc 7.2.3 / 7.2.4).
 *
 * Carries the fields SPM needs to decide whether to capture a query and to build the
 * baseline: the statement text, the execution statistics and the identity fields
 * (including the audit_log query_id of the source execution, stored on the baseline so
 * the audit record can be found by query id).
 */
public class CapturedQuery {

    /** The query statement text (used as bindSql / planSql source; exactly the audit_log
     *  stmt text, stored on the baseline verbatim). */
    private final String stmt;

    /** Actual execution time in milliseconds. */
    private final long queryTimeMs;

    /** Scanned row count. */
    private final long scanRows;

    /** Returned row count. */
    private final long returnRows;

    /** Parameterized digest recorded by the audit log (may be empty). */
    private final String sqlDigest;

    /** SQL hash recorded by the audit log (may be empty). */
    private final String sqlHash;

    /** Default database of the query. */
    private final String db;

    /** Default catalog of the query. */
    private final String catalog;

    /** audit_log query id of the captured execution (DebugUtil.printId format; may be
     *  "NaN" when the audit row carries no query id). */
    private final String queryId;

    /** audit_log is_internal flag (internal maintenance queries are never captured). */
    private final boolean isInternal;

    /**
     * CapturedQuery
     */
    public CapturedQuery(String stmt, long queryTimeMs, long scanRows, long returnRows,
            String sqlDigest, String sqlHash, String db, String catalog, String queryId) {
        this(stmt, queryTimeMs, scanRows, returnRows, sqlDigest, sqlHash, db, catalog, queryId,
                false);
    }

    /**
     * Full constructor (audit_log row).
     */
    public CapturedQuery(String stmt, long queryTimeMs, long scanRows, long returnRows,
            String sqlDigest, String sqlHash, String db, String catalog, String queryId,
            boolean isInternal) {
        this.stmt = stmt;
        this.queryTimeMs = queryTimeMs;
        this.scanRows = scanRows;
        this.returnRows = returnRows;
        this.sqlDigest = sqlDigest;
        this.sqlHash = sqlHash;
        this.db = db;
        this.catalog = catalog;
        this.queryId = queryId;
        this.isInternal = isInternal;
    }

    public String getStmt() {
        return stmt;
    }

    public long getQueryTimeMs() {
        return queryTimeMs;
    }

    public long getScanRows() {
        return scanRows;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public String getSqlDigest() {
        return sqlDigest;
    }

    public String getSqlHash() {
        return sqlHash;
    }

    public String getDb() {
        return db;
    }

    public String getCatalog() {
        return catalog;
    }

    /**
     * Returns the audit_log query id of the captured execution.
     *
     * @return the query id ("NaN" / empty when the audit row carried none)
     */
    public String getQueryId() {
        return queryId;
    }

    /**
     * Returns the audit_log is_internal flag.
     *
     * @return true when the audited statement was an internal (maintenance) query
     */
    public boolean isInternal() {
        return isInternal;
    }

    /**
     * Converts this record to an AuditEvent for the capture filter chain (only the
     * fields PlanCaptureFilter reads are filled).
     *
     * @return a new AuditEvent
     */
    public AuditEvent toAuditEvent() {
        AuditEvent event = new AuditEvent();
        event.isQuery = true;
        event.isNereids = true;
        event.isInternal = isInternal;
        event.queryTime = queryTimeMs;
        event.scanRows = scanRows;
        event.returnRows = returnRows;
        event.stmt = stmt;
        event.sqlDigest = sqlDigest;
        event.sqlHash = sqlHash;
        event.db = db;
        event.ctl = catalog;
        event.queryId = queryId;
        return event;
    }

    @Override
    public String toString() {
        return "CapturedQuery{queryTimeMs=" + queryTimeMs
                + ", scanRows=" + scanRows
                + ", digest=" + sqlDigest
                + ", stmt=" + stmt + "}";
    }
}
