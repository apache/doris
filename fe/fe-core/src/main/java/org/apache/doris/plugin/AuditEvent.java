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

package org.apache.doris.plugin;


import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/*
 * AuditEvent contains all information about audit log info.
 * It should be created by AuditEventBuilder. For example:
 *
 *      AuditEvent event = new AuditEventBuilder()
 *          .setEventType(AFTER_QUERY)
 *          .setClientIp(xxx)
 *          ...
 *          .build();
 */
public class AuditEvent {
    public enum EventType {
        CONNECTION,
        DISCONNECTION,
        BEFORE_QUERY,
        AFTER_QUERY,
        LOAD_SUCCEED,
        STREAM_LOAD_FINISH
    }

    @Retention(RetentionPolicy.RUNTIME)
    public @interface AuditField {
        String value() default "";

        String colName() default "";
    }

    public EventType type;

    // all fields which is about to be audited should be annotated by "@AuditField"
    // make them all "public" so that easy to visit.

    // uuid and time
    @AuditField(value = "QueryId", colName = "query_id")
    public String queryId = "";
    @AuditField(value = "Timestamp", colName = "time")
    public long timestamp = -1;

    // cs info
    @AuditField(value = "Client", colName = "client_ip")
    public String clientIp = "";
    @AuditField(value = "User", colName = "user")
    public String user = "";
    @AuditField(value = "FeIp", colName = "frontend_ip")
    public String feIp = "";

    // default ctl and db
    @AuditField(value = "Ctl", colName = "catalog")
    public String ctl = "";
    @AuditField(value = "Db", colName = "db")
    public String db = "";

    // query state
    @AuditField(value = "State", colName = "state")
    public String state = "";
    @AuditField(value = "ErrorCode", colName = "error_code")
    public int errorCode = 0;
    @AuditField(value = "ErrorMessage", colName = "error_message")
    public String errorMessage = "";

    // execution info
    @AuditField(value = "Time(ms)", colName = "query_time")
    public long queryTime = -1;
    @AuditField(value = "CpuTimeMS", colName = "cpu_time_ms")
    public long cpuTimeMs = -1;
    @AuditField(value = "PeakMemoryBytes", colName = "peak_memory_bytes")
    public long peakMemoryBytes = -1;
    @AuditField(value = "ScanBytes", colName = "scan_bytes")
    public long scanBytes = -1;
    @AuditField(value = "ScanRows", colName = "scan_rows")
    public long scanRows = -1;
    @AuditField(value = "ReturnRows", colName = "return_rows")
    public long returnRows = -1;
    @AuditField(value = "ShuffleSendRows", colName = "shuffle_send_rows")
    public long shuffleSendRows = -1;
    @AuditField(value = "ShuffleSendBytes", colName = "shuffle_send_bytes")
    public long shuffleSendBytes = -1;
    @AuditField(value = "SpillWriteBytesToLocalStorage", colName = "spill_write_bytes_from_local_storage")
    public long spillWriteBytesToLocalStorage = -1;
    @AuditField(value = "SpillReadBytesFromLocalStorage", colName = "spill_read_bytes_from_local_storage")
    public long spillReadBytesFromLocalStorage = -1;
    @AuditField(value = "ScanBytesFromLocalStorage", colName = "scan_bytes_from_local_storage")
    public long scanBytesFromLocalStorage = -1;
    @AuditField(value = "ScanBytesFromRemoteStorage", colName = "scan_bytes_from_remote_storage")
    public long scanBytesFromRemoteStorage = -1;

    // plan info
    @AuditField(value = "ParseTimeMs", colName = "parse_time_ms")
    public int parseTimeMs = -1;
    @AuditField(value = "PlanTimesMs", colName = "plan_times_ms")
    public String planTimesMs = "";
    @AuditField(value = "GetMetaTimesMs", colName = "get_meta_times_ms")
    public String getMetaTimesMs = "";
    @AuditField(value = "ScheduleTimesMs", colName = "schedule_times_ms")
    public String scheduleTimesMs = "";
    @AuditField(value = "HitSqlCache", colName = "hit_sql_cache")
    public boolean hitSqlCache = false;
    @AuditField(value = "isHandledInFe", colName = "handled_in_fe")
    public boolean isHandledInFe = false;

    // table, view, m-view
    @AuditField(value = "queriedTablesAndViews", colName = "queried_tables_and_views")
    public String queriedTablesAndViews = "";
    @AuditField(value = "chosenMViews", colName = "chosen_m_views")
    public String chosenMViews = "";

    // variable and configs
    @AuditField(value = "ChangedVariables", colName = "changed_variables")
    public String changedVariables = "";
    @AuditField(value = "FuzzyVariables")
    public String fuzzyVariables = "";
    @AuditField(value = "SqlMode", colName = "sql_mode")
    public String sqlMode = "";

    // type and digest
    @AuditField(value = "CommandType")
    public String commandType = "";
    @AuditField(value = "StmtType", colName = "stmt_type")
    public String stmtType = "";
    @AuditField(value = "StmtId", colName = "stmt_id")
    public long stmtId = -1;
    @AuditField(value = "SqlHash", colName = "sql_hash")
    public String sqlHash = "";
    @AuditField(value = "SqlDigest", colName = "sql_digest")
    public String sqlDigest = "";
    @AuditField(value = "IsQuery", colName = "is_query")
    public boolean isQuery = false;
    @AuditField(value = "IsNereids", colName = "is_nereids")
    public boolean isNereids = false;
    @AuditField(value = "IsInternal", colName = "is_internal")
    public boolean isInternal = false;

    // resource
    @AuditField(value = "WorkloadGroup", colName = "workload_group")
    public String workloadGroup = "";
    @AuditField(value = "ComputeGroupName", colName = "compute_group")
    public String cloudClusterName = "";

    // stmt should be last one
    @AuditField(value = "Stmt", colName = "stmt")
    public String stmt = "";

    public long pushToAuditLogQueueTime;

    public static class AuditEventBuilder {

        private AuditEvent auditEvent = new AuditEvent();

        public AuditEventBuilder() {
        }

        public void reset() {
            auditEvent = new AuditEvent();
        }

        public AuditEventBuilder setEventType(EventType eventType) {
            auditEvent.type = eventType;
            return this;
        }

        public AuditEventBuilder setTimestamp(long timestamp) {
            auditEvent.timestamp = timestamp;
            return this;
        }

        public AuditEventBuilder setClientIp(String clientIp) {
            auditEvent.clientIp = clientIp;
            return this;
        }

        public AuditEventBuilder setUser(String user) {
            auditEvent.user = user;
            return this;
        }

        public AuditEventBuilder setCtl(String ctl) {
            auditEvent.ctl = ctl;
            return this;
        }

        public AuditEventBuilder setDb(String db) {
            auditEvent.db = db;
            return this;
        }

        public AuditEventBuilder setCloudCluster(String cloudClusterName) {
            auditEvent.cloudClusterName = cloudClusterName;
            return this;
        }

        public AuditEventBuilder setState(String state) {
            auditEvent.state = state;
            return this;
        }

        public AuditEventBuilder setErrorCode(int errorCode) {
            auditEvent.errorCode = errorCode;
            return this;
        }

        public AuditEventBuilder setErrorMessage(String errorMessage) {
            auditEvent.errorMessage = errorMessage;
            return this;
        }

        public AuditEventBuilder setQueryTime(long queryTime) {
            auditEvent.queryTime = queryTime;
            return this;
        }

        public AuditEventBuilder setScanBytes(long scanBytes) {
            auditEvent.scanBytes = scanBytes;
            return this;
        }

        public AuditEventBuilder setCpuTimeMs(long cpuTimeMs) {
            auditEvent.cpuTimeMs = cpuTimeMs;
            return this;
        }

        public AuditEventBuilder setPeakMemoryBytes(long peakMemoryBytes) {
            auditEvent.peakMemoryBytes = peakMemoryBytes;
            return this;
        }

        public AuditEventBuilder setScanRows(long scanRows) {
            auditEvent.scanRows = scanRows;
            return this;
        }

        public AuditEventBuilder setReturnRows(long returnRows) {
            auditEvent.returnRows = returnRows;
            return this;
        }

        public AuditEventBuilder setStmtId(long stmtId) {
            auditEvent.stmtId = stmtId;
            return this;
        }

        public AuditEventBuilder setQueryId(String queryId) {
            auditEvent.queryId = queryId;
            return this;
        }

        public AuditEventBuilder setIsQuery(boolean isQuery) {
            auditEvent.isQuery = isQuery;
            return this;
        }

        public AuditEventBuilder setIsNereids(boolean isNereids) {
            auditEvent.isNereids = isNereids;
            return this;
        }

        public AuditEventBuilder setisInternal(boolean isInternal) {
            auditEvent.isInternal = isInternal;
            return this;
        }

        public AuditEventBuilder setFeIp(String feIp) {
            auditEvent.feIp = feIp;
            return this;
        }

        public AuditEventBuilder setStmtType(String stmtType) {
            auditEvent.stmtType = stmtType;
            return this;
        }

        public AuditEventBuilder setStmt(String stmt) {
            auditEvent.stmt = stmt;
            return this;
        }

        public AuditEventBuilder setSqlHash(String sqlHash) {
            auditEvent.sqlHash = sqlHash;
            return this;
        }

        public AuditEventBuilder setSqlDigest(String sqlDigest) {
            auditEvent.sqlDigest = sqlDigest;
            return this;
        }

        public AuditEventBuilder setFuzzyVariables(String variables) {
            auditEvent.fuzzyVariables = variables;
            return this;
        }

        public AuditEventBuilder setWorkloadGroup(String workloadGroup) {
            auditEvent.workloadGroup = workloadGroup;
            return this;
        }

        public AuditEventBuilder setScanBytesFromLocalStorage(long scanBytesFromLocalStorage) {
            auditEvent.scanBytesFromLocalStorage = scanBytesFromLocalStorage;
            return this;
        }

        public AuditEventBuilder setScanBytesFromRemoteStorage(long scanBytesFromRemoteStorage) {
            auditEvent.scanBytesFromRemoteStorage = scanBytesFromRemoteStorage;
            return this;
        }

        public AuditEventBuilder setCommandType(String commandType) {
            auditEvent.commandType = commandType;
            return this;
        }

        public AuditEventBuilder setSpillWriteBytesToLocalStorage(long bytes) {
            auditEvent.spillWriteBytesToLocalStorage = bytes;
            return this;
        }

        public AuditEventBuilder setSpillReadBytesFromLocalStorage(long bytes) {
            auditEvent.spillReadBytesFromLocalStorage = bytes;
            return this;
        }

        public AuditEventBuilder setParseTimeMs(int parseTimeMs) {
            auditEvent.parseTimeMs = parseTimeMs;
            return this;
        }

        public AuditEventBuilder setPlanTimesMs(String planTimesMs) {
            auditEvent.planTimesMs = planTimesMs;
            return this;
        }

        public AuditEventBuilder setGetMetaTimeMs(String getMetaTimeMs) {
            auditEvent.getMetaTimesMs = getMetaTimeMs;
            return this;
        }

        public AuditEventBuilder setScheduleTimeMs(String scheduleTimeMs) {
            auditEvent.scheduleTimesMs = scheduleTimeMs;
            return this;
        }

        public AuditEventBuilder setHitSqlCache(boolean hitSqlCache) {
            auditEvent.hitSqlCache = hitSqlCache;
            return this;
        }

        public AuditEventBuilder setHandledInFe(boolean handledInFe) {
            auditEvent.isHandledInFe = handledInFe;
            return this;
        }

        public AuditEventBuilder setChangedVariables(String changedVariables) {
            auditEvent.changedVariables = changedVariables;
            return this;
        }

        public AuditEventBuilder setSqlMode(String sqlMode) {
            auditEvent.sqlMode = sqlMode;
            return this;
        }

        public AuditEventBuilder setQueriedTablesAndViews(String queriedTablesAndViews) {
            auditEvent.queriedTablesAndViews = queriedTablesAndViews;
            return this;
        }

        public AuditEventBuilder setChosenMViews(String chosenMViews) {
            auditEvent.chosenMViews = chosenMViews;
            return this;
        }

        public AuditEvent build() {
            return this.auditEvent;
        }
    }
}
