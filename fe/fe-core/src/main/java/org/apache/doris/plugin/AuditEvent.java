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
    public static @interface AuditField {
        String value() default "";
    }

    public EventType type;

    // all fields which is about to be audit should be annotated by "@AuditField"
    // make them all "public" so that easy to visit.
    @AuditField(value = "Timestamp")
    public long timestamp = -1;
    @AuditField(value = "Client")
    public String clientIp = "";
    @AuditField(value = "User")
    public String user = "";
    @AuditField(value = "Catalog")
    public String catalog = "";
    @AuditField(value = "Db")
    public String db = "";
    @AuditField(value = "State")
    public String state = "";
    @AuditField(value = "ErrorCode")
    public int errorCode = 0;
    @AuditField(value = "ErrorMessage")
    public String errorMessage = "";
    @AuditField(value = "Time")
    public long queryTime = -1;
    @AuditField(value = "ScanBytes")
    public long scanBytes = -1;
    @AuditField(value = "ScanRows")
    public long scanRows = -1;
    @AuditField(value = "ReturnRows")
    public long returnRows = -1;
    @AuditField(value = "StmtId")
    public long stmtId = -1;
    @AuditField(value = "QueryId")
    public String queryId = "";
    @AuditField(value = "IsQuery")
    public boolean isQuery = false;
    @AuditField(value = "feIp")
    public String feIp = "";
    @AuditField(value = "Stmt")
    public String stmt = "";
    @AuditField(value = "CpuTimeMS")
    public long cpuTimeMs = -1;
    @AuditField(value = "SqlHash")
    public String sqlHash = "";
    @AuditField(value = "peakMemoryBytes")
    public long peakMemoryBytes = -1;
    @AuditField(value = "SqlDigest")
    public String sqlDigest = "";
    @AuditField(value = "TraceId")
    public String traceId = "";
    @AuditField(value = "FuzzyVariables")
    public String fuzzyVariables = "";

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

        public AuditEventBuilder setCatalog(String catalog) {
            auditEvent.catalog = catalog;
            return this;
        }

        public AuditEventBuilder setDb(String db) {
            auditEvent.db = db;
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

        public AuditEventBuilder setFeIp(String feIp) {
            auditEvent.feIp = feIp;
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

        public AuditEventBuilder setTraceId(String traceId) {
            auditEvent.traceId = traceId;
            return this;
        }

        public AuditEventBuilder setFuzzyVariables(String variables) {
            auditEvent.fuzzyVariables = variables;
            return this;
        }

        public AuditEvent build() {
            return this.auditEvent;
        }
    }
}
