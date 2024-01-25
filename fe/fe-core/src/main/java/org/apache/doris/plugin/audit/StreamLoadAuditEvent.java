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

package org.apache.doris.plugin.audit;

public class StreamLoadAuditEvent extends AuditEvent {

    @AuditField(value = "Label")
    public String label = "";
    @AuditField(value = "Table")
    public String table = "";
    @AuditField(value = "ClientIp")
    public String clientIp = "";
    @AuditField(value = "Status")
    public String status = "";
    @AuditField(value = "Message")
    public String message = "";
    @AuditField(value = "Url")
    public String url = "";
    @AuditField(value = "TotalRows")
    public long totalRows = -1;
    @AuditField(value = "LoadedRows")
    public long loadedRows = -1;
    @AuditField(value = "FilteredRows")
    public long filteredRows = -1;
    @AuditField(value = "UnselectedRows")
    public long unselectedRows = -1;
    @AuditField(value = "LoadBytes")
    public long loadBytes = -1;
    @AuditField(value = "StartTime")
    public String startTime = "";
    @AuditField(value = "FinishTime")
    public String finishTime = "";

    public static class AuditEventBuilder {

        private StreamLoadAuditEvent auditEvent = new StreamLoadAuditEvent();

        public AuditEventBuilder() {
        }

        public void reset() {
            auditEvent = new StreamLoadAuditEvent();
        }

        public AuditEventBuilder setEventType(EventType eventType) {
            auditEvent.type = eventType;
            return this;
        }

        public AuditEventBuilder setLabel(String label) {
            auditEvent.label = label;
            return this;
        }

        public AuditEventBuilder setDb(String db) {
            auditEvent.db = db;
            return this;
        }

        public AuditEventBuilder setTable(String table) {
            auditEvent.table = table;
            return this;
        }

        public AuditEventBuilder setUser(String user) {
            auditEvent.user = user;
            return this;
        }

        public AuditEventBuilder setClientIp(String clientIp) {
            auditEvent.clientIp = clientIp;
            return this;
        }

        public AuditEventBuilder setStatus(String status) {
            auditEvent.status = status;
            return this;
        }

        public AuditEventBuilder setMessage(String message) {
            auditEvent.message = message;
            return this;
        }

        public AuditEventBuilder setUrl(String url) {
            auditEvent.url = url;
            return this;
        }

        public AuditEventBuilder setTotalRows(long totalRows) {
            auditEvent.totalRows = totalRows;
            return this;
        }

        public AuditEventBuilder setLoadedRows(long loadedRows) {
            auditEvent.loadedRows = loadedRows;
            return this;
        }

        public AuditEventBuilder setFilteredRows(long filteredRows) {
            auditEvent.filteredRows = filteredRows;
            return this;
        }

        public AuditEventBuilder setUnselectedRows(long unselectedRows) {
            auditEvent.unselectedRows = unselectedRows;
            return this;
        }

        public AuditEventBuilder setLoadBytes(long loadBytes) {
            auditEvent.loadBytes = loadBytes;
            return this;
        }

        public AuditEventBuilder setStartTime(String startTime) {
            auditEvent.startTime = startTime;
            return this;
        }

        public AuditEventBuilder setFinishTime(String finishTime) {
            auditEvent.finishTime = finishTime;
            return this;
        }

        public AuditEvent build() {
            return this.auditEvent;
        }
    }
}
