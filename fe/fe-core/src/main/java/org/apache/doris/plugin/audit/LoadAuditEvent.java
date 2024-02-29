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

public class LoadAuditEvent extends AuditEvent {

    @AuditField(value = "JobId")
    public long jobId = -1;
    @AuditField(value = "Label")
    public String label = "";
    @AuditField(value = "LoadType")
    public String loadType = "";
    @AuditField(value = "TableList")
    public String tableList = "";
    @AuditField(value = "FilePathList")
    public String filePathList = "";
    // for Baidu HDFS/AFS it is username
    // for BOS, it is bos_accesskey
    // for Apache HDFS, it is username or kerberos_principal
    // for Amazon S3, it is fs.s3a.access.key
    @AuditField(value = "BrokerUser")
    public String brokerUser = "";
    @AuditField(value = "LoadStartTime")
    public long loadStartTime = -1;
    @AuditField(value = "LoadFinishTime")
    public long loadFinishTime = -1;
    @AuditField(value = "FileNumber")
    public long fileNumber = -1;

    public static class AuditEventBuilder {

        private LoadAuditEvent auditEvent = new LoadAuditEvent();

        public AuditEventBuilder() {
        }

        public void reset() {
            auditEvent = new LoadAuditEvent();
        }

        public AuditEventBuilder setEventType(EventType eventType) {
            auditEvent.type = eventType;
            return this;
        }

        public AuditEventBuilder setJobId(long jobId) {
            auditEvent.jobId = jobId;
            return this;
        }

        public AuditEventBuilder setLabel(String label) {
            auditEvent.label = label;
            return this;
        }

        public AuditEventBuilder setLoadType(String loadType) {
            auditEvent.loadType = loadType;
            return this;
        }

        public AuditEventBuilder setDb(String db) {
            auditEvent.db = db;
            return this;
        }

        public AuditEventBuilder setTableList(String tableList) {
            auditEvent.tableList = tableList;
            return this;
        }

        public AuditEventBuilder setFilePathList(String filePathList) {
            auditEvent.filePathList = filePathList;
            return this;
        }

        public AuditEventBuilder setBrokerUser(String brokerUser) {
            auditEvent.brokerUser = brokerUser;
            return this;
        }

        public AuditEventBuilder setTimestamp(long timestamp) {
            auditEvent.timestamp = timestamp;
            return this;
        }

        public AuditEventBuilder setLoadStartTime(long loadStartTime) {
            auditEvent.loadStartTime = loadStartTime;
            return this;
        }

        public AuditEventBuilder setLoadFinishTime(long loadFinishTime) {
            auditEvent.loadFinishTime = loadFinishTime;
            return this;
        }

        public AuditEventBuilder setScanRows(long scanRows) {
            auditEvent.scanRows = scanRows;
            return this;
        }

        public AuditEventBuilder setScanBytes(long scanBytes) {
            auditEvent.scanBytes = scanBytes;
            return this;
        }

        public AuditEventBuilder setFileNumber(long fileNumber) {
            auditEvent.fileNumber = fileNumber;
            return this;
        }

        public AuditEvent build() {
            return this.auditEvent;
        }
    }
}
