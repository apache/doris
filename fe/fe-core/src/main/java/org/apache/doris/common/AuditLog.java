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

package org.apache.doris.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;

public class AuditLog {

    public static final AuditLog SLOW_AUDIT = new AuditLog("audit.slow_query");
    public static final AuditLog QUERY_AUDIT = new AuditLog("audit.query");
    public static final AuditLog LOAD_AUDIT = new AuditLog("audit.load");
    public static final AuditLog STREAM_LOAD_AUDIT = new AuditLog("audit.stream_load");

    private Logger logger;

    public static AuditLog getQueryAudit() {
        return QUERY_AUDIT;
    }

    public static AuditLog getSlowAudit() {
        return SLOW_AUDIT;
    }

    public static AuditLog getLoadAudit() {
        return LOAD_AUDIT;
    }

    public static AuditLog getStreamLoadAudit() {
        return STREAM_LOAD_AUDIT;
    }

    public AuditLog(String auditName) {
        logger = LogManager.getLogger(auditName);
    }

    public void log(Object message) {
        logger.info(message);
    }

    public void log(String message) {
        logger.info(message);
    }

    public void log(String message, Object... params) {
        logger.info(message, params);
    }

    public void log(Message message) {
        logger.info(message);
    }

}
