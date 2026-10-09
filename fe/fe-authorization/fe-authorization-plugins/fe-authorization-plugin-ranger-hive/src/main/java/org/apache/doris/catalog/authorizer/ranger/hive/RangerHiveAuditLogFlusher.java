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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

public class RangerHiveAuditLogFlusher extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(RangerHiveAuditLogFlusher.class);
    private RangerHiveAuditHandler auditHandler;

    public RangerHiveAuditLogFlusher(RangerHiveAuditHandler auditHandler) {
        this.auditHandler = auditHandler;
    }

    @Override
    public void run() {
        try {
            this.auditHandler.flushAudit();
        } catch (Throwable t) {
            // ScheduledThreadPoolExecutor suppresses every later fixed-rate invocation when one run escapes with
            // an exception. Keep the periodic flusher alive; RangerHiveAuditHandler retains undelivered events.
            LOG.warn("Failed to flush Ranger Hive audit events; will retry on the next tick", t);
        }
    }
}
