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

package org.apache.doris.httpv2.websql;

import org.apache.doris.common.Config;

public class WebSqlLimits {
    private static final int NO_JDBC_TIMEOUT_OVERRIDE = 0;
    private static final int FAIL_FAST_LOCK_WAIT_MILLIS = 0;
    private static final int MAX_REQUESTS_IN_OR_WAITING_FOR_A_SESSION = 1;
    private static final int MAX_SESSIONS_PER_USER = 5;
    private static final int MAX_RESULT_ROWS = 10000;
    public final boolean enabled;
    public final long idleTimeoutMillis;
    public final int maxSessions;
    public final int maxSessionsPerUser;
    public final int maxResultRows;
    public final long maxResultBytes;
    public final int statementTimeoutSeconds;
    public final int lockWaitTimeoutMillis;
    public final int maxQueuedStatements;
    public final int cleanupIntervalSeconds;

    public WebSqlLimits(boolean enabled, long idleTimeoutMillis, int maxSessions, int maxSessionsPerUser,
            int maxResultRows, long maxResultBytes, int statementTimeoutSeconds, int lockWaitTimeoutMillis,
            int maxQueuedStatements, int cleanupIntervalSeconds) {
        this.enabled = enabled;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.maxSessions = maxSessions;
        this.maxSessionsPerUser = maxSessionsPerUser;
        this.maxResultRows = maxResultRows;
        this.maxResultBytes = maxResultBytes;
        this.statementTimeoutSeconds = statementTimeoutSeconds;
        this.lockWaitTimeoutMillis = lockWaitTimeoutMillis;
        this.maxQueuedStatements = maxQueuedStatements;
        this.cleanupIntervalSeconds = cleanupIntervalSeconds;
    }

    public static WebSqlLimits fromConfig() {
        int cleanupIntervalSeconds = Math.max(1,
                Math.min(60, Config.web_sql_session_idle_timeout_seconds / 2));
        return new WebSqlLimits(Config.enable_web_sql_session,
                Config.web_sql_session_idle_timeout_seconds * 1000L,
                Config.web_sql_max_sessions, Math.min(Config.web_sql_max_sessions, MAX_SESSIONS_PER_USER),
                MAX_RESULT_ROWS, Config.web_sql_max_result_bytes,
                NO_JDBC_TIMEOUT_OVERRIDE, FAIL_FAST_LOCK_WAIT_MILLIS,
                MAX_REQUESTS_IN_OR_WAITING_FOR_A_SESSION, cleanupIntervalSeconds);
    }
}
