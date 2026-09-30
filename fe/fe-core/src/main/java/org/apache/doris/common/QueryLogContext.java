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

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.ThreadContext;

import java.util.concurrent.Executor;

/** Query identity for runtime LogEvents, independent of connection and query object lifetimes. */
public final class QueryLogContext implements AutoCloseable {
    public static final String QUERY_ID = "query_id";
    private static final QueryLogContext NOOP = new QueryLogContext();

    private final String previousQueryId;
    private final boolean installed;

    private QueryLogContext() {
        previousQueryId = null;
        installed = false;
    }

    private QueryLogContext(String queryId) {
        previousQueryId = ThreadContext.get(QUERY_ID);
        installed = true;
        set(queryId);
    }

    public static QueryLogContext open(TUniqueId queryId) {
        return Config.sys_log_enable_query_id ? new QueryLogContext(format(queryId)) : NOOP;
    }

    public static void setQueryId(TUniqueId queryId) {
        if (Config.sys_log_enable_query_id) {
            set(format(queryId));
        }
    }

    public static void clear() {
        if (Config.sys_log_enable_query_id) {
            ThreadContext.remove(QUERY_ID);
        }
    }

    /** Capture the value now, before a retry or connection reuse can change the original ID. */
    public static Executor executor(Executor delegate, TUniqueId queryId) {
        if (!Config.sys_log_enable_query_id) {
            return delegate;
        }
        String capturedQueryId = format(queryId);
        return command -> delegate.execute(wrap(command, capturedQueryId));
    }

    public static Runnable wrap(Runnable command, TUniqueId queryId) {
        return Config.sys_log_enable_query_id ? wrap(command, format(queryId)) : command;
    }

    private static Runnable wrap(Runnable command, String capturedQueryId) {
        return () -> {
            try (QueryLogContext ignored = new QueryLogContext(capturedQueryId)) {
                command.run();
            }
        };
    }

    private static String format(TUniqueId queryId) {
        return queryId == null || (queryId.hi == 0 && queryId.lo == 0) ? null : DebugUtil.printId(queryId);
    }

    private static void set(String queryId) {
        if (queryId == null) {
            ThreadContext.remove(QUERY_ID);
        } else {
            ThreadContext.put(QUERY_ID, queryId);
        }
    }

    @Override
    public void close() {
        if (installed) {
            set(previousQueryId);
        }
    }
}
