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

package org.apache.doris.httpv2.util;

import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.httpv2.util.streamresponse.JsonStreamResponse;
import org.apache.doris.httpv2.util.streamresponse.StreamResponseInf;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

/**
 * This is a simple stmt submitter for submitting a statement to the local FE.
 * It uses a fixed-size thread pool to receive query requests,
 * so it is only suitable for a small number of low-frequency request scenarios.
 * <p>
 * This submitter can execute any SQL statement without pre-analysis or type checking.
 * It dynamically determines how to handle the results based on whether the statement
 * returns a ResultSet or not:
 *   - Statements with ResultSet (SELECT, SHOW, etc.): processed as query results
 *   - Statements without ResultSet (DDL, DML, etc.): processed as execution status
 * <p>
 * Supported statement types include but are not limited to:
 *   - Query statements (SELECT, SHOW, DESCRIBE, etc.)
 *   - Data manipulation (INSERT, UPDATE, DELETE, COPY, etc.)
 *   - Data definition (CREATE, DROP, ALTER, etc.)
 *   - Session management (SET, USE, etc.)
 *   - Export statements
 */
public class StatementSubmitter {
    private static final Logger LOG = LogManager.getLogger(StatementSubmitter.class);

    private static final String TYPE_RESULT_SET = "result_set";
    private static final String TYPE_EXEC_STATUS = "exec_status";

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mariadb://127.0.0.1:%d/%s";

    private static final String[] copyResult = {"id", "state", "type", "msg", "loadedRows", "filterRows",
            "unselectRows", "url"};

    private final ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPoolThrowException(
                        Config.http_sql_submitter_max_worker_threads, "SQL submitter", true);

    private ThreadPoolExecutor executorBlockPolicy = ThreadPoolManager.newDaemonCacheThreadPoolUseBlockedPolicy(
            Config.cloud_copy_into_statement_submitter_threads_num, "SQL submitter with block policy", true);

    public Future<ExecutionResultSet> submit(StmtContext queryCtx) {
        Worker worker = new Worker(ConnectContext.get(), queryCtx, null, true);
        return executor.submit(worker);
    }

    /**
     * Executes on a caller-owned connection. The connection is intentionally not closed, so callers can
     * preserve JDBC session state across statements.
     */
    public ExecutionResultSet execute(Connection connection, StmtContext queryCtx) throws Exception {
        return new Worker(null, queryCtx, connection, false).call();
    }

    public Future<ExecutionResultSet> submitBlock(StmtContext queryCtx) {
        LOG.debug("submitBlock {}", queryCtx);
        Worker worker = new Worker(ConnectContext.get(), queryCtx, null, true);
        return executorBlockPolicy.submit(worker);
    }

    private static class Worker implements Callable<ExecutionResultSet> {
        private final ConnectContext ctx;
        private final StmtContext queryCtx;
        private final Connection suppliedConnection;
        private final boolean closeConnection;

        public Worker(ConnectContext ctx, StmtContext queryCtx, Connection suppliedConnection,
                boolean closeConnection) {
            this.ctx = ctx;
            this.queryCtx = queryCtx;
            this.suppliedConnection = suppliedConnection;
            this.closeConnection = closeConnection;
        }

        @Override
        public ExecutionResultSet call() throws Exception {
            Connection conn = suppliedConnection;
            Statement stmt = null;
            try {
                if (conn == null) {
                    String dbUrl = String.format(DB_URL_PATTERN, Config.query_port, ctx.getDatabase());
                    Class.forName(JDBC_DRIVER);
                    conn = DriverManager.getConnection(dbUrl, queryCtx.user, queryCtx.passwd);
                }
                long startTime = System.currentTimeMillis();

                if (!queryCtx.clusterName.isEmpty()) {
                    Statement useStmt = conn.createStatement();
                    useStmt.execute("use @" + queryCtx.clusterName);
                    useStmt.close();
                }

                stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(1000);
                if (queryCtx.queryTimeoutSeconds > 0) {
                    stmt.setQueryTimeout(queryCtx.queryTimeoutSeconds);
                }
                queryCtx.statementObserver.accept(stmt);

                boolean hasResultSet = stmt.execute(queryCtx.stmt);

                if (hasResultSet) {
                    ResultSet rs = stmt.getResultSet();
                    if (queryCtx.isStream) {
                        StreamResponseInf streamResponse = new JsonStreamResponse(queryCtx.response);
                        streamResponse.handleQueryAndShow(rs, startTime);
                        rs.close();
                        return new ExecutionResultSet(null);
                    }
                    boolean isCopyStmt = isCopyStatement(rs);
                    ExecutionResultSet resultSet = generateResultSet(rs, startTime, isCopyStmt);
                    rs.close();
                    addWarnings(resultSet, stmt);
                    return resultSet;
                } else {
                    if (queryCtx.isStream) {
                        StreamResponseInf streamResponse = new JsonStreamResponse(queryCtx.response);
                        streamResponse.handleDdlAndExport(startTime);
                        return new ExecutionResultSet(null);
                    }
                    ExecutionResultSet resultSet = generateExecStatus(startTime, stmt.getUpdateCount());
                    addWarnings(resultSet, stmt);
                    return resultSet;
                }
            } finally {
                queryCtx.statementObserver.accept(null);
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException se2) {
                    LOG.warn("failed to close stmt", se2);
                }
                try {
                    if (closeConnection && conn != null) {
                        conn.close();
                    }
                } catch (SQLException se) {
                    LOG.warn("failed to close connection", se);
                }
            }
        }

        private boolean isCopyStatement(ResultSet rs) throws SQLException {
            if (rs == null) {
                return false;
            }

            ResultSetMetaData metaData = rs.getMetaData();
            int colNum = metaData.getColumnCount();

            if (colNum != copyResult.length) {
                return false;
            }

            for (int i = 1; i <= colNum; i++) {
                String columnName = metaData.getColumnName(i);
                if (!copyResult[i - 1].equalsIgnoreCase(columnName)) {
                    return false;
                }
            }

            return true;
        }

        /**
         * Result json sample:
         * {
         * "type": "result_set",
         * "data": [
         * [1],
         * [2]
         * ],
         * "meta": [{
         * "name": "k1",
         * "type": "INT"
         * }],
         * "status": {},
         * "time" : 10
         * }
         */
        private ExecutionResultSet generateResultSet(ResultSet rs, long startTime, boolean isCopyStmt)
                throws SQLException {
            Map<String, Object> result = Maps.newHashMap();
            result.put("type", TYPE_RESULT_SET);
            if (rs == null) {
                return new ExecutionResultSet(result);
            }
            ResultSetMetaData metaData = rs.getMetaData();
            int colNum = metaData.getColumnCount();
            if (!isCopyStmt) {
                // 1. metadata
                List<Map<String, String>> metaFields = Lists.newArrayList();
                // index start from 1
                for (int i = 1; i <= colNum; ++i) {
                    Map<String, String> field = Maps.newHashMap();
                    field.put("name", metaData.getColumnName(i));
                    field.put("type", metaData.getColumnTypeName(i));
                    metaFields.add(field);
                }
                result.put("meta", metaFields);
            }
            // 2. data
            List<List<Object>> rows = Lists.newArrayList();
            long rowCount = 0;
            Map<String, String> copyResultFields = Maps.newHashMap();
            boolean truncated = false;
            long resultBytes = 0;
            while (rs.next()) {
                if (rowCount >= queryCtx.limit) {
                    truncated = true;
                    break;
                }
                List<Object> row = Lists.newArrayListWithCapacity(colNum);
                long rowBytes = 0;
                // index start from 1
                for (int i = 1; i <= colNum && (!isCopyStmt || i <= copyResult.length); ++i) {
                    String type = rs.getMetaData().getColumnTypeName(i);
                    if ("DATE".equalsIgnoreCase(type) || "DATETIME".equalsIgnoreCase(type)
                            || "DATEV2".equalsIgnoreCase(type) || "DATETIMEV2".equalsIgnoreCase(type)) {
                        String value = rs.getString(i);
                        row.add(value);
                        rowBytes += valueSize(value);
                    } else {
                        Object value = rs.getObject(i);
                        row.add(value);
                        rowBytes += valueSize(value);
                    }
                    if (isCopyStmt) {
                        // ATTN: java.sql.SQLException: Wrong index position. Is 0 but must be in 1-7 range
                        copyResultFields.put(copyResult[i - 1], rs.getString(i));
                    }
                }
                if (resultBytes + rowBytes > queryCtx.maxResultBytes) {
                    truncated = true;
                    break;
                }
                rows.add(row);
                resultBytes += rowBytes;
                rowCount++;
            }
            result.put("truncated", truncated);
            if (!isCopyStmt) {
                result.put("data", rows);
            } else {
                result.put("result", copyResultFields);
            }
            result.put("time", (System.currentTimeMillis() - startTime));
            return new ExecutionResultSet(result);
        }

        /**
         * Result json sample:
         * {
         *  "type": "exec_status",
         *  "status": {},
         *  "time" : 10
         * }
         */
        private ExecutionResultSet generateExecStatus(long startTime, int affectedRows) {
            Map<String, Object> result = Maps.newHashMap();
            result.put("type", TYPE_EXEC_STATUS);
            result.put("status", Maps.newHashMap());
            result.put("affectedRows", Math.max(affectedRows, 0));
            result.put("time", (System.currentTimeMillis() - startTime));
            return new ExecutionResultSet(result);
        }

        private void addWarnings(ExecutionResultSet resultSet, Statement statement) throws SQLException {
            if (resultSet == null || resultSet.getResult() == null) {
                return;
            }
            List<String> warnings = Lists.newArrayList();
            java.sql.SQLWarning warning = statement.getWarnings();
            while (warning != null) {
                warnings.add(warning.getMessage());
                warning = warning.getNextWarning();
            }
            resultSet.getResult().put("warnings", warnings);
        }

        private int valueSize(Object value) {
            return value == null ? 4 : String.valueOf(value).getBytes(StandardCharsets.UTF_8).length;
        }
    }

    public static class StmtContext {
        public String stmt;
        public String user;
        public String passwd;
        public long limit; // limit the number of rows returned by the stmt
        // used for stream Work
        public boolean isStream;
        public HttpServletResponse response;
        public String clusterName;
        public int queryTimeoutSeconds;
        public long maxResultBytes = Long.MAX_VALUE;
        public Consumer<Statement> statementObserver = ignored -> { };

        public StmtContext(String stmt, String user, String passwd, long limit,
                            boolean isStream, HttpServletResponse response, String clusterName) {
            this.stmt = stmt;
            this.user = user;
            this.passwd = passwd;
            this.limit = limit;
            this.isStream = isStream;
            this.response = response;
            this.clusterName = clusterName;
        }

        public StmtContext withQueryTimeoutSeconds(int timeoutSeconds) {
            this.queryTimeoutSeconds = timeoutSeconds;
            return this;
        }

        public StmtContext withStatementObserver(Consumer<Statement> observer) {
            this.statementObserver = observer == null ? ignored -> { } : observer;
            return this;
        }

        public StmtContext withMaxResultBytes(long byteLimit) {
            this.maxResultBytes = byteLimit;
            return this;
        }
    }
}
