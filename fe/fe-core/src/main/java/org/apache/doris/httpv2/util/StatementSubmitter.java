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


import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This is a simple stmt submitter for submitting a statement to the local FE.
 * It uses a fixed-size thread pool to receive query requests,
 * so it is only suitable for a small number of low-frequency request scenarios.
 * Now it support submitting the following type of stmt:
 *      QueryStmt
 *      ShowStmt
 *      InsertStmt
 *      DdlStmt
 *      ExportStmt
 */
public class StatementSubmitter {
    private static final Logger LOG = LogManager.getLogger(StatementSubmitter.class);

    private static final String TYPE_RESULT_SET = "result_set";
    private static final String TYPE_EXEC_STATUS = "exec_status";

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://127.0.0.1:%d/%s";

    private ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPool(2, "SQL submitter", true);

    public Future<ExecutionResultSet> submit(StmtContext queryCtx) {
        Worker worker = new Worker(ConnectContext.get(), queryCtx);
        return executor.submit(worker);
    }

    private static class Worker implements Callable<ExecutionResultSet> {

        private ConnectContext ctx;
        private StmtContext queryCtx;

        public Worker(ConnectContext ctx, StmtContext queryCtx) {
            this.ctx = ctx;
            this.queryCtx = queryCtx;
        }

        @Override
        public ExecutionResultSet call() throws Exception {
            StatementBase stmtBase = analyzeStmt(queryCtx.stmt);

            Connection conn = null;
            Statement stmt = null;
            String dbUrl = String.format(DB_URL_PATTERN, Config.query_port, ctx.getDatabase());
            try {
                Class.forName(JDBC_DRIVER);
                conn = DriverManager.getConnection(dbUrl, queryCtx.user, queryCtx.passwd);
                long startTime = System.currentTimeMillis();
                if (stmtBase instanceof QueryStmt || stmtBase instanceof ShowStmt) {
                    stmt = conn.prepareStatement(queryCtx.stmt, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    // set fetch size to MIN_VALUE to enable streaming result set to avoid OOM.
                    ((PreparedStatement) stmt).setFetchSize(Integer.MIN_VALUE);
                    ResultSet rs = ((PreparedStatement) stmt).executeQuery();
                    ExecutionResultSet resultSet = generateResultSet(rs, startTime);
                    rs.close();
                    return resultSet;
                } else if (stmtBase instanceof InsertStmt || stmtBase instanceof DdlStmt || stmtBase instanceof ExportStmt) {
                    stmt = conn.createStatement();
                    stmt.execute(queryCtx.stmt);
                    ExecutionResultSet resultSet = generateExecStatus(startTime);
                    return resultSet;
                } else {
                    throw new Exception("Unsupported statement type");
                }
            } finally {
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException se2) {
                    LOG.warn("failed to close stmt", se2);
                }
                try {
                    if (conn != null) conn.close();
                } catch (SQLException se) {
                    LOG.warn("failed to close connection", se);
                }
            }
        }

        /**
         * Result json sample:
         * {
         *  "type": "result_set",
         *  "data": [
         *      [1],
         *      [2]
         *  ],
         *  "meta": [{
         *      "name": "k1",
         *      "type": "INT"
         *        }],
         *  "status": {},
         *  "time" : 10
         * }
         */
        private ExecutionResultSet generateResultSet(ResultSet rs, long startTime) throws SQLException {
            Map<String, Object> result = Maps.newHashMap();
            result.put("type", TYPE_RESULT_SET);
            if (rs == null) {
                return new ExecutionResultSet(result);
            }
            ResultSetMetaData metaData = rs.getMetaData();
            int colNum = metaData.getColumnCount();
            // 1. metadata
            List<Map<String, String>> metaFields = Lists.newArrayList();
            // index start from 1
            for (int i = 1; i <= colNum; ++i) {
                Map<String, String> field = Maps.newHashMap();
                field.put("name", metaData.getColumnName(i));
                field.put("type", metaData.getColumnTypeName(i));
                metaFields.add(field);
            }
            // 2. data
            List<List<Object>> rows = Lists.newArrayList();
            long rowCount = 0;
            while (rs.next() && rowCount < queryCtx.limit) {
                List<Object> row = Lists.newArrayListWithCapacity(colNum);
                // index start from 1
                for (int i = 1; i <= colNum; ++i) {
                    row.add(rs.getObject(i));
                }
                rows.add(row);
                rowCount++;
            }
            result.put("meta", metaFields);
            result.put("data", rows);
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
        private ExecutionResultSet generateExecStatus(long startTime) throws SQLException {
            Map<String, Object> result = Maps.newHashMap();
            result.put("type", TYPE_EXEC_STATUS);
            result.put("status", Maps.newHashMap());
            result.put("time", (System.currentTimeMillis() - startTime));
            return new ExecutionResultSet(result);
        }

        private StatementBase analyzeStmt(String stmtStr) throws Exception {
            SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmtStr)));
            try {
                return SqlParserUtils.getFirstStmt(parser);
            } catch (AnalysisException e) {
                String errorMessage = parser.getErrorMsg(stmtStr);
                if (errorMessage == null) {
                    throw e;
                } else {
                    throw new AnalysisException(errorMessage, e);
                }
            } catch (Exception e) {
                throw new Exception("error happens when parsing stmt: " + e.getMessage());
            }
        }
    }

    public static class StmtContext {
        public String stmt;
        public String user;
        public String passwd;
        public long limit; // limit the number of rows returned by the stmt

        public StmtContext(String stmt, String user, String passwd, long limit) {
            this.stmt = stmt;
            this.user = user;
            this.passwd = passwd;
            this.limit = limit;
        }
    }
}

