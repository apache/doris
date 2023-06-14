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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Cmp.java
// and modified by Doris

package org.apache.doris.hplsql;

import org.apache.doris.hplsql.executor.Metadata;
import org.apache.doris.hplsql.executor.QueryExecutor;
import org.apache.doris.hplsql.executor.QueryResult;

import org.antlr.v4.runtime.ParserRuleContext;

import java.math.BigDecimal;
import java.util.List;

public class Cmp implements Runnable {

    Exec exec;
    private QueryExecutor queryExecutor;
    Timer timer = new Timer();
    boolean trace = false;
    boolean info = false;

    String query;
    String conn;
    org.apache.doris.hplsql.HplsqlParser.Cmp_stmtContext ctx;

    int tests = 0;
    int failedTests = 0;
    int failedTestsHighDiff = 0;
    int failedTestsHighDiff10 = 0;
    private QueryResult result;

    Cmp(Exec e, QueryExecutor queryExecutor) {
        exec = e;
        trace = exec.getTrace();
        info = exec.getInfo();
        this.queryExecutor = queryExecutor;
    }

    Cmp(Exec e, org.apache.doris.hplsql.HplsqlParser.Cmp_stmtContext c, String q, String cn,
            QueryExecutor queryExecutor) {
        exec = e;
        trace = exec.getTrace();
        info = exec.getInfo();
        ctx = c;
        query = q;
        conn = cn;
        this.queryExecutor = queryExecutor;
    }

    /**
     * Run CMP command
     */
    Integer run(org.apache.doris.hplsql.HplsqlParser.Cmp_stmtContext ctx) {
        trace(ctx, "CMP");
        this.ctx = ctx;
        timer.start();
        StringBuilder conn1 = new StringBuilder();
        StringBuilder conn2 = new StringBuilder();
        Boolean equal = null;
        Cmp cmp1 = null;
        Cmp cmp2 = null;
        try {
            String sql1 = getSql(ctx, conn1, 0);
            String sql2 = getSql(ctx, conn2, 1);
            if (trace) {
                trace(ctx, "Query 1: " + sql1);
                trace(ctx, "Query 2: " + sql2);
            }
            cmp1 = new Cmp(exec, ctx, sql1, conn1.toString(), queryExecutor);
            cmp2 = new Cmp(exec, ctx, sql2, conn2.toString(), queryExecutor);
            cmp1.run();
            cmp2.run();
            equal = compare(cmp1.result, cmp2.result);
        } catch (Exception e) {
            exec.signal(e);
            return -1;
        } finally {
            timer.stop();
            if (info) {
                String message = "CMP ";
                if (equal != null) {
                    if (equal) {
                        message += "Equal, " + tests + " tests";
                    } else {
                        message += "Not Equal, " + failedTests + " of " + tests + " tests failed";
                        message += ", " + failedTestsHighDiff + " tests with more than 0.01% difference";
                        message += ", " + failedTestsHighDiff10 + " tests with more than 10% difference";
                    }
                } else {
                    message += "Failed";
                }
                info(ctx, message + ", " + timer.format());
            }
            cmp1.closeQuery();
            cmp2.closeQuery();
        }
        return 0;
    }

    private void closeQuery() {
        if (result != null) {
            result.close();
        }
    }

    /**
     * Get data for comparison from the source
     */
    @Override
    public void run() {
        result = queryExecutor.executeQuery(query, ctx);
    }

    /**
     * Compare the results
     */
    Boolean compare(QueryResult query1, QueryResult query2) {
        if (query1.error()) {
            exec.signal(query1);
            return null;
        } else if (query2.error()) {
            exec.signal(query2);
            return null;
        }
        boolean equal = true;
        tests = 0;
        failedTests = 0;
        try {
            Metadata rm1 = query1.metadata();
            Metadata rm2 = query2.metadata();
            int cnt1 = rm1.columnCount();
            int cnt2 = rm2.columnCount();
            tests = cnt1;
            while (query1.next() && query2.next()) {
                for (int i = 0; i < tests; i++) {
                    Var v1 = new Var(org.apache.doris.hplsql.Var.Type.DERIVED_TYPE);
                    Var v2 = new Var(org.apache.doris.hplsql.Var.Type.DERIVED_TYPE);
                    v1.setValue(query1, i);
                    if (i < cnt2) {
                        v2.setValue(query2, i);
                    }
                    boolean e = true;
                    if (!(v1.isNull() && v2.isNull()) && !v1.equals(v2)) {
                        equal = false;
                        e = false;
                        failedTests++;
                    }
                    if (trace || info) {
                        String m = rm1.columnName(i) + "\t" + v1 + "\t" + v2;
                        if (!e) {
                            m += "\tNot equal";
                            BigDecimal diff = v1.percentDiff(v2);
                            if (diff != null) {
                                if (diff.compareTo(BigDecimal.ZERO) != 0) {
                                    m += ", " + diff + "% difference";
                                    failedTestsHighDiff++;
                                    if (diff.compareTo(BigDecimal.TEN) > 0) {
                                        failedTestsHighDiff10++;
                                    }
                                } else {
                                    m += ", less then 0.01% difference";
                                }
                            } else {
                                failedTestsHighDiff++;
                                failedTestsHighDiff10++;
                            }
                        }
                        if (trace) {
                            trace(null, m);
                        } else {
                            info(null, m);
                        }
                    }
                }
                if (equal) {
                    exec.setSqlSuccess();
                } else {
                    exec.setSqlCode(1);
                }
            }
        } catch (Exception e) {
            exec.signal(e);
            return null;
        }
        return Boolean.valueOf(equal);
    }

    /**
     * Define the SQL query to access data
     */
    private String getSql(org.apache.doris.hplsql.HplsqlParser.Cmp_stmtContext ctx, StringBuilder conn, int idx)
            throws Exception {
        StringBuilder sql = new StringBuilder();
        String table = null;
        String query = null;
        if (ctx.cmp_source(idx).table_name() != null) {
            table = evalPop(ctx.cmp_source(idx).table_name()).toString();
        } else {
            query = evalPop(ctx.cmp_source(idx).select_stmt()).toString();
        }
        if (ctx.cmp_source(idx).T_AT() != null) {
            conn.append(ctx.cmp_source(idx).qident().getText());
        } else if (table != null) {
            conn.append(exec.getObjectConnection(ctx.cmp_source(idx).table_name().getText()));
        } else {
            conn.append(exec.getStatementConnection());
        }
        sql.append("SELECT ");
        sql.append(getSelectList(ctx, conn.toString(), table));
        sql.append(" FROM ");
        if (table != null) {
            sql.append(table);
            if (ctx.cmp_source(idx).where_clause() != null) {
                sql.append(" " + evalPop(ctx.cmp_source(idx).where_clause()).toString());
            }
        } else {
            sql.append("(");
            sql.append(query);
            sql.append(") t");
        }
        return sql.toString();
    }

    /**
     * Define SELECT listto access data
     */
    private String getSelectList(org.apache.doris.hplsql.HplsqlParser.Cmp_stmtContext ctx, String conn, String table) {
        StringBuilder sql = new StringBuilder();
        sql.append("COUNT(1) AS row_count");
        if (ctx.T_SUM() != null && table != null) {
            Row row = exec.meta.getRowDataType(ctx, conn, table);
            if (row != null) {
                List<Column> cols = row.getColumns();
                int cnt = row.size();
                sql.append(",\n");
                for (int i = 0; i < cnt; i++) {
                    Column col = cols.get(i);
                    String name = col.getName();
                    org.apache.doris.hplsql.Var.Type type
                            = org.apache.doris.hplsql.Var.defineType(col.getType());
                    sql.append("COUNT(" + name + ") AS " + name + "_COUNT_NOT_NULL");
                    if (type == org.apache.doris.hplsql.Var.Type.STRING) {
                        sql.append(",\n");
                        sql.append("SUM(LENGTH(" + name + ")) AS " + name + "_SUM_LENGTH,\n");
                        sql.append("MIN(LENGTH(" + name + ")) AS " + name + "_MIN_LENGTH,\n");
                        sql.append("MAX(LENGTH(" + name + ")) AS " + name + "_MAX_LENGTH");
                    } else if (type == org.apache.doris.hplsql.Var.Type.BIGINT
                            || type == org.apache.doris.hplsql.Var.Type.DECIMAL
                            || type == org.apache.doris.hplsql.Var.Type.DOUBLE) {
                        sql.append(",\n");
                        sql.append("SUM(" + name + ") AS " + name + "_SUM,\n");
                        sql.append("MIN(" + name + ") AS " + name + "_MIN,\n");
                        sql.append("MAX(" + name + ") AS " + name + "_MAX");
                    } else if (type == org.apache.doris.hplsql.Var.Type.DATE
                            || type == org.apache.doris.hplsql.Var.Type.TIMESTAMP) {
                        sql.append(",\n");
                        sql.append("SUM(YEAR(" + name + ")) AS " + name + "_SUM_YEAR,\n");
                        sql.append("SUM(MONTH(" + name + ")) AS " + name + "_SUM_MONTH,\n");
                        sql.append("SUM(DAY(" + name + ")) AS " + name + "_SUM_DAY,\n");
                        sql.append("MIN(" + name + ") AS " + name + "_MIN,\n");
                        sql.append("MAX(" + name + ") AS " + name + "_MAX");
                    }
                    if (i + 1 < cnt) {
                        sql.append(",\n");
                    }
                }
            }
        }
        return sql.toString();
    }

    /**
     * Evaluate the expression and pop value from the stack
     */
    private Var evalPop(ParserRuleContext ctx) {
        exec.visit(ctx);
        if (!exec.stack.isEmpty()) {
            return exec.stackPop();
        }
        return org.apache.doris.hplsql.Var.Empty;
    }

    /**
     * Trace and information
     */
    private void trace(ParserRuleContext ctx, String message) {
        exec.trace(ctx, message);
    }

    private void info(ParserRuleContext ctx, String message) {
        exec.info(ctx, message);
    }
}
