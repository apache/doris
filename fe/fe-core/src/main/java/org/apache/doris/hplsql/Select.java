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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Select.java
// and modified by Doris

package org.apache.doris.hplsql;

import org.apache.doris.hplsql.exception.QueryException;
import org.apache.doris.hplsql.exception.TypeException;
import org.apache.doris.hplsql.exception.UndefinedIdentException;
import org.apache.doris.hplsql.executor.HplsqlResult;
import org.apache.doris.hplsql.executor.QueryExecutor;
import org.apache.doris.hplsql.executor.QueryResult;
import org.apache.doris.hplsql.executor.ResultListener;
import org.apache.doris.hplsql.objects.Table;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Select {
    Exec exec = null;
    Stack<Var> stack = null;
    Conf conf;
    Console console;
    ResultListener resultListener = ResultListener.NONE;
    QueryExecutor queryExecutor;

    boolean trace = false;

    Select(Exec e, QueryExecutor queryExecutor) {
        this.exec = e;
        this.stack = exec.getStack();
        this.conf = exec.getConf();
        this.trace = exec.getTrace();
        this.console = exec.console;
        this.queryExecutor = queryExecutor;
    }

    public void setResultListener(ResultListener resultListener) {
        this.resultListener = resultListener;
    }

    /**
     * Executing or building SELECT statement
     */
    public Integer select(org.apache.doris.hplsql.HplsqlParser.Select_stmtContext ctx) {
        if (ctx.parent instanceof org.apache.doris.hplsql.HplsqlParser.StmtContext) {
            exec.stmtConnList.clear();
            trace(ctx, "SELECT");
        }
        boolean oldBuildSql = exec.buildSql;
        exec.buildSql = true;
        StringBuilder sql = new StringBuilder();
        if (ctx.cte_select_stmt() != null) {
            sql.append(evalPop(ctx.cte_select_stmt()).toString());
            sql.append("\n");
        }
        sql.append(evalPop(ctx.fullselect_stmt()).toString());
        exec.buildSql = oldBuildSql;
        // No need to execute at this stage
        if (!(ctx.parent instanceof org.apache.doris.hplsql.HplsqlParser.StmtContext)) {
            exec.stackPush(sql);
            return 0;
        }
        if (trace) {
            trace(ctx, sql.toString());
        }
        if (exec.getOffline()) {
            trace(ctx, "Not executed - offline mode set");
            return 0;
        }

        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);

        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        trace(ctx, "SELECT completed successfully");
        exec.setSqlSuccess();
        try {
            int intoCount = getIntoCount(ctx);
            if (intoCount > 0) {
                if (isBulkCollect(ctx)) {
                    trace(ctx, "SELECT BULK COLLECT INTO statement executed");
                    long rowIndex = 1;
                    List<Table> tables = exec.intoTables(ctx, intoVariableNames(ctx, intoCount));
                    tables.forEach(Table::removeAll);
                    while (query.next()) {
                        for (int i = 0; i < intoCount; i++) {
                            Table table = tables.get(i);
                            table.populate(query, rowIndex, i);
                        }
                        rowIndex++;
                    }
                } else {
                    trace(ctx, "SELECT INTO statement executed");
                    if (query.next()) {
                        for (int i = 0; i < intoCount; i++) {
                            populateVariable(ctx, query, i);
                        }
                        exec.incRowCount();
                        exec.setSqlSuccess();
                        if (query.next()) {
                            exec.setSqlCode(SqlCodes.TOO_MANY_ROWS);
                            exec.signal(Signal.Type.TOO_MANY_ROWS);
                        }
                    } else {
                        exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
                        exec.signal(Signal.Type.NOTFOUND);
                    }
                }
            } else if (ctx.parent instanceof org.apache.doris.hplsql.HplsqlParser.StmtContext) {
                // Print all results for standalone SELECT statement
                resultListener.onMetadata(query.metadata());
                int cols = query.columnCount();
                if (trace) {
                    trace(ctx, "Standalone SELECT executed: " + cols + " columns in the result set");
                }
                while (query.next()) {
                    if (resultListener instanceof HplsqlResult) {
                        resultListener.onMysqlRow(query.mysqlRow());
                    } else {
                        Object[] row = new Object[cols];
                        for (int i = 0; i < cols; i++) {
                            row[i] = query.column(i, Object.class);
                            if (i > 0) {
                                console.print("\t");
                            }
                            console.print(String.valueOf(row[i]));
                        }
                        console.printLine("");
                        exec.incRowCount();

                        resultListener.onRow(row);
                    }
                }
                resultListener.onEof();
            } else { // Scalar subquery
                trace(ctx, "Scalar subquery executed, first row and first column fetched only");
                if (query.next()) {
                    exec.stackPush(new Var().setValue(query, 1));
                    exec.setSqlSuccess();
                } else {
                    evalNull();
                    exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
                }
            }
        } catch (QueryException e) {
            exec.signal(query);
            query.close();
            return 1;
        }
        query.close();
        return 0;
    }

    private void populateVariable(org.apache.doris.hplsql.HplsqlParser.Select_stmtContext ctx, QueryResult query,
            int columnIndex) {
        String intoName = getIntoVariable(ctx, columnIndex);
        Var var = exec.findVariable(intoName);
        if (var != null) {
            if (var.type == Var.Type.HPL_OBJECT && var.value instanceof Table) {
                Table table = (Table) var.value;
                table.populate(query, getIntoTableIndex(ctx, columnIndex), columnIndex);
            } else if (var.type == Var.Type.ROW) {
                var.setRowValues(query);
            } else {
                var.setValue(query, columnIndex);
            }
            exec.trace(ctx, var, query.metadata(), columnIndex);
        } else {
            throw new UndefinedIdentException(ctx, intoName);
        }
    }

    /**
     * Common table expression (WITH clause)
     */
    public Integer cte(org.apache.doris.hplsql.HplsqlParser.Cte_select_stmtContext ctx) {
        int cnt = ctx.cte_select_stmt_item().size();
        StringBuilder sql = new StringBuilder();
        sql.append("WITH ");
        for (int i = 0; i < cnt; i++) {
            org.apache.doris.hplsql.HplsqlParser.Cte_select_stmt_itemContext c = ctx.cte_select_stmt_item(i);
            sql.append(c.qident().getText());
            if (c.cte_select_cols() != null) {
                sql.append(" ").append(exec.getFormattedText(c.cte_select_cols()));
            }
            sql.append(" AS (");
            sql.append(evalPop(ctx.cte_select_stmt_item(i).fullselect_stmt()).toString());
            sql.append(")");
            if (i + 1 != cnt) {
                sql.append(",\n");
            }
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * Part of SELECT
     */
    public Integer fullselect(org.apache.doris.hplsql.HplsqlParser.Fullselect_stmtContext ctx) {
        int cnt = ctx.fullselect_stmt_item().size();
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < cnt; i++) {
            String part = evalPop(ctx.fullselect_stmt_item(i)).toString();
            sql.append(part);
            if (i + 1 != cnt) {
                sql.append("\n").append(getText(ctx.fullselect_set_clause(i))).append("\n");
            }
        }
        exec.stackPush(sql);
        return 0;
    }

    public Integer subselect(org.apache.doris.hplsql.HplsqlParser.Subselect_stmtContext ctx) {
        StringBuilder sql = new StringBuilder();
        sql.append(ctx.start.getText());
        exec.append(sql, evalPop(ctx.select_list()).toString(), ctx.start, ctx.select_list().getStart());
        Token last = ctx.select_list().stop;
        if (ctx.into_clause() != null) {
            last = ctx.into_clause().stop;
        }
        if (ctx.from_clause() != null) {
            exec.append(sql, evalPop(ctx.from_clause()).toString(), last, ctx.from_clause().getStart());
            last = ctx.from_clause().stop;
        } else if (conf.dualTable != null) {
            sql.append(" FROM ").append(conf.dualTable);
        }
        if (ctx.where_clause() != null) {
            exec.append(sql, evalPop(ctx.where_clause()).toString(), last, ctx.where_clause().getStart());
            last = ctx.where_clause().stop;
        }
        if (ctx.group_by_clause() != null) {
            exec.append(sql, getText(ctx.group_by_clause()), last, ctx.group_by_clause().getStart());
            last = ctx.group_by_clause().stop;
        }
        if (ctx.having_clause() != null) {
            exec.append(sql, getText(ctx.having_clause()), last, ctx.having_clause().getStart());
            last = ctx.having_clause().stop;
        }
        if (ctx.qualify_clause() != null) {
            exec.append(sql, getText(ctx.qualify_clause()), last, ctx.qualify_clause().getStart());
            last = ctx.qualify_clause().stop;
        }
        if (ctx.order_by_clause() != null) {
            exec.append(sql, getText(ctx.order_by_clause()), last, ctx.order_by_clause().getStart());
            last = ctx.order_by_clause().stop;
        }
        if (ctx.select_options() != null) {
            Var opt = evalPop(ctx.select_options());
            if (!opt.isNull()) {
                sql.append(" " + opt.toString());
            }
        }
        if (ctx.select_list().select_list_limit() != null) {
            sql.append(" LIMIT " + evalPop(ctx.select_list().select_list_limit().expr()));
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * SELECT list
     */
    public Integer selectList(org.apache.doris.hplsql.HplsqlParser.Select_listContext ctx) {
        StringBuilder sql = new StringBuilder();
        if (ctx.select_list_set() != null) {
            sql.append(exec.getText(ctx.select_list_set())).append(" ");
        }
        int cnt = ctx.select_list_item().size();
        for (int i = 0; i < cnt; i++) {
            if (ctx.select_list_item(i).select_list_asterisk() == null) {
                sql.append(evalPop(ctx.select_list_item(i).expr()));
                if (ctx.select_list_item(i).select_list_alias() != null) {
                    sql.append(" " + exec.getText(ctx.select_list_item(i).select_list_alias()));
                }
            } else {
                sql.append(exec.getText(ctx.select_list_item(i).select_list_asterisk()));
            }
            if (i + 1 < cnt) {
                sql.append(", ");
            }
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * FROM clause
     */
    public Integer from(org.apache.doris.hplsql.HplsqlParser.From_clauseContext ctx) {
        StringBuilder sql = new StringBuilder();
        sql.append(ctx.T_FROM().getText()).append(" ");
        sql.append(evalPop(ctx.from_table_clause()));
        int cnt = ctx.from_join_clause().size();
        for (int i = 0; i < cnt; i++) {
            sql.append(evalPop(ctx.from_join_clause(i)));
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * Single table name in FROM
     */
    public Integer fromTable(org.apache.doris.hplsql.HplsqlParser.From_table_name_clauseContext ctx) {
        StringBuilder sql = new StringBuilder();
        sql.append(evalPop(ctx.table_name()));
        if (ctx.from_alias_clause() != null) {
            sql.append(" ").append(exec.getText(ctx.from_alias_clause()));
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * Subselect in FROM
     */
    public Integer fromSubselect(org.apache.doris.hplsql.HplsqlParser.From_subselect_clauseContext ctx) {
        StringBuilder sql = new StringBuilder();
        sql.append("(");
        sql.append(evalPop(ctx.select_stmt()).toString());
        sql.append(")");
        if (ctx.from_alias_clause() != null) {
            sql.append(" ").append(exec.getText(ctx.from_alias_clause()));
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * JOIN clause in FROM
     */
    public Integer fromJoin(org.apache.doris.hplsql.HplsqlParser.From_join_clauseContext ctx) {
        StringBuilder sql = new StringBuilder();
        if (ctx.T_COMMA() != null) {
            sql.append(", ");
            sql.append(evalPop(ctx.from_table_clause()));
        } else if (ctx.from_join_type_clause() != null) {
            sql.append(" ");
            sql.append(exec.getText(ctx.from_join_type_clause()));
            sql.append(" ");
            sql.append(evalPop(ctx.from_table_clause()));
            sql.append(" ");
            sql.append(exec.getText(ctx, ctx.T_ON().getSymbol(), ctx.bool_expr().getStop()));
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * FROM TABLE (VALUES ...) clause
     */
    public Integer fromTableValues(org.apache.doris.hplsql.HplsqlParser.From_table_values_clauseContext ctx) {
        StringBuilder sql = new StringBuilder();
        int rows = ctx.from_table_values_row().size();
        sql.append("(");
        for (int i = 0; i < rows; i++) {
            int cols = ctx.from_table_values_row(i).expr().size();
            int colsAs = ctx.from_alias_clause().L_ID().size();
            sql.append("SELECT ");
            for (int j = 0; j < cols; j++) {
                sql.append(evalPop(ctx.from_table_values_row(i).expr(j)));
                if (j < colsAs) {
                    sql.append(" AS ");
                    sql.append(ctx.from_alias_clause().L_ID(j));
                }
                if (j + 1 < cols) {
                    sql.append(", ");
                }
            }
            if (conf.dualTable != null) {
                sql.append(" FROM ").append(conf.dualTable);
            }
            if (i + 1 < rows) {
                sql.append("\nUNION ALL\n");
            }
        }
        sql.append(") ");
        if (ctx.from_alias_clause() != null) {
            sql.append(ctx.from_alias_clause().qident().getText());
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * WHERE clause
     */
    public Integer where(org.apache.doris.hplsql.HplsqlParser.Where_clauseContext ctx) {
        boolean oldBuildSql = exec.buildSql;
        exec.buildSql = true;
        StringBuilder sql = new StringBuilder();
        sql.append(ctx.T_WHERE().getText());
        sql.append(" ").append(evalPop(ctx.bool_expr()));
        exec.stackPush(sql);
        exec.buildSql = oldBuildSql;
        return 0;
    }

    /**
     * Get INTO clause
     */
    org.apache.doris.hplsql.HplsqlParser.Into_clauseContext getIntoClause(
            org.apache.doris.hplsql.HplsqlParser.Select_stmtContext ctx) {
        if (ctx.fullselect_stmt().fullselect_stmt_item(0).subselect_stmt() != null) {
            return ctx.fullselect_stmt().fullselect_stmt_item(0).subselect_stmt().into_clause();
        }
        return null;
    }

    /**
     * Get number of elements in INTO or var=col assignment clause
     */
    int getIntoCount(org.apache.doris.hplsql.HplsqlParser.Select_stmtContext ctx) {
        org.apache.doris.hplsql.HplsqlParser.Into_clauseContext into = getIntoClause(ctx);
        if (into != null) {
            return into.ident().size() + into.table_row().size();
        }
        List<org.apache.doris.hplsql.HplsqlParser.Select_list_itemContext> sl = ctx.fullselect_stmt()
                .fullselect_stmt_item(0).subselect_stmt()
                .select_list().select_list_item();
        if (sl.get(0).T_EQUAL() != null) {
            return sl.size();
        }
        return 0;
    }

    private boolean isBulkCollect(org.apache.doris.hplsql.HplsqlParser.Select_stmtContext ctx) {
        org.apache.doris.hplsql.HplsqlParser.Into_clauseContext into = getIntoClause(ctx);
        return into != null && into.bulk_collect_clause() != null;
    }

    /**
     * Get variable name assigned in INTO or var=col clause by index
     */
    String getIntoVariable(org.apache.doris.hplsql.HplsqlParser.Select_stmtContext ctx, int idx) {
        org.apache.doris.hplsql.HplsqlParser.Into_clauseContext into = getIntoClause(ctx);
        if (into != null) {
            return into.table_row(idx) != null ? into.table_row(idx).ident().getText() : into.ident(idx).getText();
        }
        org.apache.doris.hplsql.HplsqlParser.Select_list_itemContext sl = ctx.fullselect_stmt().fullselect_stmt_item(0)
                .subselect_stmt()
                .select_list().select_list_item(idx);
        if (sl != null) {
            return sl.qident().getText();
        }
        return null;
    }

    private List<String> intoVariableNames(org.apache.doris.hplsql.HplsqlParser.Select_stmtContext ctx, int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> getIntoVariable(ctx, i))
                .collect(Collectors.toList());
    }

    private int getIntoTableIndex(org.apache.doris.hplsql.HplsqlParser.Select_stmtContext ctx, int idx) {
        org.apache.doris.hplsql.HplsqlParser.Into_clauseContext into = getIntoClause(ctx);
        org.apache.doris.hplsql.HplsqlParser.Table_rowContext row = into.table_row(idx);
        if (row == null) {
            throw new TypeException(ctx, "Missing into table index");
        }
        return Integer.parseInt(row.L_INT().getText());
    }

    /**
     * SELECT statement options - LIMIT n, WITH UR i.e
     */
    public Integer option(org.apache.doris.hplsql.HplsqlParser.Select_options_itemContext ctx) {
        if (ctx.T_LIMIT() != null) {
            exec.stackPush("LIMIT " + evalPop(ctx.expr()));
        }
        return 0;
    }

    /**
     * Evaluate the expression to NULL
     */
    void evalNull() {
        exec.stackPush(Var.Null);
    }

    /**
     * Evaluate the expression and pop value from the stack
     */
    Var evalPop(ParserRuleContext ctx) {
        exec.visit(ctx);
        if (!exec.stack.isEmpty()) {
            return exec.stackPop();
        }
        return Var.Empty;
    }

    /**
     * Get node text including spaces
     */
    String getText(ParserRuleContext ctx) {
        return ctx.start.getInputStream().getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    /**
     * Trace information
     */
    void trace(ParserRuleContext ctx, String message) {
        exec.trace(ctx, message);
    }
}
