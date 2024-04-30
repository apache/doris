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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Stmt.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.PLParser.Allocate_cursor_stmtContext;
import org.apache.doris.nereids.PLParser.Assignment_stmt_select_itemContext;
import org.apache.doris.nereids.PLParser.Associate_locator_stmtContext;
import org.apache.doris.nereids.PLParser.Break_stmtContext;
import org.apache.doris.nereids.PLParser.Close_stmtContext;
import org.apache.doris.nereids.PLParser.Declare_cursor_itemContext;
import org.apache.doris.nereids.PLParser.Doris_statementContext;
import org.apache.doris.nereids.PLParser.Exec_stmtContext;
import org.apache.doris.nereids.PLParser.Exit_stmtContext;
import org.apache.doris.nereids.PLParser.Fetch_stmtContext;
import org.apache.doris.nereids.PLParser.For_cursor_stmtContext;
import org.apache.doris.nereids.PLParser.For_range_stmtContext;
import org.apache.doris.nereids.PLParser.Get_diag_stmt_exception_itemContext;
import org.apache.doris.nereids.PLParser.Get_diag_stmt_rowcount_itemContext;
import org.apache.doris.nereids.PLParser.If_bteq_stmtContext;
import org.apache.doris.nereids.PLParser.If_plsql_stmtContext;
import org.apache.doris.nereids.PLParser.If_tsql_stmtContext;
import org.apache.doris.nereids.PLParser.Include_stmtContext;
import org.apache.doris.nereids.PLParser.IntoClauseContext;
import org.apache.doris.nereids.PLParser.Leave_stmtContext;
import org.apache.doris.nereids.PLParser.Open_stmtContext;
import org.apache.doris.nereids.PLParser.Print_stmtContext;
import org.apache.doris.nereids.PLParser.QueryPrimaryDefaultContext;
import org.apache.doris.nereids.PLParser.Quit_stmtContext;
import org.apache.doris.nereids.PLParser.RegularQuerySpecificationContext;
import org.apache.doris.nereids.PLParser.Resignal_stmtContext;
import org.apache.doris.nereids.PLParser.Return_stmtContext;
import org.apache.doris.nereids.PLParser.Set_current_schema_optionContext;
import org.apache.doris.nereids.PLParser.Signal_stmtContext;
import org.apache.doris.nereids.PLParser.StatementDefaultContext;
import org.apache.doris.nereids.PLParser.TableRowContext;
import org.apache.doris.nereids.PLParser.Unconditional_loop_stmtContext;
import org.apache.doris.nereids.PLParser.Values_into_stmtContext;
import org.apache.doris.nereids.PLParser.While_stmtContext;
import org.apache.doris.nereids.trees.plans.commands.info.FuncNameInfo;
import org.apache.doris.plsql.Var.Type;
import org.apache.doris.plsql.exception.QueryException;
import org.apache.doris.plsql.exception.UndefinedIdentException;
import org.apache.doris.plsql.executor.Metadata;
import org.apache.doris.plsql.executor.PlsqlResult;
import org.apache.doris.plsql.executor.QueryExecutor;
import org.apache.doris.plsql.executor.QueryResult;
import org.apache.doris.plsql.executor.ResultListener;
import org.apache.doris.plsql.objects.Table;

import org.antlr.v4.runtime.ParserRuleContext;

import java.sql.SQLException;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * PL/SQL statements execution
 */
public class Stmt {
    Exec exec = null;
    Stack<Var> stack = null;
    Conf conf;
    Meta meta;
    Console console;

    boolean trace = false;
    ResultListener resultListener = ResultListener.NONE;
    private final QueryExecutor queryExecutor;

    Stmt(Exec e, QueryExecutor queryExecutor) {
        exec = e;
        stack = exec.getStack();
        conf = exec.getConf();
        meta = exec.getMeta();
        trace = exec.getTrace();
        console = exec.console;
        this.queryExecutor = queryExecutor;
    }

    public void setResultListener(ResultListener resultListener) {
        this.resultListener = resultListener;
    }

    /**
     * Executing Statement statement
     */
    public Integer statement(ParserRuleContext ctx) {
        trace(ctx, "SELECT");
        if (exec.getOffline()) {
            trace(ctx, "Not executed - offline mode set");
            return 0;
        }

        QueryResult query = queryExecutor.executeQuery(exec.logicalPlanBuilder.getOriginSql(ctx), ctx);
        resultListener.setProcessor(query.processor());

        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        trace(ctx, "statement completed successfully");
        exec.setSqlSuccess();
        try {
            int intoCount = getIntoCount(ctx);
            if (intoCount > 0) {
                // TODO SELECT BULK COLLECT INTO statement executed
                trace(ctx, "SELECT INTO statement executed");
                if (query.next()) {
                    for (int i = 0; i < intoCount; i++) {
                        populateVariable(ctx, query, i);
                    }
                    exec.incRowCount();
                    exec.setSqlSuccess();
                    if (query.next()) {
                        exec.setSqlCode(SqlCodes.TOO_MANY_ROWS);
                        exec.signal(Signal.Type.TOO_MANY_ROWS, "too many rows into variables");
                    }
                } else {
                    exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
                    exec.signal(Signal.Type.NOTFOUND, "no rows into variables");
                }
            } else if (ctx instanceof Doris_statementContext) { // only from visitStatement
                // Print all results for standalone Statement.
                if (query.metadata() != null && !query.isHandleQueryInFe()) {
                    resultListener.onMetadata(query.metadata());
                    int cols = query.columnCount();
                    if (trace) {
                        trace(ctx, "Standalone statement executed: " + cols + " columns in the result set");
                    }
                    while (query.next()) {
                        if (resultListener instanceof PlsqlResult) { // if running from mysql clent
                            resultListener.onMysqlRow(query.mysqlRow());
                        } else { // if running from plsql.sh
                            Object[] row = new Object[cols]; // TODO if there is a large amount of data?
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
                }
            } else { // Scalar subquery, such as visitExpr
                trace(ctx, "Scalar subquery executed, first row and first column fetched only");
                if (query.next()) {
                    exec.stackPush(new Var().setValue(query, 1));
                    exec.setSqlSuccess();
                } else {
                    evalNull();
                    exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
                }
            }
        } catch (QueryException | AnalysisException e) {
            if (query.error()) {
                exec.signal(query);
            } else {
                exec.signal(e);
            }
            query.close();
            return 1;
        }
        query.close();
        return 0;
    }

    /**
     * Get INTO clause
     */
    IntoClauseContext getIntoClause(ParserRuleContext ctx) {
        if (ctx.getChild(0) instanceof StatementDefaultContext) {
            ParserRuleContext queryTermDefaultCtx = ((StatementDefaultContext) ctx.getChild(0)).query().queryTerm();
            if (queryTermDefaultCtx.getChild(0) instanceof QueryPrimaryDefaultContext) {
                ParserRuleContext queryPrimaryDefaultContext
                        = ((QueryPrimaryDefaultContext) queryTermDefaultCtx.getChild(0));
                if (queryPrimaryDefaultContext.getChild(0) instanceof RegularQuerySpecificationContext) {
                    return ((RegularQuerySpecificationContext) queryPrimaryDefaultContext.getChild(0)).intoClause();
                }
            }
        }
        return null;
    }

    /**
     * Get number of elements in INTO or var=col assignment clause
     */
    int getIntoCount(ParserRuleContext ctx) {
        IntoClauseContext into = getIntoClause(ctx);
        if (into != null) {
            return into.identifier().size() + into.tableRow().size();
        }
        // TODO support var=col assignment clause
        return 0;
    }

    /**
     * Get variable name assigned in INTO or var=col clause by index
     */
    String getIntoVariable(ParserRuleContext ctx, int idx) {
        IntoClauseContext into = getIntoClause(ctx);
        if (into != null) {
            return into.tableRow(idx) != null ? into.tableRow(idx).identifier().getText()
                    : into.identifier(idx).getText();
        }
        // TODO support var=col assignment clause
        return null;
    }

    private int getIntoTableIndex(ParserRuleContext ctx, int idx) {
        IntoClauseContext into = getIntoClause(ctx);
        TableRowContext row = into.tableRow(idx);
        if (row == null) {
            throw new RuntimeException("Missing into table index");
        }
        return Integer.parseInt(row.INTEGER_VALUE().getText());
    }

    private void populateVariable(ParserRuleContext ctx, QueryResult query, int columnIndex) throws AnalysisException {
        String intoName = getIntoVariable(ctx, columnIndex);
        Var var = exec.findVariable(intoName);
        if (var != null) {
            if (var.type == Var.Type.PL_OBJECT && var.value instanceof Table) {
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
     * ALLOCATE CURSOR statement
     */
    public Integer allocateCursor(Allocate_cursor_stmtContext ctx) {
        trace(ctx, "ALLOCATE CURSOR");
        String name = ctx.ident_pl(0).getText();
        Var cur = null;
        if (ctx.PROCEDURE() != null) {
            cur = exec.consumeReturnCursor(ctx.ident_pl(1).getText());
        } else if (ctx.RESULT() != null) {
            cur = exec.findVariable(ctx.ident_pl(1).getText());
            if (cur != null && cur.type != Type.RS_LOCATOR) {
                cur = null;
            }
        }
        if (cur == null) {
            trace(ctx, "Cursor for procedure not found: " + name);
            exec.signal(Signal.Type.SQLEXCEPTION);
            return -1;
        }
        exec.addVariable(new Var(name, Type.CURSOR, cur.value));
        return 0;
    }

    /**
     * ASSOCIATE LOCATOR statement
     */
    public Integer associateLocator(Associate_locator_stmtContext ctx) {
        trace(ctx, "ASSOCIATE LOCATOR");
        int cnt = ctx.ident_pl().size();
        if (cnt < 2) {
            return -1;
        }
        String procedure = ctx.ident_pl(cnt - 1).getText();
        for (int i = 0; i < cnt - 1; i++) {
            Var cur = exec.consumeReturnCursor(procedure);
            if (cur != null) {
                String name = ctx.ident_pl(i).getText();
                Var loc = exec.findVariable(name);
                if (loc == null) {
                    loc = new Var(name, Type.RS_LOCATOR, cur.value);
                    exec.addVariable(loc);
                } else {
                    loc.setValue(cur.value);
                }
            }
        }
        return 0;
    }

    /**
     * DECLARE cursor statement
     */
    public Integer declareCursor(Declare_cursor_itemContext ctx) {
        String name = ctx.ident_pl().getText();
        if (trace) {
            trace(ctx, "DECLARE CURSOR " + name);
        }
        Cursor cursor = new Cursor(null);
        if (ctx.expr() != null) {
            cursor.setExprCtx(ctx.expr());
        } else if (ctx.query() != null) {
            cursor.setSelectCtx(ctx.query());
        }
        if (ctx.cursor_with_return() != null) {
            cursor.setWithReturn(true);
        }
        Var var = new Var(name, Type.CURSOR, cursor);
        exec.addVariable(var);
        return 0;
    }

    /**
     * OPEN cursor statement
     */
    public Integer open(Open_stmtContext ctx) {
        trace(ctx, "OPEN");
        Cursor cursor = null;
        Var var = null;
        String cursorName = ctx.ident_pl().getText();
        String sql = null;
        if (ctx.FOR() != null) {                             // SELECT statement or dynamic SQL
            sql = ctx.expr() != null ? exec.logicalPlanBuilder.getOriginSql(ctx.expr())
                    : exec.logicalPlanBuilder.getOriginSql(ctx.query());
            cursor = new Cursor(sql);
            var = exec.findCursor(cursorName);                      // Can be a ref cursor variable
            if (var == null) {
                var = new Var(cursorName, Type.CURSOR, cursor);
                exec.addVariable(var);
            } else {
                var.setValue(cursor);
            }
        } else {                                                 // Declared cursor
            var = exec.findVariable(cursorName);
            if (var != null && var.type == Type.CURSOR) {
                cursor = (Cursor) var.value;
                if (cursor.getSqlExpr() != null) {
                    cursor.setSql(exec.logicalPlanBuilder.getOriginSql(cursor.getSqlExpr()));
                } else if (cursor.getSqlSelect() != null) {
                    cursor.setSql(exec.logicalPlanBuilder.getOriginSql(cursor.getSqlSelect()));
                }
            }
        }
        if (cursor != null) {
            if (trace) {
                trace(ctx, cursorName + ": " + sql);
            }
            cursor.open(queryExecutor, ctx);
            QueryResult queryResult = cursor.getQueryResult();
            if (queryResult.error()) {
                exec.signal(queryResult);
                return 1;
            } else if (!exec.getOffline()) {
                exec.setSqlCode(SqlCodes.SUCCESS);
            }
            if (cursor.isWithReturn()) {
                exec.addReturnCursor(var);
            }
        } else {
            trace(ctx, "Cursor not found: " + cursorName);
            exec.setSqlCode(SqlCodes.ERROR);
            exec.signal(Signal.Type.SQLEXCEPTION);
            return 1;
        }
        return 0;
    }

    /**
     * FETCH cursor statement
     */
    public Integer fetch(Fetch_stmtContext ctx) {
        trace(ctx, "FETCH");
        String name = ctx.ident_pl(0).getText();
        Var varCursor = exec.findCursor(name);
        if (varCursor == null) {
            trace(ctx, "Cursor not found: " + name);
            exec.setSqlCode(SqlCodes.ERROR);
            exec.signal(Signal.Type.SQLEXCEPTION);
            return 1;
        } else if (varCursor.value == null) {
            trace(ctx, "Cursor not open: " + name);
            exec.setSqlCode(SqlCodes.ERROR);
            exec.signal(Signal.Type.SQLEXCEPTION);
            return 1;
        } else if (exec.getOffline()) {
            exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
            exec.signal(Signal.Type.NOTFOUND, "fetch not found data");
            return 0;
        }
        // Assign values from the row to local variables
        try {
            Cursor cursor = (Cursor) varCursor.value;
            int cols = ctx.ident_pl().size() - 1;
            QueryResult queryResult = cursor.getQueryResult();

            if (ctx.bulkCollectClause() != null) {
                long limit = ctx.fetch_limit() != null ? evalPop(ctx.fetch_limit().expr()).longValue() : -1;
                long rowIndex = 1;
                List<Table> tables = exec.intoTables(ctx, intoVariableNames(ctx, cols));
                tables.forEach(Table::removeAll);
                while (queryResult.next()) {
                    cursor.setFetch(true);
                    for (int i = 0; i < cols; i++) {
                        Table table = tables.get(i);
                        table.populate(queryResult, rowIndex, i);
                    }
                    rowIndex++;
                    if (limit != -1 && rowIndex - 1 >= limit) {
                        break;
                    }
                }
            } else {
                if (queryResult.next()) {
                    cursor.setFetch(true);
                    for (int i = 0; i < cols; i++) {
                        Var var = exec.findVariable(ctx.ident_pl(i + 1).getText());
                        if (var != null) { // Variables must be defined in advance by DECLARE etc.
                            if (var.type != Var.Type.ROW) {
                                var.setValue(queryResult, i); // Set value of each column into variable
                            } else {
                                var.setRowValues(queryResult);
                            }
                            if (trace) {
                                trace(ctx, var, queryResult.metadata(), i);
                            }
                        } else if (trace) {
                            trace(ctx, "Variable not found: " + ctx.ident_pl(i + 1).getText());
                        }
                    }
                    exec.incRowCount();
                    exec.setSqlSuccess();
                } else {
                    cursor.setFetch(false);
                    exec.setSqlCode(SqlCodes.NO_DATA_FOUND); // Check when exiting cursor
                }
            }
        } catch (QueryException | AnalysisException e) {
            exec.setSqlCode(e);
            exec.signal(Signal.Type.SQLEXCEPTION, e.getMessage(), e);
        }
        return 0;
    }

    private List<String> intoVariableNames(Fetch_stmtContext ctx, int count) {
        return IntStream.range(0, count).mapToObj(i -> ctx.ident_pl(i + 1).getText()).collect(Collectors.toList());
    }


    /**
     * CLOSE cursor statement
     */
    public Integer close(Close_stmtContext ctx) {
        trace(ctx, "CLOSE");
        String name = ctx.IDENTIFIER().toString();
        Var var = exec.findVariable(name);
        if (var != null && var.type == Type.CURSOR) {
            ((Cursor) var.value).close();
            exec.setSqlCode(SqlCodes.SUCCESS);
        } else if (trace) {
            trace(ctx, "Cursor not found: " + name);
        }
        return 0;
    }

    /**
     * INCLUDE statement
     */
    public Integer include(Include_stmtContext ctx) {
        String file;
        if (ctx.file_name() != null) {
            file = ctx.file_name().getText();
        } else {
            file = evalPop(ctx.expr()).toString();
        }
        trace(ctx, "INCLUDE " + file);
        exec.includeFile(file, true);
        return 0;
    }

    /**
     * IF statement (PL/SQL syntax)
     */
    public Integer ifPlsql(If_plsql_stmtContext ctx) {
        boolean trueExecuted = false;
        trace(ctx, "IF");
        if (evalPop(ctx.bool_expr()).isTrue()) {
            trace(ctx, "IF TRUE executed");
            visit(ctx.block());
            trueExecuted = true;
        } else if (ctx.elseif_block() != null) {
            int cnt = ctx.elseif_block().size();
            for (int i = 0; i < cnt; i++) {
                if (evalPop(ctx.elseif_block(i).bool_expr()).isTrue()) {
                    trace(ctx, "ELSE IF executed");
                    visit(ctx.elseif_block(i).block());
                    trueExecuted = true;
                    break;
                }
            }
        }
        if (!trueExecuted && ctx.else_block() != null) {
            trace(ctx, "ELSE executed");
            visit(ctx.else_block());
        }
        return 0;
    }

    /**
     * IF statement (Transact-SQL syntax)
     */
    public Integer ifTsql(If_tsql_stmtContext ctx) {
        trace(ctx, "IF");
        visit(ctx.bool_expr());
        if (exec.stackPop().isTrue()) {
            trace(ctx, "IF TRUE executed");
            visit(ctx.single_block_stmt(0));
        } else if (ctx.ELSE() != null) {
            trace(ctx, "ELSE executed");
            visit(ctx.single_block_stmt(1));
        }
        return 0;
    }

    /**
     * IF statement (BTEQ syntax)
     */
    public Integer ifBteq(If_bteq_stmtContext ctx) {
        trace(ctx, "IF");
        visit(ctx.bool_expr());
        if (exec.stackPop().isTrue()) {
            trace(ctx, "IF TRUE executed");
            visit(ctx.single_block_stmt());
        }
        return 0;
    }

    /**
     * Assignment from SELECT statement
     */
    public Integer assignFromSelect(Assignment_stmt_select_itemContext ctx) {
        String sql = evalPop(ctx.query()).toString();
        if (trace) {
            trace(ctx, sql);
        }
        QueryResult query = queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        try {
            int cnt = ctx.ident_pl().size();
            if (query.next()) {
                for (int i = 0; i < cnt; i++) {
                    Var var = exec.findVariable(ctx.ident_pl(i).getText());
                    if (var != null) {
                        var.setValue(query, i);
                        if (trace) {
                            trace(ctx, "COLUMN: " + query.metadata().columnName(i) + ", " + query.metadata()
                                    .columnTypeName(i));
                            trace(ctx, "SET " + var.getName() + " = " + var);
                        }
                    } else if (trace) {
                        trace(ctx, "Variable not found: " + ctx.ident_pl(i).getText());
                    }
                }
                exec.incRowCount();
                exec.setSqlSuccess();
            } else {
                exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
                exec.signal(Signal.Type.NOTFOUND, "assign from select not found data");
            }
        } catch (QueryException | AnalysisException e) {
            exec.signal(query);
            return 1;
        } finally {
            query.close();
        }
        return 0;
    }

    /**
     * GET DIAGNOSTICS EXCEPTION statement
     */
    public Integer getDiagnosticsException(Get_diag_stmt_exception_itemContext ctx) {
        trace(ctx, "GET DIAGNOSTICS EXCEPTION");
        Signal signal = exec.signalPeek();
        if (signal == null || (signal != null && signal.type != Signal.Type.SQLEXCEPTION)) {
            signal = exec.currentSignal;
        }
        if (signal != null) {
            exec.setVariable(ctx.qident().getText(), signal.getValue());
        }
        return 0;
    }

    /**
     * GET DIAGNOSTICS ROW_COUNT statement
     */
    public Integer getDiagnosticsRowCount(Get_diag_stmt_rowcount_itemContext ctx) {
        trace(ctx, "GET DIAGNOSTICS ROW_COUNT");
        exec.setVariable(ctx.qident().getText(), exec.getRowCount());
        return 0;
    }

    public Integer use(ParserRuleContext ctx, String sql) {
        if (trace) {
            trace(ctx, "SQL statement: " + sql);
        }
        QueryResult query = queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlCode(SqlCodes.SUCCESS);
        query.close();
        return 0;
    }

    /**
     * VALUES statement
     */
    public Integer values(Values_into_stmtContext ctx) {
        trace(ctx, "VALUES statement");
        int cnt = ctx.ident_pl().size();        // Number of variables and assignment expressions
        int ecnt = ctx.expr().size();
        for (int i = 0; i < cnt; i++) {
            String name = ctx.ident_pl(i).getText();
            if (i < ecnt) {
                visit(ctx.expr(i));
                Var var = exec.setVariable(name);
                if (trace) {
                    trace(ctx, "SET " + name + " = " + var.toString());
                }
            }
        }
        return 0;
    }

    /**
     * WHILE statement
     */
    public Integer while_(While_stmtContext ctx) {
        trace(ctx, "WHILE - ENTERED");
        String label = exec.labelPop();
        while (true) {
            if (evalPop(ctx.bool_expr()).isTrue()) {
                exec.enterScope(Scope.Type.LOOP);
                visit(ctx.block());
                exec.leaveScope();
                if (canContinue(label)) {
                    continue;
                }
            }
            break;
        }
        trace(ctx, "WHILE - LEFT");
        return 0;
    }

    /**
     * FOR cursor statement
     */
    public Integer forCursor(For_cursor_stmtContext ctx) {
        trace(ctx, "FOR CURSOR - ENTERED");
        exec.enterScope(Scope.Type.LOOP);
        String cursor = ctx.IDENTIFIER().getText();
        String sql = exec.logicalPlanBuilder.getOriginSql(ctx.query());
        trace(ctx, sql);
        QueryResult query = exec.queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        trace(ctx, "SELECT completed successfully");
        exec.setSqlSuccess();
        try {
            int cols = query.columnCount();
            Row row = new Row();
            for (int i = 0; i < cols; i++) {
                row.addColumnDefinition(query.metadata().columnName(i), query.metadata().columnTypeName(i));
            }
            Var var = new Var(cursor, row);
            exec.addVariable(var);
            while (query.next()) {
                var.setRowValues(query);
                if (trace) {
                    trace(ctx, var, query.metadata(), 0);
                }
                visit(ctx.block());
                exec.incRowCount();
            }
        } catch (QueryException | AnalysisException e) {
            exec.signal(e);
            query.close();
            return 1;
        }
        exec.setSqlSuccess();
        query.close();
        exec.leaveScope();
        trace(ctx, "FOR CURSOR - LEFT");
        return 0;
    }

    /**
     * FOR (integer range) statement
     */
    public Integer forRange(For_range_stmtContext ctx) {
        trace(ctx, "FOR RANGE - ENTERED");
        int start = evalPop(ctx.expr(0)).intValue();
        int end = evalPop(ctx.expr(1)).intValue();
        int step = evalPop(ctx.expr(2), 1L).intValue();
        exec.enterScope(Scope.Type.LOOP);
        Var index = setIndex(start, end, ctx);
        exec.addVariable(index);
        for (int i = start; i <= end; i += step) {
            visit(ctx.block());
            updateIndex(step, index, ctx);
        }
        exec.leaveScope();
        trace(ctx, "FOR RANGE - LEFT");
        return 0;
    }

    public Integer unconditionalLoop(Unconditional_loop_stmtContext ctx) {
        trace(ctx, "UNCONDITIONAL LOOP - ENTERED");
        String label = exec.labelPop();
        do {
            exec.enterScope(Scope.Type.LOOP);
            visit(ctx.block());
            exec.leaveScope();
        } while (canContinue(label));
        trace(ctx, "UNCONDITIONAL LOOP - LEFT");
        return 0;
    }

    /**
     * To set the Value index for FOR Statement
     */
    private Var setIndex(int start, int end, For_range_stmtContext ctx) {

        if (ctx.REVERSE() == null) {
            return new Var(ctx.IDENTIFIER().getText(), Long.valueOf(start));
        } else {
            return new Var(ctx.IDENTIFIER().getText(), Long.valueOf(end));
        }
    }

    /**
     * To update the value of index for FOR Statement
     */
    private void updateIndex(int step, Var index, For_range_stmtContext ctx) {

        if (ctx.REVERSE() == null) {
            index.increment(step);
        } else {
            index.decrement(step);
        }
    }

    /**
     * EXEC, EXECUTE and EXECUTE IMMEDIATE statement to execute dynamic SQL or stored procedure
     */
    public Integer exec(Exec_stmtContext ctx) {
        if (execProc(ctx)) {
            return 0;
        }
        trace(ctx, "EXECUTE");
        Var vsql = evalPop(ctx.expr());
        String sql = vsql.toString();
        if (trace) {
            trace(ctx, "SQL statement: " + sql);
        }
        QueryResult query = queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        try {
            if (ctx.INTO() != null) {
                int cols = ctx.IDENTIFIER().size();
                if (query.next()) {
                    for (int i = 0; i < cols; i++) {
                        Var var = exec.findVariable(ctx.IDENTIFIER(i).getText());
                        if (var != null) {
                            if (var.type != Type.ROW) {
                                var.setValue(query, i);
                            } else {
                                var.setRowValues(query);
                            }
                            if (trace) {
                                trace(ctx, var, query.metadata(), i);
                            }
                        } else if (trace) {
                            trace(ctx, "Variable not found: " + ctx.IDENTIFIER(i).getText());
                        }
                    }
                    exec.setSqlCode(SqlCodes.SUCCESS);
                }
            } else { // Print the results
                int cols = query.columnCount();
                while (query.next()) {
                    for (int i = 0; i < cols; i++) {
                        if (i > 1) {
                            console.print("\t");
                        }
                        console.print(query.column(i, String.class));
                    }
                    console.printLine("");
                }
            }
        } catch (QueryException | AnalysisException e) {
            exec.signal(query);
            query.close();
            return 1;
        }
        query.close();
        return 0;
    }

    /**
     * EXEC to execute a stored procedure
     */
    public Boolean execProc(Exec_stmtContext ctx) {
        String name = evalPop(ctx.expr()).toString().toUpperCase();
        return exec.functions.exec(new FuncNameInfo(name), ctx.expr_func_params());
    }

    /**
     * EXIT statement (leave the specified loop with a condition)
     */
    public Integer exit(Exit_stmtContext ctx) {
        trace(ctx, "EXIT");
        String label = "";
        if (ctx.IDENTIFIER() != null) {
            label = ctx.IDENTIFIER().toString();
        }
        if (ctx.WHEN() != null) {
            if (evalPop(ctx.bool_expr()).isTrue()) {
                leaveLoop(label);
            }
        } else {
            leaveLoop(label);
        }
        return 0;
    }

    /**
     * BREAK statement (leave the innermost loop unconditionally)
     */
    public Integer break_(Break_stmtContext ctx) {
        trace(ctx, "BREAK");
        leaveLoop("");
        return 0;
    }

    /**
     * LEAVE statement (leave the specified loop unconditionally)
     */
    public Integer leave(Leave_stmtContext ctx) {
        trace(ctx, "LEAVE");
        String label = "";
        if (ctx.IDENTIFIER() != null) {
            label = ctx.IDENTIFIER().toString();
        }
        leaveLoop(label);
        return 0;
    }

    /**
     * Leave the specified or innermost loop unconditionally
     */
    public void leaveLoop(String value) {
        exec.signal(Signal.Type.LEAVE_LOOP, value);
    }

    /**
     * PRINT Statement
     */
    public Integer print(Print_stmtContext ctx) {
        trace(ctx, "PRINT");
        if (ctx.expr() != null) {
            console.printLine(evalPop(ctx.expr()).toString());
        }
        return 0;
    }

    /**
     * QUIT Statement
     */
    public Integer quit(Quit_stmtContext ctx) {
        trace(ctx, "QUIT");
        String rc = null;
        if (ctx.expr() != null) {
            rc = evalPop(ctx.expr()).toString();
        }
        exec.signal(Signal.Type.LEAVE_PROGRAM, rc);
        return 0;
    }

    /**
     * SET current schema
     */
    public Integer setCurrentSchema(Set_current_schema_optionContext ctx) {
        trace(ctx, "SET CURRENT SCHEMA");
        return use(ctx, "USE " + meta.normalizeIdentifierPart(evalPop(ctx.expr()).toString()));
    }

    /**
     * SIGNAL statement
     */
    public Integer signal(Signal_stmtContext ctx) {
        trace(ctx, "SIGNAL");
        Signal signal = new Signal(Signal.Type.USERDEFINED, ctx.ident_pl().getText());
        exec.signal(signal);
        return 0;
    }

    /**
     * RESIGNAL statement
     */
    public Integer resignal(Resignal_stmtContext ctx) {
        trace(ctx, "RESIGNAL");
        if (ctx.SQLSTATE() != null) {
            String sqlstate = evalPop(ctx.expr(0)).toString();
            String text = "";
            if (ctx.MESSAGE_TEXT() != null) {
                text = evalPop(ctx.expr(1)).toString();
            }
            SQLException exception = new SQLException(text, sqlstate, -1);
            Signal signal = new Signal(Signal.Type.SQLEXCEPTION, text, exception);
            exec.setSqlCode(exception);
            exec.resignal(signal);
        } else {
            exec.resignal();
        }
        return 0;
    }

    /**
     * RETURN statement
     */
    public Integer return_(Return_stmtContext ctx) {
        trace(ctx, "RETURN");
        if (ctx.expr() != null) {
            eval(ctx.expr());
        }
        exec.signal(Signal.Type.LEAVE_ROUTINE);
        return 0;
    }

    /**
     * Check if an exception is raised or EXIT executed, and we should leave the block
     */
    boolean canContinue(String label) {
        Signal signal = exec.signalPeek();
        if (signal != null && signal.type == Signal.Type.SQLEXCEPTION) {
            return false;
        }
        signal = exec.signalPeek();
        if (signal != null && signal.type == Signal.Type.LEAVE_LOOP) {
            if (signal.value == null || signal.value.isEmpty() || (label != null && label.equalsIgnoreCase(
                    signal.value))) {
                exec.signalPop();
            } // why?
            return false;
        }
        return true;
    }

    /**
     * Evaluate the expression and push the value to the stack
     */
    void eval(ParserRuleContext ctx) {
        exec.visit(ctx);
    }

    /**
     * Evaluate the expression to specified String value
     */
    void evalString(String string) {
        exec.stackPush(new Var(string));
    }

    void evalString(StringBuilder string) {
        evalString(string.toString());
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
        visit(ctx);
        if (!exec.stack.isEmpty()) {
            return exec.stackPop();
        }
        return Var.Empty;
    }

    Var evalPop(ParserRuleContext ctx, long def) {
        if (ctx != null) {
            exec.visit(ctx);
            return exec.stackPop();
        }
        return new Var(def);
    }

    /**
     * Execute rules
     */
    Integer visit(ParserRuleContext ctx) {
        return exec.visit(ctx);
    }

    /**
     * Execute children rules
     */
    Integer visitChildren(ParserRuleContext ctx) {
        return exec.visitChildren(ctx);
    }

    /**
     * Trace information
     */
    void trace(ParserRuleContext ctx, String message) {
        exec.trace(ctx, message);
    }

    void trace(ParserRuleContext ctx, Var var, Metadata metadata, int idx) {
        exec.trace(ctx, var, metadata, idx);
    }
}
