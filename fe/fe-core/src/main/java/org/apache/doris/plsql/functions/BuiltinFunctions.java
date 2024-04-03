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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/functions/BuiltinFunctions.java
// and modified by Doris

package org.apache.doris.plsql.functions;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.PLParser.Expr_func_paramsContext;
import org.apache.doris.nereids.PLParser.Expr_spec_funcContext;
import org.apache.doris.nereids.PLParser.Expr_stmtContext;
import org.apache.doris.plsql.Console;
import org.apache.doris.plsql.Exec;
import org.apache.doris.plsql.Utils;
import org.apache.doris.plsql.Var;
import org.apache.doris.plsql.exception.QueryException;
import org.apache.doris.plsql.executor.QueryExecutor;
import org.apache.doris.plsql.executor.QueryResult;

import org.antlr.v4.runtime.ParserRuleContext;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BuiltinFunctions {
    protected final Exec exec;
    protected final Console console;
    protected boolean trace;
    protected final QueryExecutor queryExecutor;
    protected HashMap<String, org.apache.doris.plsql.functions.FuncCommand> map = new HashMap<>();
    protected HashMap<String, org.apache.doris.plsql.functions.FuncSpecCommand> specMap = new HashMap<>();
    protected HashMap<String, org.apache.doris.plsql.functions.FuncSpecCommand> specSqlMap = new HashMap<>();

    public BuiltinFunctions(Exec exec, QueryExecutor queryExecutor) {
        this.exec = exec;
        this.trace = exec.getTrace();
        this.console = exec.getConsole();
        this.queryExecutor = queryExecutor;
    }

    public void register(BuiltinFunctions f) {
    }

    public boolean exec(String name, Expr_func_paramsContext ctx) {
        if (name.contains(".")) {               // Name can be qualified and spaces are allowed between parts
            String[] parts = name.split("\\.");
            StringBuilder str = new StringBuilder();
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    str.append(".");
                }
                str.append(parts[i].trim());
            }
            name = str.toString();
        }
        if (trace && ctx != null && ctx.parent != null && ctx.parent.parent instanceof Expr_stmtContext) {
            trace(ctx, "FUNC " + name);
        }
        org.apache.doris.plsql.functions.FuncCommand func = map.get(name.toUpperCase());
        if (func != null) {
            func.run(ctx);
            return true;
        } else {
            return false;
        }
    }

    public boolean exists(String name) {
        if (name == null) {
            return false;
        }
        name = name.toUpperCase();
        return map.containsKey(name) || specMap.containsKey(name) || specSqlMap.containsKey(name);
    }

    /**
     * Execute a special function
     */
    public void specExec(Expr_spec_funcContext ctx) {
        String name = ctx.start.getText().toUpperCase();
        if (trace && ctx.parent.parent instanceof Expr_stmtContext) {
            trace(ctx, "FUNC " + name);
        }
        org.apache.doris.plsql.functions.FuncSpecCommand func = specMap.get(name);
        if (func != null) {
            func.run(ctx);
        } else if (ctx.MAX_PART_STRING() != null) {
            execMaxPartString(ctx);
        } else if (ctx.MIN_PART_STRING() != null) {
            execMinPartString(ctx);
        } else if (ctx.MAX_PART_INT() != null) {
            execMaxPartInt(ctx);
        } else if (ctx.MIN_PART_INT() != null) {
            execMinPartInt(ctx);
        } else if (ctx.MAX_PART_DATE() != null) {
            execMaxPartDate(ctx);
        } else if (ctx.MIN_PART_DATE() != null) {
            execMinPartDate(ctx);
        } else if (ctx.PART_LOC() != null) {
            execPartLoc(ctx);
        } else {
            evalNull();
        }
    }

    /**
     * Execute a special function in executable SQL statement
     */
    public void specExecSql(Expr_spec_funcContext ctx) {
        String name = ctx.start.getText().toUpperCase();
        if (trace && ctx.parent.parent instanceof Expr_stmtContext) {
            trace(ctx, "FUNC " + name);
        }
        org.apache.doris.plsql.functions.FuncSpecCommand func = specSqlMap.get(name);
        if (func != null) {
            func.run(ctx);
        } else {
            exec.stackPush(Exec.getFormattedText(ctx));
        }
    }

    /**
     * Get the current date
     */
    public void execCurrentDate(Expr_spec_funcContext ctx) {
        if (trace) {
            trace(ctx, "CURRENT_DATE");
        }
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        String s = f.format(Calendar.getInstance().getTime());
        exec.stackPush(new Var(Var.Type.DATE, Utils.toDate(s)));
    }

    /**
     * Execute MAX_PART_STRING function
     */
    public void execMaxPartString(Expr_spec_funcContext ctx) {
        if (trace) {
            trace(ctx, "MAX_PART_STRING");
        }
        execMinMaxPart(ctx, Var.Type.STRING, true /*max*/);
    }

    /**
     * Execute MIN_PART_STRING function
     */
    public void execMinPartString(Expr_spec_funcContext ctx) {
        if (trace) {
            trace(ctx, "MIN_PART_STRING");
        }
        execMinMaxPart(ctx, Var.Type.STRING, false /*max*/);
    }

    /**
     * Execute MAX_PART_INT function
     */
    public void execMaxPartInt(Expr_spec_funcContext ctx) {
        if (trace) {
            trace(ctx, "MAX_PART_INT");
        }
        execMinMaxPart(ctx, Var.Type.BIGINT, true /*max*/);
    }

    /**
     * Execute MIN_PART_INT function
     */
    public void execMinPartInt(Expr_spec_funcContext ctx) {
        if (trace) {
            trace(ctx, "MIN_PART_INT");
        }
        execMinMaxPart(ctx, Var.Type.BIGINT, false /*max*/);
    }

    /**
     * Execute MAX_PART_DATE function
     */
    public void execMaxPartDate(Expr_spec_funcContext ctx) {
        if (trace) {
            trace(ctx, "MAX_PART_DATE");
        }
        execMinMaxPart(ctx, Var.Type.DATE, true /*max*/);
    }

    /**
     * Execute MIN_PART_DATE function
     */
    public void execMinPartDate(Expr_spec_funcContext ctx) {
        if (trace) {
            trace(ctx, "MIN_PART_DATE");
        }
        execMinMaxPart(ctx, Var.Type.DATE, false /*max*/);
    }

    /**
     * Execute MIN or MAX partition function
     */
    public void execMinMaxPart(Expr_spec_funcContext ctx, Var.Type type, boolean max) {
        String tabname = evalPop(ctx.expr(0)).toString();
        StringBuilder sql = new StringBuilder("SHOW PARTITIONS " + tabname);
        String colname = null;
        int colnum = -1;
        int exprnum = ctx.expr().size();
        // Column name
        if (ctx.expr(1) != null) {
            colname = evalPop(ctx.expr(1)).toString();
        } else {
            colnum = 0;
        }
        // Partition filter
        if (exprnum >= 4) {
            sql.append(" PARTITION (");
            int i = 2;
            while (i + 1 < exprnum) {
                String fcol = evalPop(ctx.expr(i)).toString();
                String fval = evalPop(ctx.expr(i + 1)).toSqlString();
                if (i > 2) {
                    sql.append(", ");
                }
                sql.append(fcol).append("=").append(fval);
                i += 2;
            }
            sql.append(")");
        }
        if (trace) {
            trace(ctx, "Query: " + sql);
        }
        if (exec.getOffline()) {
            evalNull();
            return;
        }
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        if (query.error()) {
            evalNullClose(query);
            return;
        }
        try {
            String resultString = null;
            Long resultInt = null;
            Date resultDate = null;
            while (query.next()) {
                String[] parts = query.column(0, String.class).split("/");
                // Find partition column by name
                if (colnum == -1) {
                    for (int i = 0; i < parts.length; i++) {
                        String[] name = parts[i].split("=");
                        if (name[0].equalsIgnoreCase(colname)) {
                            colnum = i;
                            break;
                        }
                    }
                    // No partition column with the specified name exists
                    if (colnum == -1) {
                        evalNullClose(query);
                        return;
                    }
                }
                String[] pair = parts[colnum].split("=");
                if (type == Var.Type.STRING) {
                    resultString = Utils.minMaxString(resultString, pair[1], max);
                } else if (type == Var.Type.BIGINT) {
                    resultInt = Utils.minMaxInt(resultInt, pair[1], max);
                } else if (type == Var.Type.DATE) {
                    resultDate = Utils.minMaxDate(resultDate, pair[1], max);
                }
            }
            if (resultString != null) {
                evalString(resultString);
            } else if (resultInt != null) {
                evalInt(resultInt);
            } else if (resultDate != null) {
                evalDate(resultDate);
            } else {
                evalNull();
            }
        } catch (QueryException | AnalysisException ignored) {
            // ignored
        }
        query.close();
    }

    /**
     * Execute PART_LOC function
     */
    public void execPartLoc(Expr_spec_funcContext ctx) {
        String tabname = evalPop(ctx.expr(0)).toString();
        StringBuilder sql = new StringBuilder("DESCRIBE EXTENDED " + tabname);
        int exprnum = ctx.expr().size();
        boolean hostname = false;
        // Partition filter
        if (exprnum > 1) {
            sql.append(" PARTITION (");
            int i = 1;
            while (i + 1 < exprnum) {
                String col = evalPop(ctx.expr(i)).toString();
                String val = evalPop(ctx.expr(i + 1)).toSqlString();
                if (i > 2) {
                    sql.append(", ");
                }
                sql.append(col).append("=").append(val);
                i += 2;
            }
            sql.append(")");
        }
        // With host name
        if (exprnum % 2 == 0 && evalPop(ctx.expr(exprnum - 1)).intValue() == 1) {
            hostname = true;
        }
        if (trace) {
            trace(ctx, "Query: " + sql);
        }
        if (exec.getOffline()) {
            evalNull();
            return;
        }
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        if (query.error()) {
            evalNullClose(query);
            return;
        }
        String result = null;
        try {
            while (query.next()) {
                if (query.column(0, String.class).startsWith("Detailed Partition Information")) {
                    Matcher m = Pattern.compile(".*, location:(.*?),.*").matcher(query.column(1, String.class));
                    if (m.find()) {
                        result = m.group(1);
                    }
                }
            }
        } catch (QueryException | AnalysisException ignored) {
            // ignored
        }
        if (result != null) {
            // Remove the host name
            if (!hostname) {
                Matcher m = Pattern.compile(".*://.*?(/.*)").matcher(result);
                if (m.find()) {
                    result = m.group(1);
                }
            }
            evalString(result);
        } else {
            evalNull();
        }
        query.close();
    }

    public void trace(ParserRuleContext ctx, String message) {
        if (trace) {
            exec.trace(ctx, message);
        }
    }

    protected void evalNull() {
        exec.stackPush(Var.Null);
    }

    protected void evalString(String string) {
        exec.stackPush(new Var(string));
    }

    protected Var evalPop(ParserRuleContext ctx) {
        exec.visit(ctx);
        return exec.stackPop();
    }

    protected void evalInt(Long i) {
        exec.stackPush(new Var(i));
    }

    protected void evalDate(Date date) {
        exec.stackPush(new Var(Var.Type.DATE, date));
    }

    protected void evalNullClose(QueryResult query) {
        exec.stackPush(Var.Null);
        query.close();
        if (trace) {
            query.printStackTrace();
        }
    }

    protected void evalVar(Var var) {
        exec.stackPush(var);
    }

    protected void evalString(StringBuilder string) {
        evalString(string.toString());
    }

    protected void evalInt(int i) {
        evalInt(Long.valueOf(i));
    }

    protected Var evalPop(ParserRuleContext ctx, int value) {
        if (ctx != null) {
            return evalPop(ctx);
        }
        return new Var(Long.valueOf(value));
    }

    /**
     * Get the number of parameters in function call
     */
    public static int getParamCount(Expr_func_paramsContext ctx) {
        if (ctx == null) {
            return 0;
        }
        return ctx.func_param().size();
    }

    protected void eval(ParserRuleContext ctx) {
        exec.visit(ctx);
    }

    protected Integer visit(ParserRuleContext ctx) {
        return exec.visit(ctx);
    }

    protected void info(ParserRuleContext ctx, String message) {
        exec.info(ctx, message);
    }
}
