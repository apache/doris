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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/functions/FunctionMisc.java
// and modified by Doris

package org.apache.doris.plsql.functions;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.PLParser.Expr_func_paramsContext;
import org.apache.doris.nereids.PLParser.Expr_spec_funcContext;
import org.apache.doris.plsql.Conn;
import org.apache.doris.plsql.Exec;
import org.apache.doris.plsql.Var;
import org.apache.doris.plsql.exception.QueryException;
import org.apache.doris.plsql.executor.QueryExecutor;
import org.apache.doris.plsql.executor.QueryResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FunctionMisc extends BuiltinFunctions {
    public FunctionMisc(Exec e, QueryExecutor queryExecutor) {
        super(e, queryExecutor);
    }

    /**
     * Register functions
     */
    @Override
    public void register(BuiltinFunctions f) {
        f.map.put("COALESCE", this::nvl);
        f.map.put("DECODE", this::decode);
        f.map.put("NVL", this::nvl);
        f.map.put("NVL2", this::nvl2);
        f.map.put("PART_COUNT_BY", this::partCountBy);
        f.map.put("MOD", this::modulo);

        f.specMap.put("ACTIVITY_COUNT", this::activityCount);
        f.specMap.put("CAST", this::cast);
        f.specMap.put("CURRENT", this::current);
        f.specMap.put("CURRENT_USER", this::currentUser);
        f.specMap.put("PART_COUNT", this::partCount);
        f.specMap.put("USER", this::currentUser);

        f.specSqlMap.put("CURRENT", this::currentSql);
    }

    /**
     * ACTIVITY_COUNT function (built-in variable)
     */
    void activityCount(Expr_spec_funcContext ctx) {
        evalInt(Long.valueOf(exec.getRowCount()));
    }

    /**
     * CAST function
     */
    void cast(Expr_spec_funcContext ctx) {
        if (ctx.expr().size() != 1) {
            evalNull();
            return;
        }
        String type = ctx.dtype().getText();
        String len = null;
        String scale = null;
        if (ctx.dtype_len() != null) {
            len = ctx.dtype_len().INTEGER_VALUE(0).getText();
            if (ctx.dtype_len().INTEGER_VALUE(1) != null) {
                scale = ctx.dtype_len().INTEGER_VALUE(1).getText();
            }
        }
        Var var = new Var(null, type, len, scale, null);
        var.cast(evalPop(ctx.expr(0)));
        evalVar(var);
    }

    /**
     * CURRENT <VALUE> function
     */
    void current(Expr_spec_funcContext ctx) {
        if (ctx.DATE() != null) {
            evalVar(FunctionDatetime.currentDate());
        } else if (ctx.TIMESTAMP() != null) {
            int precision = evalPop(ctx.expr(0), 3).intValue();
            evalVar(FunctionDatetime.currentTimestamp(precision));
        } else if (ctx.USER() != null) {
            evalVar(FunctionMisc.currentUser());
        } else {
            evalNull();
        }
    }

    /**
     * CURRENT <VALUE> function in executable SQL statement
     */
    void currentSql(Expr_spec_funcContext ctx) {
        if (ctx.DATE() != null) {
            if (exec.getConnectionType() == Conn.Type.HIVE) {
                evalString("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))");
            } else {
                evalString("CURRENT_DATE");
            }
        } else if (ctx.TIMESTAMP() != null) {
            if (exec.getConnectionType() == Conn.Type.HIVE) {
                evalString("FROM_UNIXTIME(UNIX_TIMESTAMP())");
            } else {
                evalString("CURRENT_TIMESTAMP");
            }
        } else {
            evalString(exec.getFormattedText(ctx));
        }
    }

    /**
     * CURRENT_USER function
     */
    void currentUser(Expr_spec_funcContext ctx) {
        evalVar(currentUser());
    }

    public static Var currentUser() {
        return new Var(System.getProperty("user.name"));
    }

    /**
     * DECODE function
     */
    void decode(Expr_func_paramsContext ctx) {
        int cnt = ctx.func_param().size();
        if (cnt < 3) {
            evalNull();
            return;
        }
        Var value = evalPop(ctx.func_param(0).expr());
        int i = 1;
        while (i + 1 < cnt) {
            Var when = evalPop(ctx.func_param(i).expr());
            if ((value.isNull() && when.isNull()) || value.equals(when)) {
                eval(ctx.func_param(i + 1).expr());
                return;
            }
            i += 2;
        }
        if (i < cnt) {           // ELSE expression
            eval(ctx.func_param(i).expr());
        } else {
            evalNull();
        }
    }

    /**
     * NVL function - Return first non-NULL expression
     */
    void nvl(Expr_func_paramsContext ctx) {
        for (int i = 0; i < ctx.func_param().size(); i++) {
            Var v = evalPop(ctx.func_param(i).expr());
            if (v.type != Var.Type.NULL) {
                exec.stackPush(v);
                return;
            }
        }
        evalNull();
    }

    /**
     * NVL2 function - If expr1 is not NULL return expr2, otherwise expr3
     */
    void nvl2(Expr_func_paramsContext ctx) {
        if (ctx.func_param().size() == 3) {
            if (!evalPop(ctx.func_param(0).expr()).isNull()) {
                eval(ctx.func_param(1).expr());
            } else {
                eval(ctx.func_param(2).expr());
            }
        } else {
            evalNull();
        }
    }

    /**
     * PART_COUNT function
     */
    public void partCount(Expr_spec_funcContext ctx) {
        String tabname = evalPop(ctx.expr(0)).toString();
        StringBuilder sql = new StringBuilder();
        sql.append("SHOW PARTITIONS ");
        sql.append(tabname);
        int cnt = ctx.expr().size();
        if (cnt > 1) {
            sql.append(" PARTITION (");
            int i = 1;
            while (i + 1 < cnt) {
                String col = evalPop(ctx.expr(i)).toString();
                String val = evalPop(ctx.expr(i + 1)).toSqlString();
                if (i > 2) {
                    sql.append(", ");
                }
                sql.append(col);
                sql.append("=");
                sql.append(val);
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
        int result = 0;
        try {
            while (query.next()) {
                result++;
            }
        } catch (Exception e) {
            evalNullClose(query);
            return;
        }
        evalInt(result);
        query.close();
    }

    public void modulo(Expr_func_paramsContext ctx) {
        if (ctx.func_param().size() == 2) {
            int a = evalPop(ctx.func_param(0).expr()).intValue();
            int b = evalPop(ctx.func_param(1).expr()).intValue();
            evalInt(a % b);
        } else {
            evalNull();
        }
    }

    /**
     * PART_COUNT_BY function
     */
    public void partCountBy(Expr_func_paramsContext ctx) {
        int cnt = ctx.func_param().size();
        if (cnt < 1 || exec.getOffline()) {
            return;
        }
        String tabname = evalPop(ctx.func_param(0).expr()).toString();
        ArrayList<String> keys = null;
        if (cnt > 1) {
            keys = new ArrayList<>();
            for (int i = 1; i < cnt; i++) {
                keys.add(evalPop(ctx.func_param(i).expr()).toString().toUpperCase());
            }
        }
        String sql = "SHOW PARTITIONS " + tabname;
        QueryResult query = queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            query.close();
            return;
        }
        Map<String, Integer> group = new HashMap<>();
        try {
            while (query.next()) {
                String part = query.column(0, String.class);
                String[] parts = part.split("/");
                String key = parts[0];
                if (cnt > 1) {
                    StringBuilder k = new StringBuilder();
                    for (int i = 0; i < parts.length; i++) {
                        if (keys.contains(parts[i].split("=")[0].toUpperCase())) {
                            if (k.length() > 0) {
                                k.append("/");
                            }
                            k.append(parts[i]);
                        }
                    }
                    key = k.toString();
                }
                Integer count = group.get(key);
                if (count == null) {
                    count = Integer.valueOf(0);
                }
                group.put(key, count + 1);
            }
        } catch (QueryException | AnalysisException e) {
            query.close();
            return;
        }
        if (cnt == 1) {
            evalInt(group.size());
        } else {
            for (Map.Entry<String, Integer> i : group.entrySet()) {
                console.printLine(i.getKey() + '\t' + i.getValue());
            }
        }
        query.close();
    }
}
