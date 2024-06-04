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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/functions/FunctionDatetime.java
// and modified by Doris

package org.apache.doris.plsql.functions;

import org.apache.doris.nereids.PLParser.Expr_func_paramsContext;
import org.apache.doris.nereids.PLParser.Expr_spec_funcContext;
import org.apache.doris.plsql.Exec;
import org.apache.doris.plsql.Var;
import org.apache.doris.plsql.executor.QueryExecutor;

import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class FunctionDatetime extends BuiltinFunctions {
    public FunctionDatetime(Exec e, QueryExecutor queryExecutor) {
        super(e, queryExecutor);
    }

    /**
     * Register functions
     */
    @Override
    public void register(BuiltinFunctions f) {
        f.map.put("DATE", this::date);
        f.map.put("FROM_UNIXTIME", this::fromUnixtime);
        f.map.put("NOW", ctx -> now(ctx));
        f.map.put("TIMESTAMP_ISO", this::timestampIso);
        f.map.put("TO_TIMESTAMP", this::toTimestamp);
        f.map.put("UNIX_TIMESTAMP", this::unixTimestamp);
        f.map.put("CURRENT_TIME_MILLIS", this::currentTimeMillis);

        f.specMap.put("CURRENT_DATE", this::currentDate);
        f.specMap.put("CURRENT_TIMESTAMP", this::currentTimestamp);
        f.specMap.put("SYSDATE", this::currentTimestamp);

        f.specSqlMap.put("CURRENT_DATE",
                (org.apache.doris.plsql.functions.FuncSpecCommand) this::currentDateSql);
        f.specSqlMap.put("CURRENT_TIMESTAMP",
                (org.apache.doris.plsql.functions.FuncSpecCommand) this::currentTimestampSql);
    }

    /**
     * CURRENT_DATE
     */
    public void currentDate(Expr_spec_funcContext ctx) {
        evalVar(currentDate());
    }

    public static Var currentDate() {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        String s = f.format(Calendar.getInstance().getTime());
        return new Var(org.apache.doris.plsql.Var.Type.DATE,
                org.apache.doris.plsql.Utils.toDate(s));
    }

    /**
     * CURRENT_DATE in executable SQL statement
     */
    public void currentDateSql(Expr_spec_funcContext ctx) {
        if (exec.getConnectionType() == org.apache.doris.plsql.Conn.Type.HIVE) {
            evalString("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))");
        } else {
            evalString(exec.getFormattedText(ctx));
        }
    }

    /**
     * CURRENT_TIMESTAMP
     */
    public void currentTimestamp(Expr_spec_funcContext ctx) {
        int precision = evalPop(ctx.expr(0), 3).intValue();
        evalVar(currentTimestamp(precision));
    }

    public static Var currentTimestamp(int precision) {
        String format = "yyyy-MM-dd HH:mm:ss";
        if (precision > 0 && precision <= 3) {
            format += "." + StringUtils.repeat("S", precision);
        }
        SimpleDateFormat f = new SimpleDateFormat(format);
        String s = f.format(Calendar.getInstance(TimeZone.getDefault()).getTime());
        return new Var(org.apache.doris.plsql.Utils.toTimestamp(s), precision);
    }

    /**
     * CURRENT_TIMESTAMP in executable SQL statement
     */
    public void currentTimestampSql(Expr_spec_funcContext ctx) {
        if (exec.getConnectionType() == org.apache.doris.plsql.Conn.Type.HIVE) {
            evalString("FROM_UNIXTIME(UNIX_TIMESTAMP())");
        } else {
            evalString(org.apache.doris.plsql.Exec.getFormattedText(ctx));
        }
    }

    /**
     * DATE function
     */
    void date(Expr_func_paramsContext ctx) {
        if (ctx.func_param().size() != 1) {
            evalNull();
            return;
        }
        Var var = new Var(org.apache.doris.plsql.Var.Type.DATE);
        var.cast(evalPop(ctx.func_param(0).expr()));
        evalVar(var);
    }

    /**
     * NOW() function (current date and time)
     */
    void now(Expr_func_paramsContext ctx) {
        if (ctx != null) {
            evalNull();
            return;
        }
        evalVar(currentTimestamp(3));
    }

    /**
     * TIMESTAMP_ISO function
     */
    void timestampIso(Expr_func_paramsContext ctx) {
        if (ctx.func_param().size() != 1) {
            evalNull();
            return;
        }
        Var var = new Var(org.apache.doris.plsql.Var.Type.TIMESTAMP);
        var.cast(evalPop(ctx.func_param(0).expr()));
        evalVar(var);
    }

    /**
     * TO_TIMESTAMP function
     */
    void toTimestamp(Expr_func_paramsContext ctx) {
        if (ctx.func_param().size() != 2) {
            evalNull();
            return;
        }
        String value = evalPop(ctx.func_param(0).expr()).toString();
        String sqlFormat = evalPop(ctx.func_param(1).expr()).toString();
        String format = org.apache.doris.plsql.Utils.convertSqlDatetimeFormat(sqlFormat);
        try {
            long timeInMs = new SimpleDateFormat(format).parse(value).getTime();
            evalVar(new Var(org.apache.doris.plsql.Var.Type.TIMESTAMP, new Timestamp(timeInMs)));
        } catch (Exception e) {
            exec.signal(e);
            evalNull();
        }
    }

    /**
     * FROM_UNIXTIME() function (convert seconds since 1970-01-01 00:00:00 to timestamp)
     */
    void fromUnixtime(Expr_func_paramsContext ctx) {
        int cnt = getParamCount(ctx);
        if (cnt == 0) {
            evalNull();
            return;
        }
        long epoch = evalPop(ctx.func_param(0).expr()).longValue();
        String format = "yyyy-MM-dd HH:mm:ss";
        if (cnt > 1) {
            format = evalPop(ctx.func_param(1).expr()).toString();
        }
        evalString(new SimpleDateFormat(format).format(new Date(epoch * 1000)));
    }

    /**
     * UNIX_TIMESTAMP() function (current date and time in seconds since 1970-01-01 00:00:00)
     */
    void unixTimestamp(Expr_func_paramsContext ctx) {
        evalVar(new Var(System.currentTimeMillis() / 1000));
    }

    public void currentTimeMillis(Expr_func_paramsContext ctx) {
        evalVar(new Var(System.currentTimeMillis()));
    }
}
