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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/functions/FunctionString.java
// and modified by Doris

package org.apache.doris.plsql.functions;

import org.apache.doris.nereids.PLParser.Expr_func_paramsContext;
import org.apache.doris.nereids.PLParser.Expr_spec_funcContext;
import org.apache.doris.plsql.Exec;
import org.apache.doris.plsql.executor.QueryExecutor;

public class FunctionString extends BuiltinFunctions {
    public FunctionString(Exec e, QueryExecutor queryExecutor) {
        super(e, queryExecutor);
    }

    /**
     * Register functions
     */
    @Override
    public void register(BuiltinFunctions f) {
        f.map.put("CONCAT", this::concat);
        f.map.put("CHAR", this::char_);
        f.map.put("INSTR", this::instr);
        f.map.put("LEN", this::len);
        f.map.put("LENGTH", this::length);
        f.map.put("LOWER", this::lower);
        f.map.put("REPLACE", this::replace);
        f.map.put("SUBSTR", this::substr);
        f.map.put("SUBSTRING", this::substr);
        f.map.put("TO_CHAR", this::toChar);
        f.map.put("UPPER", this::upper);

        f.specMap.put("SUBSTRING", this::substring);
        f.specMap.put("TRIM", this::trim);
    }

    /**
     * CONCAT function
     */
    void concat(Expr_func_paramsContext ctx) {
        StringBuilder val = new StringBuilder();
        int cnt = getParamCount(ctx);
        boolean nulls = true;
        for (int i = 0; i < cnt; i++) {
            org.apache.doris.plsql.Var c = evalPop(ctx.func_param(i).expr());
            if (!c.isNull()) {
                val.append(c.toString());
                nulls = false;
            }
        }
        if (nulls) {
            evalNull();
        } else {
            evalString(val);
        }
    }

    /**
     * CHAR function
     */
    void char_(Expr_func_paramsContext ctx) {
        int cnt = getParamCount(ctx);
        if (cnt != 1) {
            evalNull();
            return;
        }
        String str = evalPop(ctx.func_param(0).expr()).toString();
        evalString(str);
    }

    /**
     * INSTR function
     */
    void instr(Expr_func_paramsContext ctx) {
        int cnt = getParamCount(ctx);
        if (cnt < 2) {
            evalNull();
            return;
        }
        String str = evalPop(ctx.func_param(0).expr()).toString();
        if (str == null) {
            evalNull();
            return;
        } else if (str.isEmpty()) {
            evalInt(0);
            return;
        }
        String substr = evalPop(ctx.func_param(1).expr()).toString();
        int pos = 1;
        int occur = 1;
        int idx = 0;
        if (cnt >= 3) {
            pos = evalPop(ctx.func_param(2).expr()).intValue();
            if (pos == 0) {
                pos = 1;
            }
        }
        if (cnt >= 4) {
            occur = evalPop(ctx.func_param(3).expr()).intValue();
            if (occur < 0) {
                occur = 1;
            }
        }
        for (int i = occur; i > 0; i--) {
            if (pos > 0) {
                idx = str.indexOf(substr, pos - 1);
            } else {
                str = str.substring(0, str.length() - pos * (-1));
                idx = str.lastIndexOf(substr);
            }
            if (idx == -1) {
                idx = 0;
                break;
            } else {
                idx++;
            }
            if (i > 1) {
                if (pos > 0) {
                    pos = idx + 1;
                } else {
                    pos = (str.length() - idx + 1) * (-1);
                }
            }
        }
        evalInt(idx);
    }

    /**
     * LEN function (excluding trailing spaces)
     */
    void len(Expr_func_paramsContext ctx) {
        if (ctx.func_param().size() != 1) {
            evalNull();
            return;
        }
        int len = evalPop(ctx.func_param(0).expr()).toString().trim().length();
        evalInt(len);
    }

    /**
     * LENGTH function
     */
    void length(Expr_func_paramsContext ctx) {
        if (ctx.func_param().size() != 1) {
            evalNull();
            return;
        }
        int len = evalPop(ctx.func_param(0).expr()).toString().length();
        evalInt(len);
    }

    /**
     * LOWER function
     */
    void lower(Expr_func_paramsContext ctx) {
        if (ctx.func_param().size() != 1) {
            evalNull();
            return;
        }
        String str = evalPop(ctx.func_param(0).expr()).toString().toLowerCase();
        evalString(str);
    }

    /**
     * REPLACE function
     */
    void replace(Expr_func_paramsContext ctx) {
        int cnt = getParamCount(ctx);
        if (cnt < 3) {
            evalNull();
            return;
        }
        String str = evalPop(ctx.func_param(0).expr()).toString();
        String what = evalPop(ctx.func_param(1).expr()).toString();
        String with = evalPop(ctx.func_param(2).expr()).toString();
        evalString(str.replaceAll(what, with));
    }

    /**
     * SUBSTR and SUBSTRING function
     */
    void substr(Expr_func_paramsContext ctx) {
        int cnt = getParamCount(ctx);
        if (cnt < 2) {
            evalNull();
            return;
        }
        String str = evalPop(ctx.func_param(0).expr()).toString();
        int start = evalPop(ctx.func_param(1).expr()).intValue();
        int len = -1;
        if (start == 0) {
            start = 1;
        }
        if (cnt > 2) {
            len = evalPop(ctx.func_param(2).expr()).intValue();
        }
        substr(str, start, len);
    }

    void substr(String str, int start, int len) {
        if (str == null) {
            evalNull();
            return;
        } else if (str.isEmpty()) {
            evalString(str);
            return;
        }
        if (start == 0) {
            start = 1;
        }
        if (len == -1) {
            if (start > 0) {
                evalString(str.substring(start - 1));
            }
        } else {
            evalString(str.substring(start - 1, start - 1 + len));
        }
    }

    /**
     * SUBSTRING FROM FOR function
     */
    void substring(Expr_spec_funcContext ctx) {
        String str = evalPop(ctx.expr(0)).toString();
        int start = evalPop(ctx.expr(1)).intValue();
        int len = -1;
        if (start == 0) {
            start = 1;
        }
        if (ctx.FOR() != null) {
            len = evalPop(ctx.expr(2)).intValue();
        }
        substr(str, start, len);
    }

    /**
     * TRIM function
     */
    void trim(Expr_spec_funcContext ctx) {
        int cnt = ctx.expr().size();
        if (cnt != 1) {
            evalNull();
            return;
        }
        String str = evalPop(ctx.expr(0)).toString();
        evalString(str.trim());
    }

    /**
     * TO_CHAR function
     */
    void toChar(Expr_func_paramsContext ctx) {
        int cnt = getParamCount(ctx);
        if (cnt != 1) {
            evalNull();
            return;
        }
        String str = evalPop(ctx.func_param(0).expr()).toString();
        evalString(str);
    }

    /**
     * UPPER function
     */
    void upper(Expr_func_paramsContext ctx) {
        if (ctx.func_param().size() != 1) {
            evalNull();
            return;
        }
        String str = evalPop(ctx.func_param(0).expr()).toString().toUpperCase();
        evalString(str);
    }
}
