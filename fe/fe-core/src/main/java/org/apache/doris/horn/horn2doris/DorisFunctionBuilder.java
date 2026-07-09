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

package org.apache.doris.horn.horn2doris;


import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

/**
 * horn wire 上的 fn_name 跟 Doris BoundFunction 的双向桥接：forward 用 {@link #KNOWN_SCALAR_FNS}
 * 白名单 gate，backward 调 {@link #build(String, List)} 复用 Doris {@link FunctionRegistry}
 * （lookup → canApply → build → TypeCoercion，同 BindExpression 路径）。
 */
public final class DorisFunctionBuilder {

    /**
     * forward + backward 共享的 scalar fn 白名单。小写、须跟 BuiltinScalarFunctions primary name 一致。
     */
    public static final ImmutableSet<String> KNOWN_SCALAR_FNS = ImmutableSet.of(
            "abs", "ceil", "coalesce", "concat",
            "datediff", "date_add", "date_format", "date_sub",
            "day", "floor", "from_unixtime", "greatest",
            "if", "ifnull", "least", "length",
            "like", "lower", "ltrim", "mod",
            "month", "nullif", "power", "quarter",
            "round", "rtrim", "sqrt", "substring",
            "to_date", "trim", "unix_timestamp", "upper",
            "year",
            // 日期算术（TimestampArithmetic + DaysAdd/DaysSub 两种形态）
            "days_add", "days_sub", "weeks_add", "weeks_sub",
            "months_add", "months_sub", "quarters_add", "quarters_sub",
            "years_add", "years_sub");

    /**
     * forward + backward 共享的 window fn 白名单，走 {@link #build} 同款 FunctionRegistry lookup。
     */
    public static final ImmutableSet<String> KNOWN_WINDOW_FNS = ImmutableSet.of(
            "cume_dist", "dense_rank", "first_value", "lag", "last_value",
            "lead", "nth_value", "ntile", "percent_rank", "rank", "row_number");

    /**
     * forward + backward 共享的 agg fn 白名单，走 {@link #build} 同款 lookup（canApply 按 arity 选
     * ctor）。multi_distinct_count / multi_distinct_sum 是 forward 端 distinct 重写后的 builtin 名。
     */
    public static final ImmutableSet<String> KNOWN_AGG_FNS = ImmutableSet.of(
            "any_value", "avg", "count", "max", "min", "multi_distinct_count",
            "multi_distinct_sum", "stddev_samp", "sum");

    /**
     * 反译：fn_name + children → BoundFunction。走 {@link FunctionRegistry#tryGetBuiltinBuilders}，
     * canApply 按 arity + 参数类型选 ctor，最后 {@link TypeCoercionUtils#processBoundFunction} 补 implicit cast。
     */
    public static Expression build(String fnName, List<Expression> children) {
        FunctionRegistry registry = Env.getCurrentEnv().getFunctionRegistry();
        Optional<List<FunctionBuilder>> builders = registry.tryGetBuiltinBuilders(fnName);
        if (!builders.isPresent() || builders.get().isEmpty()) {
            throw new UnsupportedOperationException(
                    "DorisFunctionBuilder: no builtin builder for fn_name=" + fnName);
        }
        FunctionBuilder picked = null;
        for (FunctionBuilder fb : builders.get()) {
            if (fb.canApply(children)) {
                picked = fb;
                break;
            }
        }
        if (picked == null) {
            throw new UnsupportedOperationException(
                    "DorisFunctionBuilder: no ctor for fn=" + fnName
                            + " arity=" + children.size());
        }
        BoundFunction fn = (BoundFunction) picked.build(fnName, children).second;
        return (Expression) TypeCoercionUtils.processBoundFunction(fn);
    }

    private DorisFunctionBuilder() {
    }
}
