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

/** horn wire 上的 fn_name 跟 Doris BoundFunction 的双向桥接 */
public final class DorisFunctionBuilder {

    /** forward + backward 共享的 scalar fn 白名单 */
    public static final ImmutableSet<String> KNOWN_SCALAR_FNS = ImmutableSet.of(
            "abs", "ceil", "coalesce", "concat",
            "datediff", "date_add", "date_format", "date_sub",
            "day", "floor", "from_unixtime", "greatest",
            "if", "ifnull", "least", "length",
            "like", "lower", "ltrim", "mod",
            "month", "nullif", "power", "quarter",
            "regexp", "regexp_like", "rlike",
            "round", "rtrim", "sqrt", "substring",
            "to_date", "trim", "unix_timestamp", "upper",
            "year",
            // 日期算术（TimestampArithmetic.getFuncName() 形态 + DaysAdd/DaysSub ScalarFunction 形态）
            "days_add", "days_sub", "weeks_add", "weeks_sub",
            "months_add", "months_sub", "quarters_add", "quarters_sub",
            "years_add", "years_sub",
            // Math 高阶
            "sin", "cos", "tan", "asin", "acos", "atan", "atan2",
            "sinh", "cosh", "tanh", "asinh", "acosh", "atanh",
            "log", "log2", "log10", "ln", "exp", "pi", "e",
            "sign", "pow", "radians", "degrees", "cbrt", "cot",
            "bin", "conv", "hex", "unhex",
            // Random (side-effectful, but Doris allows)
            "rand", "random",
            // 字符串扩展
            "split_part", "replace", "reverse", "repeat",
            "lpad", "rpad", "locate", "find_in_set",
            "strleft", "strright", "ascii", "char_length",
            "concat_ws", "instr", "space", "left", "right",
            // Regexp 家族
            "regexp_extract", "regexp_extract_all",
            "regexp_replace", "regexp_replace_one", "regexp_count",
            // Hash / 编码
            "md5", "sha1", "sha2", "crc32",
            "to_base64", "from_base64", "aes_encrypt", "aes_decrypt",
            // 日期高阶
            "date_trunc", "week", "weekofyear", "yearweek",
            "dayofweek", "dayofyear", "dayofmonth", "dayname",
            "hour", "minute", "second", "microsecond",
            "curdate", "curtime", "now", "current_timestamp",
            "add_time", "convert_tz",
            // 类型转换 / 元信息
            "uuid", "version", "connection_id", "database",
            "current_catalog", "current_user");

    /** forward + backward 共享的 window fn 白名单 */
    public static final ImmutableSet<String> KNOWN_WINDOW_FNS = ImmutableSet.of(
            "cume_dist", "dense_rank", "first_value", "lag", "last_value",
            "lead", "nth_value", "ntile", "percent_rank", "rank", "row_number");

    /** forward + backward 共享的 agg fn 白名单 */
    public static final ImmutableSet<String> KNOWN_AGG_FNS = ImmutableSet.of(
            "any_value", "avg", "count", "max", "min", "multi_distinct_count",
            "multi_distinct_sum", "stddev_samp", "sum",
            // 分位 / 统计
            "stddev", "stddev_pop", "variance", "variance_pop",
            "variance_samp", "var_samp", "var_pop",
            "covar", "covar_samp",
            "corr", "corr_welford",
            "median", "percentile", "percentile_approx",
            // 聚合位运算 / 布尔
            "group_bit_and", "group_bit_or", "group_bit_xor",
            "bool_and", "bool_or", "bool_xor",
            // 位置 / 分组
            "max_by", "min_by", "avg_weighted",
            // 分组拼接
            "group_concat", "multi_distinct_group_concat",
            // 特殊聚合
            "sum0", "multi_distinct_sum0", "count_by_enum",
            "topn", "topn_array", "topn_weighted",
            "retention", "sequence_count", "sequence_match",
            "window_funnel",
            "kurt", "skew");

    /** 反译：fn_name + children → BoundFunction（含 implicit cast） */
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
