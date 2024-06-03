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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Change functio name to function class name on nereids alias list:
 * catalog/BuiltinScalarFunctions.java
 */
public final class FunctionAlias implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new FunctionAlias();

    static final Map<String, String> aliasToName = ImmutableMap.<String, String>builder()
            .put("array_size", "cardinality").put("size", "cardinality").put("ceiling", "ceil")
            .put("char_length", "character_length").put("curdate", "current_date").put("curtime", "current_time")
            .put("schema", "database").put("day", "dayofmonth").put("date_add", "days_add").put("adddate", "days_add")
            .put("date_sub", "days_sub").put("subdate", "days_sub").put("inet_ntoa", "ipv4_num_to_string")
            .put("inet_aton", "ipv4_string_to_num_or_null").put("inet6_ntoa", "ipv6_num_to_string")
            .put("inet6_aton", "ipv6_string_to_num_or_null").put("lcase", "lower").put("add_months", "months_add")
            .put("current_timestamp", "now").put("localtime", "now").put("localtimestamp", "now").put("nvl", "ifnull")
            .put("rand", "random").put("sha", "sha1").put("substr", "substring").put("ucase", "upper")
            .put("approx_count_distinct", "ndv").build();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr functionCall = (FunctionCallExpr) expr;
        if (aliasToName.containsKey(functionCall.getFnName().getFunction())) {
            FunctionCallExpr result = (FunctionCallExpr) functionCall.clone();
            result.getFnName().setFn(aliasToName.get(functionCall.getFnName().getFunction()));
            return result;
        }
        return expr;
    }
}
