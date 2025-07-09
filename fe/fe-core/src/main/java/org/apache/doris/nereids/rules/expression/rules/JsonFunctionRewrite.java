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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonArray;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonArrayIgnoreNull;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToJson;
import org.apache.doris.nereids.types.JsonType;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * JsonFunctionRewrite
 * `JsonArray(col1, col2, col3)` => `JsonArray(ToJson(col1), ToJson(col2), ToJson(col3))`
 */
public class JsonFunctionRewrite implements ExpressionPatternRuleFactory {
    public static JsonFunctionRewrite INSTANCE = new JsonFunctionRewrite();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(JsonArray.class).then(JsonFunctionRewrite::rewriteJsonArrayArguments)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_ARRAY),
                matchesType(JsonArrayIgnoreNull.class).then(JsonFunctionRewrite::rewriteJsonArrayArguments)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_ARRAY_IGNORE_NULL)
        );
    }

    private static <T extends ScalarFunction> Expression rewriteJsonArrayArguments(T function) {
        List<Expression> convectedChildren = new ArrayList<Expression>();
        function.children().forEach(child -> {
            if (child.getDataType() instanceof JsonType) {
                convectedChildren.add(child);
            } else {
                convectedChildren.add(new ToJson(child));
            }
        });
        return function.withChildren(convectedChildren);
    }

}
