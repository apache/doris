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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonArray;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonArrayIgnoreNull;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonInsert;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonObject;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonReplace;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonSet;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonUnQuote;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractBigint;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractBool;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractDouble;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractInt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractLargeint;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToJson;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MoreFieldsThread;

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
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_ARRAY_IGNORE_NULL),
                matchesType(JsonObject.class).then(JsonFunctionRewrite::rewriteJsonObjectArguments)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_OBJECT),
                matchesType(JsonInsert.class).then(JsonFunctionRewrite::rewriteJsonModifyArguments)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_INSERT),
                matchesType(JsonSet.class).then(JsonFunctionRewrite::rewriteJsonModifyArguments)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_SET),
                matchesType(JsonReplace.class).then(JsonFunctionRewrite::rewriteJsonModifyArguments)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_REPLACE),
                matchesType(JsonbExtractInt.class).then(JsonFunctionRewrite::rewriteJsonExtractFunctions)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_EXTRACT_INT),
                matchesType(JsonbExtractBigint.class).then(JsonFunctionRewrite::rewriteJsonExtractFunctions)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_EXTRACT_BIGINT),
                matchesType(JsonbExtractLargeint.class).then(JsonFunctionRewrite::rewriteJsonExtractFunctions)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_EXTRACT_LARGEINT),
                matchesType(JsonbExtractBool.class).then(JsonFunctionRewrite::rewriteJsonExtractFunctions)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_EXTRACT_BOOLEAN),
                matchesType(JsonbExtractDouble.class).then(JsonFunctionRewrite::rewriteJsonExtractFunctions)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_EXTRACT_DOUBLE),
                matchesType(JsonbExtractString.class).then(JsonFunctionRewrite::rewriteJsonExtractFunctions)
                        .toRule(ExpressionRuleType.JSON_FUNCTION_REWRITE_JSON_EXTRACT_STRING)
        );
    }

    private static <T extends ScalarFunction> Expression rewriteJsonArrayArguments(T function) {
        return MoreFieldsThread.keepFunctionSignature(false, () -> {
            List<Expression> convectedChildren = new ArrayList<>();
            for (Expression child : function.children()) {
                if (child.getDataType() instanceof JsonType) {
                    convectedChildren.add(child);
                } else {
                    convectedChildren.add(new ToJson(child));
                }
            }
            return function.withChildren(convectedChildren);
        });
    }

    private static Expression rewriteJsonObjectArguments(JsonObject function) {
        return MoreFieldsThread.keepFunctionSignature(false, () -> {
            List<Expression> convectedChildren = new ArrayList<Expression>();
            List<Expression> children = function.children();
            for (int i = 0; i < children.size(); i++) {
                Expression child = children.get(i);
                if (i % 2 == 0) {
                    convectedChildren.add(child);
                } else if (child.getDataType() instanceof JsonType) {
                    convectedChildren.add(child);
                } else {
                    convectedChildren.add(new ToJson(child));
                }
            }
            return function.withChildren(convectedChildren);
        });
    }

    private static <T extends ScalarFunction> Expression rewriteJsonModifyArguments(T function) {
        return MoreFieldsThread.keepFunctionSignature(false, () -> {
            List<Expression> convectedChildren = new ArrayList<Expression>();
            List<Expression> children = function.children();

            convectedChildren.add(children.get(0));
            for (int i = 1; i < children.size(); i++) {
                Expression child = children.get(i);
                if (i % 2 == 1) {
                    convectedChildren.add(child);
                } else if (child.getDataType() instanceof JsonType) {
                    convectedChildren.add(child);
                } else {
                    convectedChildren.add(new ToJson(child));
                }
            }
            return function.withChildren(convectedChildren);
        });
    }

    private static <T extends ScalarFunction> Expression rewriteJsonExtractFunctions(T function) {
        JsonbExtract jsonExtract = new JsonbExtract(function.children().get(0), function.children().get(1));
        if (function instanceof JsonbExtractInt) {
            return new Cast(jsonExtract, IntegerType.INSTANCE, false);
        } else if (function instanceof JsonbExtractBigint) {
            return new Cast(jsonExtract, BigIntType.INSTANCE, false);
        } else if (function instanceof JsonbExtractLargeint) {
            return new Cast(jsonExtract, LargeIntType.INSTANCE, false);
        } else if (function instanceof JsonbExtractBool) {
            return new Cast(jsonExtract, BooleanType.INSTANCE, false);
        } else if (function instanceof JsonbExtractDouble) {
            return new Cast(jsonExtract, DoubleType.INSTANCE, false);
        } else if (function instanceof JsonbExtractString) {
            return new JsonUnQuote(new Cast(jsonExtract, StringType.INSTANCE, false));
        } else {
            return function;
        }
    }

}
