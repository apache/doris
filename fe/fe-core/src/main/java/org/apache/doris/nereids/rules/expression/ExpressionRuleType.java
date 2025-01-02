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

package org.apache.doris.nereids.rules.expression;

/**
 * Type of rewrite expression rules.
 */
public enum ExpressionRuleType {
    ADD_MIN_MAX,
    ARRAY_CONTAIN_TO_ARRAY_OVERLAP,
    BETWEEN_TO_EQUAL,
    CASE_WHEN_TO_IF,
    CHECK_CAST,
    CONVERT_AGG_STATE_CAST,
    DATE_FUNCTION_REWRITE,
    DIGITAL_MASKING_CONVERT,
    DISTINCT_PREDICATES,
    EXPR_ID_REWRITE_REPLACE,
    EXTRACT_COMMON_FACTOR,
    FOLD_CONSTANT_ON_BE,
    FOLD_CONSTANT_ON_FE,
    IN_PREDICATE_DEDUP,
    IN_PREDICATE_TO_EQUAL_TO,
    LIKE_TO_EQUAL,
    MERGE_DATE_TRUNC,
    NORMALIZE_BINARY_PREDICATES,
    NULL_SAFE_EQUAL_TO_EQUAL,
    OR_TO_IN,
    REPLACE_VARIABLE_BY_LITERAL,
    SIMPLIFY_ARITHMETIC_COMPARISON,
    SIMPLIFY_ARITHMETIC,
    SIMPLIFY_CAST,
    SIMPLIFY_COMPARISON_PREDICATE,
    SIMPLIFY_CONDITIONAL_FUNCTION,
    SIMPLIFY_IN_PREDICATE,
    SIMPLIFY_NOT_EXPR,
    SIMPLIFY_RANGE,
    SUPPORT_JAVA_DATE_FORMATTER,
    TOPN_TO_MAX;

    public int type() {
        return ordinal();
    }
}
