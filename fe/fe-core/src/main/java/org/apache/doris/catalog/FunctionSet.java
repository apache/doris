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

package org.apache.doris.catalog;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class FunctionSet<T> {

    // For most build-in functions, it will return NullLiteral when params contain NullLiteral.
    // But a few functions need to handle NullLiteral differently, such as "if". It need to add
    // an attribute to LiteralExpr to mark null and check the attribute to decide whether to
    // replace the result with NullLiteral when function finished. It leaves to be realized.

    private ImmutableSet<String> nullResultWithOneNullParamFunctions;

    // Including now(), curdate(), etc..
    private ImmutableSet<String> nondeterministicFunctions;

    public FunctionSet() {
    }

    public boolean isNondeterministicFunction(String funcName) {
        return nondeterministicFunctions.contains(funcName);
    }

    public boolean isNullResultWithOneNullParamFunctions(String funcName) {
        return nullResultWithOneNullParamFunctions.contains(funcName);
    }

    public static final Set<String> nonDeterministicFunctions =
            ImmutableSet.<String>builder()
                    .add("RAND")
                    .add("RANDOM")
                    .add("RANDOM_BYTES")
                    .add("CONNECTION_ID")
                    .add("DATABASE")
                    .add("USER")
                    .add("UUID")
                    .add("CURRENT_USER")
                    .add("UUID_NUMERIC")
                    .build();

    public static final Set<String> nonDeterministicTimeFunctions =
            ImmutableSet.<String>builder()
                    .add("NOW")
                    .add("CURDATE")
                    .add("CURRENT_DATE")
                    .add("UTC_TIMESTAMP")
                    .add("CURTIME")
                    .add("CURRENT_TIMESTAMP")
                    .add("CURRENT_TIME")
                    .add("UNIX_TIMESTAMP")
                    .add()
                    .build();

    public static final String HLL_HASH = "hll_hash";
    public static final String HLL_UNION = "hll_union";
    public static final String HLL_UNION_AGG = "hll_union_agg";
    public static final String HLL_RAW_AGG = "hll_raw_agg";
    public static final String HLL_FROM_BASE64 = "hll_from_base64";

    public static final String TO_BITMAP = "to_bitmap";
    public static final String BITMAP_UNION = "bitmap_union";
    public static final String BITMAP_UNION_COUNT = "bitmap_union_count";
    public static final String BITMAP_UNION_INT = "bitmap_union_int";
    public static final String INTERSECT_COUNT = "intersect_count";
    public static final String BITMAP_INTERSECT = "bitmap_intersect";
    public static final String ORTHOGONAL_BITMAP_INTERSECT = "orthogonal_bitmap_intersect";
    public static final String ORTHOGONAL_BITMAP_INTERSECT_COUNT = "orthogonal_bitmap_intersect_count";
    public static final String ORTHOGONAL_BITMAP_UNION_COUNT = "orthogonal_bitmap_union_count";
    public static final String NDV = "ndv";
    public static final String ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT = "orthogonal_bitmap_expr_calculate_count";
    public static final String ORTHOGONAL_BITMAP_EXPR_CALCULATE = "orthogonal_bitmap_expr_calculate";

    //TODO(weixiang): is quantile_percent can be replaced by approx_percentile?
    public static final String COLLECT_LIST = "collect_list";
    public static final String COLLECT_SET = "collect_set";
    public static final String HISTOGRAM = "histogram";
    public static final String LINEAR_HISTOGRAM = "linear_histogram";
    public static final String MAP_AGG = "map_agg";

    public static final String BITMAP_AGG = "bitmap_agg";

    public static final String COUNT = "count";

    public static final String REGR_INTERCEPT = "regr_intercept";

    public static final String REGR_SLOPE = "regr_slope";

    public static final String SEQUENCE_COUNT = "sequence_count";

    public static final String GROUP_ARRAY_INTERSECT = "group_array_intersect";

    public static final String ARRAY_AGG = "array_agg";

    public static final String SUM0 = "sum0";

    public static final String MULTI_DISTINCT_SUM0 = "multi_distinct_sum0";
}
