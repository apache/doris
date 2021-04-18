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

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.builtins.ScalarBuiltins;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FunctionSet {
    private static final Logger LOG = LogManager.getLogger(FunctionSet.class);

    // All of the registered user functions. The key is the user facing name (e.g. "myUdf"),
    // and the values are all the overloaded variants (e.g. myUdf(double), myUdf(string))
    // This includes both UDFs and UDAs. Updates are made thread safe by synchronizing
    // on this map. Functions are sorted in a canonical order defined by
    // FunctionResolutionOrder.
    private final HashMap<String, List<Function>> functions;

    // For most build-in functions, it will return NullLiteral when params contain NullLiteral.
    // But a few functions need to handle NullLiteral differently, such as "if". It need to add
    // an attribute to LiteralExpr to mark null and check the attribute to decide whether to
    // replace the result with NullLiteral when function finished. It leaves to be realized.
    // Functions in this set is defined in `gensrc/script/doris_builtins_functions.py`,
    // and will be built automatically.

    // cmy: This does not contain any user defined functions. All UDFs handle null values by themselves.
    private ImmutableSet<String> nonNullResultWithNullParamFunctions;

    public FunctionSet() {
        functions = Maps.newHashMap();
    }

    public void init() {
        // Populate all aggregate builtins.
        initAggregateBuiltins();

        ArithmeticExpr.initBuiltins(this);
        BinaryPredicate.initBuiltins(this);
        CastExpr.initBuiltins(this);
        IsNullPredicate.initBuiltins(this);
        ScalarBuiltins.initBuiltins(this);
        LikePredicate.initBuiltins(this);
        InPredicate.initBuiltins(this);
    }

    public void buildNonNullResultWithNullParamFunction(Set<String> funcNames) {
        ImmutableSet.Builder<String> setBuilder = new ImmutableSet.Builder<String>();
        for (String funcName : funcNames) {
            setBuilder.add(funcName);
        }
        this.nonNullResultWithNullParamFunctions = setBuilder.build();
    }

    public boolean isNonNullResultWithNullParamFunctions(String funcName) {
        return nonNullResultWithNullParamFunctions.contains(funcName);
    }

    private static final Map<Type, String> MIN_UPDATE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                    "3minIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.TINYINT,
                    "3minIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.SMALLINT,
                    "3minIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.INT,
                    "3minIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.BIGINT,
                    "3minIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.FLOAT,
                    "3minIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DOUBLE,
                    "3minIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
               // .put(Type.CHAR,
               //     "3minIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.VARCHAR,
                    "3minIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATE,
                    "3minIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATETIME,
                    "3minIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMAL,
                    "3minIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMALV2,
                    "3minIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.LARGEINT,
                    "3minIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .build();

    private static final Map<Type, String> MAX_UPDATE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                    "3maxIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.TINYINT,
                    "3maxIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.SMALLINT,
                    "3maxIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.INT,
                    "3maxIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.BIGINT,
                    "3maxIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.FLOAT,
                    "3maxIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DOUBLE,
                    "3maxIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                // .put(Type.CHAR,
                //    "3maxIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.VARCHAR,
                        "3maxIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATE,
                    "3maxIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATETIME,
                    "3maxIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMAL,
                    "3maxIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMALV2,
                    "3maxIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.LARGEINT,
                    "3maxIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
               .build();

    private static final Map<Type, Type> MULTI_DISTINCT_SUM_RETURN_TYPE =
             ImmutableMap.<Type, Type>builder()
                    .put(Type.TINYINT, Type.BIGINT)
                    .put(Type.SMALLINT, Type.BIGINT)
                    .put(Type.INT, Type.BIGINT)
                    .put(Type.BIGINT, Type.BIGINT)
                    .put(Type.FLOAT, Type.DOUBLE)
                    .put(Type.DOUBLE, Type.DOUBLE)
                    .put(Type.LARGEINT, Type.LARGEINT)
                    .put(Type.DECIMAL, Type.DECIMAL)
                    .put(Type.DECIMALV2, Type.DECIMALV2)
                    .build();

    private static final Map<Type, String> MULTI_DISTINCT_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "34count_or_sum_distinct_numeric_initIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "34count_or_sum_distinct_numeric_initIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.INT,
                            "34count_or_sum_distinct_numeric_initIN9doris_udf6IntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.BIGINT,
                            "34count_or_sum_distinct_numeric_initIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.FLOAT,
                            "34count_or_sum_distinct_numeric_initIN9doris_udf8FloatValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "34count_or_sum_distinct_numeric_initIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "34count_or_sum_distinct_numeric_initIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .build();

    private static final Map<Type, String> MULTI_DISTINCT_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "36count_or_sum_distinct_numeric_updateIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "36count_or_sum_distinct_numeric_updateIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.INT,
                            "36count_or_sum_distinct_numeric_updateIN9doris_udf6IntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.BIGINT,
                            "36count_or_sum_distinct_numeric_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.FLOAT,
                            "36count_or_sum_distinct_numeric_updateIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "36count_or_sum_distinct_numeric_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "36count_or_sum_distinct_numeric_updateIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .build();

    private static final Map<Type, String> MULTI_DISTINCT_MERGE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "35count_or_sum_distinct_numeric_mergeIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.SMALLINT,
                            "35count_or_sum_distinct_numeric_mergeIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.INT,
                            "35count_or_sum_distinct_numeric_mergeIN9doris_udf6IntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.BIGINT,
                            "35count_or_sum_distinct_numeric_mergeIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.FLOAT,
                            "35count_or_sum_distinct_numeric_mergeIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.DOUBLE,
                            "35count_or_sum_distinct_numeric_mergeIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.LARGEINT,
                            "35count_or_sum_distinct_numeric_mergeIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .build();

    private static final Map<Type, String> MULTI_DISTINCT_SERIALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "39count_or_sum_distinct_numeric_serializeIN9doris_udf10TinyIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.SMALLINT,
                            "39count_or_sum_distinct_numeric_serializeIN9doris_udf11SmallIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.INT,
                            "39count_or_sum_distinct_numeric_serializeIN9doris_udf6IntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.BIGINT,
                            "39count_or_sum_distinct_numeric_serializeIN9doris_udf9BigIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.FLOAT,
                            "39count_or_sum_distinct_numeric_serializeIN9doris_udf8FloatValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.DOUBLE,
                            "39count_or_sum_distinct_numeric_serializeIN9doris_udf9DoubleValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.LARGEINT,
                            "39count_or_sum_distinct_numeric_serializeIN9doris_udf11LargeIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .build();

    private static final Map<Type, String> MULTI_DISTINCT_COUNT_FINALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "38count_or_sum_distinct_numeric_finalizeIN9doris_udf10TinyIntValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "38count_or_sum_distinct_numeric_finalizeIN9doris_udf11SmallIntValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.INT,
                            "38count_or_sum_distinct_numeric_finalizeIN9doris_udf8FloatValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.BIGINT,
                            "38count_or_sum_distinct_numeric_finalizeIN9doris_udf9BigIntValEEES3_PNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.FLOAT,
                            "38count_or_sum_distinct_numeric_finalizeIN9doris_udf8FloatValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "38count_or_sum_distinct_numeric_finalizeIN9doris_udf9DoubleValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "38count_or_sum_distinct_numeric_finalizeIN9doris_udf11LargeIntValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .build();


    private static final Map<Type, String> MULTI_DISTINCT_SUM_FINALIZE_SYMBOL =
             ImmutableMap.<Type, String>builder()
                    .put(Type.BIGINT,
                            "28sum_distinct_bigint_finalizeIN9doris_udf9BigIntValEEES3_PNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.FLOAT,
                            "28sum_distinct_double_finalizeIN9doris_udf9DoubleValEEES3_PNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "28sum_distinct_double_finalizeIN9doris_udf9DoubleValEEES3_PNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "30sum_distinct_largeint_finalizeIN9doris_udf11LargeIntValEEES3_PNS2_15FunctionContextERKNS2_9StringValE")
                    .build();

    private static final Map<Type, Type> STDDEV_RETTYPE_SYMBOL =
            ImmutableMap.<Type, Type>builder()
                    .put(Type.TINYINT, Type.DOUBLE)
                    .put(Type.SMALLINT, Type.DOUBLE)
                    .put(Type.INT, Type.DOUBLE)
                    .put(Type.BIGINT, Type.DOUBLE)
                    .put(Type.FLOAT, Type.DOUBLE)
                    .put(Type.DOUBLE, Type.DOUBLE)
                    .put(Type.DECIMALV2, Type.DECIMALV2)
                    .build();

    private static final Map<Type, String> STDDEV_INIT_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "14knuth_var_initEPN9doris_udf15FunctionContextEPNS1_9StringValE")
                .put(Type.SMALLINT,
                        "14knuth_var_initEPN9doris_udf15FunctionContextEPNS1_9StringValE")
                .put(Type.INT,
                        "14knuth_var_initEPN9doris_udf15FunctionContextEPNS1_9StringValE")
                .put(Type.BIGINT,
                        "14knuth_var_initEPN9doris_udf15FunctionContextEPNS1_9StringValE")
                .put(Type.FLOAT,
                        "14knuth_var_initEPN9doris_udf15FunctionContextEPNS1_9StringValE")
                .put(Type.DOUBLE,
                        "14knuth_var_initEPN9doris_udf15FunctionContextEPNS1_9StringValE")
                .put(Type.DECIMALV2,
                        "24decimalv2_knuth_var_initEPN9doris_udf15FunctionContextEPNS1_9StringValE")
                .build();

    private static final Map<Type, String> STDDEV_UPDATE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "16knuth_var_updateIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.SMALLINT,
                        "16knuth_var_updateIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.INT,
                        "16knuth_var_updateIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.BIGINT,
                        "16knuth_var_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.FLOAT,
                        "16knuth_var_updateIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.DOUBLE,
                        "16knuth_var_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.DECIMALV2,
                        "16knuth_var_updateEPN9doris_udf15FunctionContextERKNS1_12DecimalV2ValEPNS1_9StringValE")
                .build();

    private static final Map<Type, String> STDDEV_MERGE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "15knuth_var_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .put(Type.SMALLINT,
                        "15knuth_var_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .put(Type.INT,
                        "15knuth_var_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .put(Type.BIGINT,
                        "15knuth_var_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .put(Type.FLOAT,
                        "15knuth_var_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .put(Type.DOUBLE,
                        "15knuth_var_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .put(Type.DECIMALV2,
                        "25decimalv2_knuth_var_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .build();

    private static final Map<Type, String> STDDEV_FINALIZE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "21knuth_stddev_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.SMALLINT,
                        "21knuth_stddev_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.INT,
                        "21knuth_stddev_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.BIGINT,
                        "21knuth_stddev_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.FLOAT,
                        "21knuth_stddev_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DOUBLE,
                        "21knuth_stddev_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DECIMALV2,
                        "31decimalv2_knuth_stddev_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .build();

    private static final Map<Type, String> STDDEV_POP_FINALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "25knuth_stddev_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                    .put(Type.SMALLINT,
                            "25knuth_stddev_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                    .put(Type.INT,
                            "25knuth_stddev_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                    .put(Type.BIGINT,
                            "25knuth_stddev_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                    .put(Type.FLOAT,
                            "25knuth_stddev_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                    .put(Type.DOUBLE,
                            "25knuth_stddev_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                    .put(Type.DECIMALV2,
                            "35decimalv2_knuth_stddev_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                    .build();

    private static final Map<Type, String> VAR_FINALIZE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "18knuth_var_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.SMALLINT,
                        "18knuth_var_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.INT,
                        "18knuth_var_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.BIGINT,
                        "18knuth_var_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.FLOAT,
                        "18knuth_var_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DOUBLE,
                        "18knuth_var_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DECIMALV2,
                        "28decimalv2_knuth_var_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .build();

    private static final Map<Type, String> VAR_POP_FINALIZE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "22knuth_var_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.SMALLINT,
                        "22knuth_var_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.INT,
                        "22knuth_var_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.BIGINT,
                        "22knuth_var_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.FLOAT,
                        "22knuth_var_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DOUBLE,
                        "22knuth_var_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DECIMALV2,
                        "32decimalv2_knuth_var_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .build();

    public static final String HLL_HASH = "hll_hash";
    public static final String HLL_UNION = "hll_union";

    private static final Map<Type, String> HLL_UPDATE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                    "10hll_updateIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.TINYINT,
                    "10hll_updateIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.SMALLINT,
                    "10hll_updateIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.INT,
                    "10hll_updateIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.BIGINT,
                    "10hll_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.FLOAT,
                    "10hll_updateIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.DOUBLE,
                    "10hll_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                // .put(Type.CHAR,
                //    "10hll_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
                .put(Type.VARCHAR,
                    "10hll_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
                .put(Type.DATE,
                    "10hll_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.DATETIME,
                    "10hll_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.DECIMAL,
                    "10hll_updateIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.DECIMALV2,
                    "10hll_updateIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.LARGEINT,
                    "10hll_updateIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .build();


    private static final Map<Type, String> HLL_UNION_AGG_UPDATE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.VARCHAR,
                        "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .put(Type.HLL,
                        "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .build();

    private static final Map<Type, String> OFFSET_FN_INIT_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                     "14offset_fn_initIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.DECIMAL,
                     "14offset_fn_initIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.DECIMALV2,
                     "14offset_fn_initIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.TINYINT,
                     "14offset_fn_initIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.SMALLINT,
                     "14offset_fn_initIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.DATE,
                     "14offset_fn_initIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.DATETIME,
                     "14offset_fn_initIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.INT,
                     "14offset_fn_initIN9doris_udf6IntValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.FLOAT,
                     "14offset_fn_initIN9doris_udf8FloatValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.BIGINT,
                     "14offset_fn_initIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.DOUBLE,
                     "14offset_fn_initIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextEPT_")
                // .put(Type.CHAR,
                //     "14offset_fn_initIN9doris_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.VARCHAR,
                     "14offset_fn_initIN9doris_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.LARGEINT,
                     "14offset_fn_initIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextEPT_")

                .build();

    private static final Map<Type, String> OFFSET_FN_UPDATE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                     "16offset_fn_updateIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.DECIMAL,
                     "16offset_fn_updateIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.DECIMALV2,
                     "16offset_fn_updateIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.TINYINT,
                     "16offset_fn_updateIN9doris_udf10TinyIntValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.SMALLINT,
                     "16offset_fn_updateIN9doris_udf11SmallIntValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.DATE,
                     "16offset_fn_updateIN9doris_udf11DateTimeValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.DATETIME,
                     "16offset_fn_updateIN9doris_udf11DateTimeValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.INT,
                     "16offset_fn_updateIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.FLOAT,
                     "16offset_fn_updateIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.BIGINT,
                     "16offset_fn_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKS3_S8_PS6_")
                .put(Type.DOUBLE,
                     "16offset_fn_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                // .put(Type.CHAR,
                //     "16offset_fn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.VARCHAR,
                     "16offset_fn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.LARGEINT,
                     "16offset_fn_updateIN9doris_udf11LargeIntValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .build();

    private static final Map<Type, String> LAST_VALUE_UPDATE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                     "15last_val_updateIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMAL,
                     "15last_val_updateIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMALV2,
                     "15last_val_updateIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.TINYINT,
                     "15last_val_updateIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.SMALLINT,
                     "15last_val_updateIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATE,
                     "15last_val_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATETIME,
                     "15last_val_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.INT,
                     "15last_val_updateIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.FLOAT,
                     "15last_val_updateIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.BIGINT,
                     "15last_val_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DOUBLE,
                     "15last_val_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                // .put(Type.CHAR,
                //     "15last_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.VARCHAR,
                     "15last_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.LARGEINT,
                     "15last_val_updateIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .build();

    private static final Map<Type, String> FIRST_VALUE_REWRITE_UPDATE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                     "24first_val_rewrite_updateIN9doris_udf10BooleanValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.DECIMAL,
                     "24first_val_rewrite_updateIN9doris_udf10DecimalValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.DECIMALV2,
                     "24first_val_rewrite_updateIN9doris_udf12DecimalV2ValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.TINYINT,
                     "24first_val_rewrite_updateIN9doris_udf10TinyIntValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.SMALLINT,
                     "24first_val_rewrite_updateIN9doris_udf11SmallIntValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.DATE,
                     "24first_val_rewrite_updateIN9doris_udf11DateTimeValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.DATETIME,
                     "24first_val_rewrite_updateIN9doris_udf11DateTimeValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.INT,
                     "24first_val_rewrite_updateIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.FLOAT,
                     "24first_val_rewrite_updateIN9doris_udf8FloatValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.BIGINT,
                     "24first_val_rewrite_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKS3_PS6_")
                .put(Type.DOUBLE,
                     "24first_val_rewrite_updateIN9doris_udf9DoubleValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.VARCHAR,
                     "24first_val_rewrite_updateIN9doris_udf9StringValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                .put(Type.LARGEINT,
                     "24first_val_rewrite_updateIN9doris_udf11LargeIntValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                // .put(Type.VARCHAR,
                //     "15last_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .build();

    private static final Map<Type, String> LAST_VALUE_REMOVE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                     "15last_val_removeIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMAL,
                     "15last_val_removeIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMALV2,
                     "15last_val_removeIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.TINYINT,
                     "15last_val_removeIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.SMALLINT,
                     "15last_val_removeIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATE,
                     "15last_val_removeIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATETIME,
                     "15last_val_removeIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.INT,
                     "15last_val_removeIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.FLOAT,
                     "15last_val_removeIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.BIGINT,
                     "15last_val_removeIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DOUBLE,
                     "15last_val_removeIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                // .put(Type.CHAR,
                //     "15last_val_removeIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.VARCHAR,
                     "15last_val_removeIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.LARGEINT,
                     "15last_val_removeIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .build();

    private static final Map<Type, String> FIRST_VALUE_UPDATE_SYMBOL =
        ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                     "16first_val_updateIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMAL,
                     "16first_val_updateIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DECIMALV2,
                     "16first_val_updateIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.TINYINT,
                     "16first_val_updateIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.SMALLINT,
                     "16first_val_updateIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATE,
                     "16first_val_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATETIME,
                     "16first_val_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.INT,
                     "16first_val_updateIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.FLOAT,
                     "16first_val_updateIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.BIGINT,
                     "16first_val_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DOUBLE,
                     "16first_val_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                // .put(Type.CHAR,
                //     "16first_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.VARCHAR,
                     "16first_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.LARGEINT,
                     "16first_val_updateIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")

                .build();

    public static final String TO_BITMAP = "to_bitmap";
    public static final String BITMAP_UNION = "bitmap_union";
    public static final String BITMAP_UNION_COUNT = "bitmap_union_count";
    public static final String BITMAP_UNION_INT = "bitmap_union_int";
    public static final String BITMAP_COUNT = "bitmap_count";
    public static final String INTERSECT_COUNT = "intersect_count";
    public static final String BITMAP_INTERSECT = "bitmap_intersect";

    private static final Map<Type, String> BITMAP_UNION_INT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions17bitmap_update_intIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions17bitmap_update_intIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions17bitmap_update_intIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .build();

    private static final Map<Type, String> BITMAP_INTERSECT_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initIaN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initIsN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initIiN9doris_udf6IntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.BIGINT,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initIlN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initInN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.FLOAT,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initIfN9doris_udf8FloatValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initIdN9doris_udf9DoubleValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.DATE,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initINS_13DateTimeValueEN9doris_udf11DateTimeValEEEvPNS3_15FunctionContextEPNS3_9StringValE")
                    .put(Type.DATETIME,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initINS_13DateTimeValueEN9doris_udf11DateTimeValEEEvPNS3_15FunctionContextEPNS3_9StringValE")
                    .put(Type.DECIMALV2,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initINS_14DecimalV2ValueEN9doris_udf12DecimalV2ValEEEvPNS3_15FunctionContextEPNS3_9StringValE")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .build();

    private static final Map<Type, String> BITMAP_INTERSECT_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateIaN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateIsN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateIiN9doris_udf6IntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.BIGINT,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateIlN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.LARGEINT,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateInN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.FLOAT,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateIfN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.DOUBLE,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateIdN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.DATE,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateINS_13DateTimeValueEN9doris_udf11DateTimeValEEEvPNS3_15FunctionContextERKNS3_9StringValERKT0_iPSB_PS8_")
                    .put(Type.DATETIME,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateINS_13DateTimeValueEN9doris_udf11DateTimeValEEEvPNS3_15FunctionContextERKNS3_9StringValERKT0_iPSB_PS8_")
                    .put(Type.DECIMALV2,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateINS_14DecimalV2ValueEN9doris_udf12DecimalV2ValEEEvPNS3_15FunctionContextERKNS3_9StringValERKT0_iPSB_PS8_")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextERKS4_RKT0_iPSA_PS7_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextERKS4_RKT0_iPSA_PS7_")
                    .build();

    private static final Map<Type, String> BITMAP_INTERSECT_MERGE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeIaEEvPN9doris_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeIsEEvPN9doris_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeIiEEvPN9doris_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.BIGINT,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeIlEEvPN9doris_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.LARGEINT,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeInEEvPN9doris_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.FLOAT,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeIfEEvPN9doris_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.DOUBLE,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeIdEEvPN9doris_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.DATE,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeINS_13DateTimeValueEEEvPN9doris_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.DATETIME,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeINS_13DateTimeValueEEEvPN9doris_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.DECIMALV2,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeINS_14DecimalV2ValueEEEvPN9doris_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeINS_11StringValueEEEvPN9doris_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeINS_11StringValueEEEvPN9doris_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .build();

    private static final Map<Type, String> BITMAP_INTERSECT_SERIALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeIaEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeIsEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeIiEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.BIGINT,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeIlEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.LARGEINT,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeInEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.FLOAT,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeIfEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.DOUBLE,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeIdEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.DATE,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeINS_13DateTimeValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.DATETIME,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeINS_13DateTimeValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.DECIMALV2,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeINS_14DecimalV2ValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .build();

    private static final Map<Type, String> BITMAP_INTERSECT_FINALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeIaEEN9doris_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeIsEEN9doris_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeIiEEN9doris_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.BIGINT,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeIlEEN9doris_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeInEEN9doris_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.FLOAT,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeIfEEN9doris_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeIdEEN9doris_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.DATE,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeINS_13DateTimeValueEEEN9doris_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.DATETIME,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeINS_13DateTimeValueEEEN9doris_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.DECIMALV2,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeINS_14DecimalV2ValueEEEN9doris_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeINS_11StringValueEEEN9doris_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeINS_11StringValueEEEN9doris_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .build();

    private static final Map<Type, String> TOPN_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.TINYINT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_RKS3_PNS2_9StringValE")
                    .put(Type.BIGINT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.FLOAT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.CHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPS3_")
                    .put(Type.VARCHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPS3_")
                    .put(Type.DATE,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.DATETIME,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.DECIMAL,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.DECIMALV2,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
                    .build();

    private static final Map<Type, String> TOPN_UPDATE_MORE_PARAM_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.TINYINT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_RKS3_SA_PNS2_9StringValE")
                    .put(Type.BIGINT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.FLOAT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.CHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PS3_")
                    .put(Type.VARCHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PS3_")
                    .put(Type.DATE,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.DATETIME,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.DECIMAL,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf10DecimalValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.DECIMALV2,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PNS2_9StringValE")
                    .build();

    public Function getFunction(Function desc, Function.CompareMode mode) {
        List<Function> fns = functions.get(desc.functionName());
        if (fns == null) {
            return null;
        }

        // First check for identical
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_IDENTICAL)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_IDENTICAL) {
            return null;
        }

        // Next check for indistinguishable
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_INDISTINGUISHABLE)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_INDISTINGUISHABLE) {
            return null;
        }

        // Next check for strict supertypes
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_SUPERTYPE_OF) && isCastMatchAllowed(desc, f)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_SUPERTYPE_OF) {
            return null;
        }

        // Finally check for non-strict supertypes
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF) && isCastMatchAllowed(desc, f)) {
                return f;
            }
        }
        return null;
    }

    /**
     * There are essential differences in the implementation of some functions for different
     * types params, which should be prohibited.
     * @param desc
     * @param candicate
     * @return
     */
    public static boolean isCastMatchAllowed(Function desc, Function candicate) {
        final String functionName = desc.getFunctionName().getFunction();
        final Type[] descArgTypes = desc.getArgs();
        final Type[] candicateArgTypes = candicate.getArgs();
        if (functionName.equalsIgnoreCase("hex")
                || functionName.equalsIgnoreCase("greast")
                || functionName.equalsIgnoreCase("least")
                || functionName.equalsIgnoreCase("lead")
                || functionName.equalsIgnoreCase("lag")) {
            final ScalarType descArgType = (ScalarType)descArgTypes[0];
            final ScalarType candicateArgType = (ScalarType)candicateArgTypes[0];
            if (!descArgType.isStringType() && candicateArgType.isStringType()) {
                // The implementations of hex for string and int are different.
                return false;
            }
        }
        return true;
    }

    public Function getFunction(String signatureString) {
        for (List<Function> fns : functions.values()) {
            for (Function f : fns) {
                if (f.signatureString().equals(signatureString)) {
                    return f;
                }
            }
        }
        return null;
    }

    private boolean addFunction(Function fn, boolean isBuiltin) {
        // TODO: add this to persistent store
        if (getFunction(fn, Function.CompareMode.IS_INDISTINGUISHABLE) != null) {
            return false;
        }
        List<Function> fns = functions.get(fn.functionName());
        if (fns == null) {
            fns = Lists.newArrayList();
            functions.put(fn.functionName(), fns);
        }
        if (fns.add(fn)) {
            return true;
        }
        return false;
    }

    /**
     * Add a builtin with the specified name and signatures to this
     * This defaults to not using a Prepare/Close function.
     */
    public void addScalarBuiltin(String fnName, String symbol, boolean userVisible,
                                 boolean varArgs, PrimitiveType retType, PrimitiveType ... args) {
        addScalarBuiltin(fnName, symbol, userVisible, null, null, varArgs, retType, args);
    }

    /**
     * Add a builtin with the specified name and signatures to this db.
     */
    public void addScalarBuiltin(String fnName, String symbol, boolean userVisible,
                                 String prepareFnSymbol, String closeFnSymbol, boolean varArgs,
                                 PrimitiveType retType, PrimitiveType ... args) {
        ArrayList<Type> argsType = new ArrayList<Type>();
        for (PrimitiveType type : args) {
            argsType.add(Type.fromPrimitiveType(type));
        }
        addBuiltin(ScalarFunction.createBuiltin(
                fnName, argsType, varArgs, Type.fromPrimitiveType(retType),
                symbol, prepareFnSymbol, closeFnSymbol, userVisible));
    }

    /**
     * Adds a builtin to this database. The function must not already exist.
     */
    public void addBuiltin(Function fn) {
        addFunction(fn, true);
    }

    public static final String COUNT = "count";
    // Populate all the aggregate builtins in the catalog.
    // null symbols indicate the function does not need that step of the evaluation.
    // An empty symbol indicates a TODO for the BE to implement the function.
    private void initAggregateBuiltins() {
        final String prefix = "_ZN5doris18AggregateFunctions";
        final String initNull = prefix + "9init_nullEPN9doris_udf15FunctionContextEPNS1_6AnyValE";
        final String initNullString = prefix
                + "16init_null_stringEPN9doris_udf15FunctionContextEPNS1_9StringValE";
        final String stringValSerializeOrFinalize = prefix
                + "32string_val_serialize_or_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE";
        final String stringValGetValue = prefix
                + "20string_val_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE";

        // Type stringType[] = {Type.CHAR, Type.VARCHAR};
        // count(*)
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
            new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
            prefix + "9init_zeroIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
            prefix + "17count_star_updateEPN9doris_udf15FunctionContextEPNS1_9BigIntValE",
            prefix + "11count_mergeEPN9doris_udf15FunctionContextERKNS1_9BigIntValEPS4_",
            null, null,
            prefix + "17count_star_removeEPN9doris_udf15FunctionContextEPNS1_9BigIntValE",
            null, false, true, true));

        for (Type t : Type.getSupportedTypes()) {
            if (t.isNull()) {
                continue; // NULL is handled through type promotion.
            }
            if (t.isScalarType(PrimitiveType.CHAR)) {
                continue; // promoted to STRING
            }
            // Count
            addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                    Lists.newArrayList(t), Type.BIGINT, Type.BIGINT,
                    prefix + "9init_zeroIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                    prefix + "12count_updateEPN9doris_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
                    prefix + "11count_mergeEPN9doris_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                    null, null,
                    prefix + "12count_removeEPN9doris_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
                    null, false, true, true));

            // count in multi distinct
            if (t.equals(Type.CHAR) || t.equals(Type.VARCHAR)) {
               addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                    Type.BIGINT,
                    Type.VARCHAR,
                    prefix + "26count_distinct_string_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    prefix + "28count_distinct_string_updateEPN9doris_udf15FunctionContextERNS1_9StringValEPS4_",
                    prefix + "27count_distinct_string_mergeEPN9doris_udf15FunctionContextERNS1_9StringValEPS4_",
                    prefix + "31count_distinct_string_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    null,
                    null,
                    prefix + "30count_distinct_string_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    false, true, true));
            } else if (t.equals(Type.TINYINT) || t.equals(Type.SMALLINT) || t.equals(Type.INT)
                    || t.equals(Type.BIGINT) || t.equals(Type.LARGEINT) || t.equals(Type.DOUBLE)) {
               addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                    Type.BIGINT,
                    Type.VARCHAR,
                    prefix + MULTI_DISTINCT_INIT_SYMBOL.get(t),
                    prefix + MULTI_DISTINCT_UPDATE_SYMBOL.get(t),
                    prefix + MULTI_DISTINCT_MERGE_SYMBOL.get(t),
                    prefix + MULTI_DISTINCT_SERIALIZE_SYMBOL.get(t),
                    null,
                    null,
                    prefix + MULTI_DISTINCT_COUNT_FINALIZE_SYMBOL.get(t),
                    false, true, true));
            } else if (t.equals(Type.DATE) || t.equals(Type.DATETIME)) {
               addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                    Type.BIGINT,
                    Type.VARCHAR,
                    prefix + "24count_distinct_date_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    prefix + "26count_distinct_date_updateEPN9doris_udf15FunctionContextERNS1_11DateTimeValEPNS1_9StringValE",
                    prefix + "25count_distinct_date_mergeEPN9doris_udf15FunctionContextERNS1_9StringValEPS4_",
                    prefix + "29count_distinct_date_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    null,
                    null,
                    prefix + "28count_distinct_date_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    false, true, true));
            } else if (t.equals(Type.DECIMAL)) {
               addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                    Type.BIGINT,
                    Type.VARCHAR,
                    prefix + "34count_or_sum_distinct_decimal_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    prefix + "36count_or_sum_distinct_decimal_updateEPN9doris_udf15FunctionContextERNS1_10DecimalValEPNS1_9StringValE",
                    prefix + "35count_or_sum_distinct_decimal_mergeEPN9doris_udf15FunctionContextERNS1_9StringValEPS4_",
                    prefix + "39count_or_sum_distinct_decimal_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    null,
                    null,
                    prefix + "31count_distinct_decimal_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    false, true, true));
            } else if (t.equals(Type.DECIMALV2)) {
               addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                    Type.BIGINT,
                    Type.VARCHAR,
                    prefix + "36count_or_sum_distinct_decimalv2_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    prefix + "38count_or_sum_distinct_decimalv2_updateEPN9doris_udf15FunctionContextERNS1_12DecimalV2ValEPNS1_9StringValE",
                    prefix + "37count_or_sum_distinct_decimalv2_mergeEPN9doris_udf15FunctionContextERNS1_9StringValEPS4_",
                    prefix + "41count_or_sum_distinct_decimalv2_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    null,
                    null,
                    prefix + "33count_distinct_decimalv2_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    false, true, true));
            }

            // sum in multi distinct
            if (t.equals(Type.BIGINT) || t.equals(Type.LARGEINT) || t.equals(Type.DOUBLE)) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                    t,
                    Type.VARCHAR,
                    prefix + MULTI_DISTINCT_INIT_SYMBOL.get(t),
                    prefix + MULTI_DISTINCT_UPDATE_SYMBOL.get(t),
                    prefix + MULTI_DISTINCT_MERGE_SYMBOL.get(t),
                    prefix + MULTI_DISTINCT_SERIALIZE_SYMBOL.get(t),
                    null,
                    null,
                    prefix + MULTI_DISTINCT_SUM_FINALIZE_SYMBOL.get(t),
                    false, true, true));
            } else if (t.equals(Type.DECIMAL)) {
               addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                    MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                    Type.VARCHAR,
                    prefix + "34count_or_sum_distinct_decimal_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    prefix + "36count_or_sum_distinct_decimal_updateEPN9doris_udf15FunctionContextERNS1_10DecimalValEPNS1_9StringValE",
                    prefix + "35count_or_sum_distinct_decimal_mergeEPN9doris_udf15FunctionContextERNS1_9StringValEPS4_",
                    prefix + "39count_or_sum_distinct_decimal_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    null,
                    null,
                    prefix + "29sum_distinct_decimal_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    false, true, true));
            } else if (t.equals(Type.DECIMALV2)) {
               addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                    MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                    Type.VARCHAR,
                    prefix + "36count_or_sum_distinct_decimalv2_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    prefix + "38count_or_sum_distinct_decimalv2_updateEPN9doris_udf15FunctionContextERNS1_12DecimalV2ValEPNS1_9StringValE",
                    prefix + "37count_or_sum_distinct_decimalv2_mergeEPN9doris_udf15FunctionContextERNS1_9StringValEPS4_",
                    prefix + "41count_or_sum_distinct_decimalv2_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    null,
                    null,
                    prefix + "31sum_distinct_decimalv2_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    false, true, true));
            }
            // Min
            String minMaxInit = t.isStringType() ? initNullString : initNull;
            String minMaxSerializeOrFinalize = t.isStringType() ? stringValSerializeOrFinalize : null;
            String minMaxGetValue = t.isStringType() ? stringValGetValue : null;
            addBuiltin(AggregateFunction.createBuiltin("min",
                    Lists.newArrayList(t), t, t, minMaxInit,
                    prefix + MIN_UPDATE_SYMBOL.get(t),
                    prefix + MIN_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false));

            // Max
            addBuiltin(AggregateFunction.createBuiltin("max",
                    Lists.newArrayList(t), t, t, minMaxInit,
                    prefix + MAX_UPDATE_SYMBOL.get(t),
                    prefix + MAX_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false));

            // NDV
            // ndv return string
            addBuiltin(AggregateFunction.createBuiltin("ndv",
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris12HllFunctions" + HLL_UPDATE_SYMBOL.get(t),
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions12hll_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            //APPROX_COUNT_DISTINCT
            //alias of ndv, compute approx count distinct use HyperLogLog
            addBuiltin(AggregateFunction.createBuiltin("approx_count_distinct",
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris12HllFunctions" + HLL_UPDATE_SYMBOL.get(t),
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions12hll_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            // BITMAP_UNION_INT
            addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_INT,
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    "_ZN5doris15BitmapFunctions11bitmap_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    BITMAP_UNION_INT_SYMBOL.get(t),
                    "_ZN5doris15BitmapFunctions12bitmap_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris15BitmapFunctions15bitmap_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            // INTERSECT_COUNT
            addBuiltin(AggregateFunction.createBuiltin(INTERSECT_COUNT,
                    Lists.newArrayList(Type.BITMAP, t, t), Type.BIGINT, Type.VARCHAR, true,
                    BITMAP_INTERSECT_INIT_SYMBOL.get(t),
                    BITMAP_INTERSECT_UPDATE_SYMBOL.get(t),
                    BITMAP_INTERSECT_MERGE_SYMBOL.get(t),
                    BITMAP_INTERSECT_SERIALIZE_SYMBOL.get(t),
                    null,
                    null,
                    BITMAP_INTERSECT_FINALIZE_SYMBOL.get(t),
                    true, false, true));

            // HLL_UNION_AGG
            addBuiltin(AggregateFunction.createBuiltin("hll_union_agg",
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                     HLL_UNION_AGG_UPDATE_SYMBOL.get(t),
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions13hll_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    null,
                    "_ZN5doris12HllFunctions12hll_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, true, true));

            // HLL_UNION
            addBuiltin(AggregateFunction.createBuiltin(HLL_UNION,
                    Lists.newArrayList(t), Type.HLL, Type.HLL,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            // HLL_RAW_AGG is alias of HLL_UNION
            addBuiltin(AggregateFunction.createBuiltin("hll_raw_agg",
                    Lists.newArrayList(t), Type.HLL, Type.HLL,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            // TopN
            if (TOPN_UPDATE_SYMBOL.containsKey(t)) {
                addBuiltin(AggregateFunction.createBuiltin("topn",
                        Lists.newArrayList(t, Type.INT), Type.VARCHAR, Type.VARCHAR,
                        "_ZN5doris13TopNFunctions9topn_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                        TOPN_UPDATE_SYMBOL.get(t),
                        "_ZN5doris13TopNFunctions10topn_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                        "_ZN5doris13TopNFunctions14topn_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        "_ZN5doris13TopNFunctions13topn_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("topn",
                        Lists.newArrayList(t, Type.INT, Type.INT), Type.VARCHAR, Type.VARCHAR,
                        "_ZN5doris13TopNFunctions9topn_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                        TOPN_UPDATE_MORE_PARAM_SYMBOL.get(t),
                        "_ZN5doris13TopNFunctions10topn_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                        "_ZN5doris13TopNFunctions14topn_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        "_ZN5doris13TopNFunctions13topn_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        true, false, true));
            }

            if (STDDEV_UPDATE_SYMBOL.containsKey(t)) {
                addBuiltin(AggregateFunction.createBuiltin("stddev",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null,
                        prefix + STDDEV_POP_FINALIZE_SYMBOL.get(t),
                        false, false, false));
                addBuiltin(AggregateFunction.createBuiltin("stddev_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null,
                        prefix + STDDEV_FINALIZE_SYMBOL.get(t),
                        false, false, false));
                addBuiltin(AggregateFunction.createBuiltin("stddev_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null,
                        prefix + STDDEV_POP_FINALIZE_SYMBOL.get(t),
                        false, false, false));
                addBuiltin(AggregateFunction.createBuiltin("variance",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null,
                        prefix + VAR_POP_FINALIZE_SYMBOL.get(t),
                        false, false, false));
                addBuiltin(AggregateFunction.createBuiltin("variance_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null,
                        prefix + VAR_FINALIZE_SYMBOL.get(t),
                        false, false, false));
                addBuiltin(AggregateFunction.createBuiltin("var_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null,
                        prefix + VAR_FINALIZE_SYMBOL.get(t),
                        false, false, false));
                addBuiltin(AggregateFunction.createBuiltin("variance_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null,
                        prefix + VAR_POP_FINALIZE_SYMBOL.get(t),
                        false, false, false));
                addBuiltin(AggregateFunction.createBuiltin("var_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null,
                        prefix + VAR_POP_FINALIZE_SYMBOL.get(t),
                        false, false, false));
            }
        }


        // Sum
        String []sumNames = {"sum", "sum_distinct"};
        for (String name : sumNames) {
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.BIGINT), Type.BIGINT, Type.BIGINT, initNull,
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, Type.DOUBLE, initNull,
                    prefix + "3sumIN9doris_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DECIMAL), Type.DECIMAL, Type.DECIMAL, initNull,
                    prefix + "3sumIN9doris_udf10DecimalValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf10DecimalValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf10DecimalValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DECIMALV2), Type.DECIMALV2, Type.DECIMALV2, initNull,
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.LARGEINT), Type.LARGEINT, Type.LARGEINT, initNull,
                    prefix + "3sumIN9doris_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
        }

        // bitmap
        addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP,
                Type.VARCHAR,
                "_ZN5doris15BitmapFunctions11bitmap_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                "_ZN5doris15BitmapFunctions12bitmap_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions12bitmap_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_COUNT, Lists.newArrayList(Type.BITMAP),
                Type.BIGINT,
                Type.VARCHAR,
                "_ZN5doris15BitmapFunctions11bitmap_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                "_ZN5doris15BitmapFunctions12bitmap_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions12bitmap_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                "_ZN5doris15BitmapFunctions16bitmap_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                null,
                "_ZN5doris15BitmapFunctions15bitmap_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                true, true, true));
        // TODO(ml): supply function symbol
        addBuiltin(AggregateFunction.createBuiltin(BITMAP_INTERSECT, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP, Type.VARCHAR,
                "_ZN5doris15BitmapFunctions20nullable_bitmap_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                "_ZN5doris15BitmapFunctions16bitmap_intersectEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions16bitmap_intersectEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        //PercentileApprox
        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "22percentile_approx_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "24percentile_approx_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKS3_PNS2_9StringValE",
                prefix + "23percentile_approx_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "27percentile_approx_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "26percentile_approx_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "22percentile_approx_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "24percentile_approx_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKS3_SA_PNS2_9StringValE",
                prefix + "23percentile_approx_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "27percentile_approx_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "26percentile_approx_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));

        // Avg
        // TODO: switch to CHAR(sizeof(AvgIntermediateType) when that becomes available
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.BIGINT), Type.DOUBLE, Type.VARCHAR,
                prefix + "8avg_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "8avg_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.DECIMAL), Type.DECIMAL, Type.VARCHAR,
                prefix + "16decimal_avg_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "18decimal_avg_updateEPN9doris_udf15FunctionContextERKNS1_10DecimalValEPNS1_9StringValE",
                prefix + "17decimal_avg_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "21decimal_avg_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "18decimal_avg_removeEPN9doris_udf15FunctionContextERKNS1_10DecimalValEPNS1_9StringValE",
                prefix + "20decimal_avg_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.DECIMALV2), Type.DECIMALV2, Type.VARCHAR,
                prefix + "18decimalv2_avg_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "20decimalv2_avg_updateEPN9doris_udf15FunctionContextERKNS1_12DecimalV2ValEPNS1_9StringValE",
                prefix + "19decimalv2_avg_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "23decimalv2_avg_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "23decimalv2_avg_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "20decimalv2_avg_removeEPN9doris_udf15FunctionContextERKNS1_12DecimalV2ValEPNS1_9StringValE",
                prefix + "22decimalv2_avg_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        // Avg(Timestamp)
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.DATE), Type.DATE, Type.VARCHAR,
                prefix + "8avg_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "20timestamp_avg_updateEPN9doris_udf15FunctionContextERKNS1_11DateTimeValEPNS1_9StringValE",
                prefix + "9avg_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "23timestamp_avg_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "20timestamp_avg_removeEPN9doris_udf15FunctionContextERKNS1_11DateTimeValEPNS1_9StringValE",
                prefix + "22timestamp_avg_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.DATETIME), Type.DATETIME, Type.DATETIME,
                prefix + "8avg_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "20timestamp_avg_updateEPN9doris_udf15FunctionContextERKNS1_11DateTimeValEPNS1_9StringValE",
                prefix + "9avg_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "23timestamp_avg_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "20timestamp_avg_removeEPN9doris_udf15FunctionContextERKNS1_11DateTimeValEPNS1_9StringValE",
                prefix + "22timestamp_avg_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        // Group_concat(string)
        addBuiltin(AggregateFunction.createBuiltin("group_concat",
                Lists.<Type>newArrayList(Type.VARCHAR), Type.VARCHAR, Type.VARCHAR, initNullString,
                prefix + "20string_concat_updateEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "19string_concat_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "22string_concat_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));
        // Group_concat(string, string)
        addBuiltin(AggregateFunction.createBuiltin("group_concat",
                Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR), Type.VARCHAR, Type.VARCHAR,
                initNullString,
                prefix + "20string_concat_updateEPN9doris_udf15FunctionContextERKNS1_9StringValES6_PS4_",
                prefix + "19string_concat_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "22string_concat_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));

        // analytic functions
        // Rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("rank",
                Lists.<Type>newArrayList(), Type.BIGINT, Type.VARCHAR,
                prefix + "9rank_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "11rank_updateEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                null,
                prefix + "14rank_get_valueEPN9doris_udf15FunctionContextERNS1_9StringValE",
                prefix + "13rank_finalizeEPN9doris_udf15FunctionContextERNS1_9StringValE"));
        // Dense rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("dense_rank",
                Lists.<Type>newArrayList(), Type.BIGINT, Type.VARCHAR,
                prefix + "9rank_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "17dense_rank_updateEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                null,
                prefix + "20dense_rank_get_valueEPN9doris_udf15FunctionContextERNS1_9StringValE",
                prefix + "13rank_finalizeEPN9doris_udf15FunctionContextERNS1_9StringValE"));
        addBuiltin(AggregateFunction.createAnalyticBuiltin( "row_number",
                new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
                prefix + "9init_zeroIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                prefix + "17count_star_updateEPN9doris_udf15FunctionContextEPNS1_9BigIntValE",
                prefix + "11count_mergeEPN9doris_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                null, null));


        for (Type t : Type.getSupportedTypes()) {
            if (t.isNull()) {
                continue; // NULL is handled through type promotion.
            }
            if (t.isScalarType(PrimitiveType.CHAR)) {
                continue; // promoted to STRING
            }
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "first_value", Lists.newArrayList(t), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + FIRST_VALUE_UPDATE_SYMBOL.get(t),
                    null,
                    t.equals(Type.VARCHAR) ? stringValGetValue : null,
                    t.equals(Type.VARCHAR) ? stringValSerializeOrFinalize : null));
            // Implements FIRST_VALUE for some windows that require rewrites during planning.
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "first_value_rewrite", Lists.newArrayList(t, Type.BIGINT), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + FIRST_VALUE_REWRITE_UPDATE_SYMBOL.get(t),
                    null,
                    t.equals(Type.VARCHAR) ? stringValGetValue : null,
                    t.equals(Type.VARCHAR) ? stringValSerializeOrFinalize : null,
                    false));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "last_value", Lists.newArrayList(t), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + LAST_VALUE_UPDATE_SYMBOL.get(t),
                    prefix + LAST_VALUE_REMOVE_SYMBOL.get(t),
                    t.equals(Type.VARCHAR) ? stringValGetValue : null,
                    t.equals(Type.VARCHAR) ? stringValSerializeOrFinalize : null));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    prefix + OFFSET_FN_INIT_SYMBOL.get(t),
                    prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
                    null, null, null));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    prefix + OFFSET_FN_INIT_SYMBOL.get(t),
                    prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
                    null, null, null));

            // lead() and lag() the default offset and the default value should be
            // rewritten to call the overrides that take all parameters.
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t), t, t));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT), t, t));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t), t, t));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT), t, t));
        }

    }

    public List<Function> getBulitinFunctions() {
        List<Function> builtinFunctions = Lists.newArrayList();
        for (Map.Entry<String, List<Function>> entry : functions.entrySet()) {
            builtinFunctions.addAll(entry.getValue());
        }
        return builtinFunctions;
    }
}
