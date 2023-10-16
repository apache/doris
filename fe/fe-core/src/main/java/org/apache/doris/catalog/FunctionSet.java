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
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.builtins.ScalarBuiltins;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FunctionSet<T> {
    private static final Logger LOG = LogManager.getLogger(FunctionSet.class);

    // All of the registered user functions. The key is the user facing name (e.g. "myUdf"),
    // and the values are all the overloaded variants (e.g. myUdf(double), myUdf(string))
    // This includes both UDFs and UDAs. Updates are made thread safe by synchronizing
    // on this map. Functions are sorted in a canonical order defined by
    // FunctionResolutionOrder.
    private final HashMap<String, List<Function>> functions;
    private final HashMap<String, List<Function>> vectorizedFunctions;
    private final HashMap<String, List<Function>> tableFunctions;
    // For most build-in functions, it will return NullLiteral when params contain NullLiteral.
    // But a few functions need to handle NullLiteral differently, such as "if". It need to add
    // an attribute to LiteralExpr to mark null and check the attribute to decide whether to
    // replace the result with NullLiteral when function finished. It leaves to be realized.
    // Functions in this set is defined in `gensrc/script/doris_builtins_functions.py`,
    // and will be built automatically.

    private ImmutableSet<String> nullResultWithOneNullParamFunctions;

    // Including now(), curdate(), etc..
    private ImmutableSet<String> nondeterministicFunctions;

    public FunctionSet() {
        functions = Maps.newHashMap();
        vectorizedFunctions = Maps.newHashMap();
        tableFunctions = Maps.newHashMap();
    }

    public void init() {
        // Populate all aggregate builtins.
        initAggregateBuiltins();

        ArithmeticExpr.initBuiltins(this);
        BinaryPredicate.initBuiltins(this);
        CompoundPredicate.initBuiltins(this);
        CastExpr.initBuiltins(this);

        IsNullPredicate.initBuiltins(this);
        ScalarBuiltins.initBuiltins(this);
        LikePredicate.initBuiltins(this);
        InPredicate.initBuiltins(this);
        AliasFunction.initBuiltins(this);

        // init table function
        initTableFunction();
    }

    public void buildNullResultWithOneNullParamFunction(Set<String> funcNames) {
        ImmutableSet.Builder<String> setBuilder = new ImmutableSet.Builder<String>();
        for (String funcName : funcNames) {
            setBuilder.add(funcName);
        }
        this.nullResultWithOneNullParamFunctions = setBuilder.build();
    }

    public void buildNondeterministicFunctions(Set<String> funcNames) {
        ImmutableSet.Builder<String> setBuilder = new ImmutableSet.Builder<String>();
        for (String funcName : funcNames) {
            setBuilder.add(funcName);
        }
        this.nondeterministicFunctions = setBuilder.build();
    }

    public boolean isNondeterministicFunction(String funcName) {
        return nondeterministicFunctions.contains(funcName);
    }

    public boolean isNullResultWithOneNullParamFunctions(String funcName) {
        return nullResultWithOneNullParamFunctions.contains(funcName);
    }

    private static final Map<Type, String> MIN_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "8min_initIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.TINYINT,
                            "8min_initIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.SMALLINT,
                            "8min_initIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.INT,
                            "8min_initIN9doris_udf6IntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.BIGINT,
                            "8min_initIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.FLOAT,
                            "8min_initIN9doris_udf8FloatValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DOUBLE,
                            "8min_initIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextEPT_")
                    // .put(Type.CHAR,
                    //     "3minIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "8min_initIN9doris_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.STRING,
                            "8min_initIN9doris_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DATE,
                            "8min_initIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DATETIME,
                            "8min_initIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "8min_initIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.LARGEINT,
                            "8min_initIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextEPT_")
                    .build();

    private static final Map<Type, String> MAX_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "8max_initIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.TINYINT,
                            "8max_initIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.SMALLINT,
                            "8max_initIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.INT,
                            "8max_initIN9doris_udf6IntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.BIGINT,
                            "8max_initIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.FLOAT,
                            "8max_initIN9doris_udf8FloatValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DOUBLE,
                            "8max_initIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextEPT_")
                    // .put(Type.CHAR,
                    //     "3minIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "8max_initIN9doris_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.STRING,
                            "8max_initIN9doris_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DATE,
                            "8max_initIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DATETIME,
                            "8max_initIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "8max_initIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.LARGEINT,
                            "8max_initIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextEPT_")
                    .build();

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
                    //          "3minIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "3minIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.STRING,
                            "3minIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATE,
                            "3minIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATETIME,
                            "3minIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.MAX_DECIMALV2_TYPE,
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
                .put(Type.STRING,
                        "3maxIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATE,
                    "3maxIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.DATETIME,
                    "3maxIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.MAX_DECIMALV2_TYPE,
                    "3maxIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                .put(Type.LARGEINT,
                    "3maxIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
               .build();

    private static final Map<Type, String> ANY_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "8any_initIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.TINYINT,
                            "8any_initIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.SMALLINT,
                            "8any_initIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.INT,
                            "8any_initIN9doris_udf6IntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.BIGINT,
                            "8any_initIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.FLOAT,
                            "8any_initIN9doris_udf8FloatValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DOUBLE,
                            "8any_initIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextEPT_")
                    // .put(Type.CHAR,
                    //     "3anyIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "8any_initIN9doris_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.STRING,
                            "8any_initIN9doris_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DATE,
                            "8any_initIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DATETIME,
                            "8any_initIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "8any_initIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.LARGEINT,
                            "8any_initIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextEPT_")
                    .build();

    private static final Map<Type, String> ANY_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "3anyIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.TINYINT,
                            "3anyIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.SMALLINT,
                            "3anyIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.INT,
                            "3anyIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.BIGINT,
                            "3anyIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.FLOAT,
                            "3anyIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DOUBLE,
                            "3anyIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                    // .put(Type.CHAR,
                    //    "3anyIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "3anyIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.STRING,
                            "3anyIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATE,
                            "3anyIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATETIME,
                            "3anyIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "3anyIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.LARGEINT,
                            "3anyIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
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
                    .put(Type.MAX_DECIMALV2_TYPE, Type.MAX_DECIMALV2_TYPE)
                    .put(Type.DECIMAL32, Type.DECIMAL32)
                    .put(Type.DECIMAL64, Type.DECIMAL64)
                    .put(Type.DECIMAL128, Type.DECIMAL128)
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
                    .put(Type.MAX_DECIMALV2_TYPE, Type.MAX_DECIMALV2_TYPE)
                    .put(Type.DECIMAL32, Type.DECIMAL32)
                    .put(Type.DECIMAL64, Type.DECIMAL64)
                    .put(Type.DECIMAL128, Type.DECIMAL128)
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
                .put(Type.MAX_DECIMALV2_TYPE,
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
                .put(Type.MAX_DECIMALV2_TYPE,
                        "16knuth_var_updateEPN9doris_udf15FunctionContextERKNS1_12DecimalV2ValEPNS1_9StringValE")
                .build();


    private static final Map<Type, String> STDDEV_REMOVE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "16knuth_var_removeIN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.SMALLINT,
                        "16knuth_var_removeIN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.INT,
                        "16knuth_var_removeIN9doris_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.BIGINT,
                        "16knuth_var_removeIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.FLOAT,
                        "16knuth_var_removeIN9doris_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.DOUBLE,
                        "16knuth_var_removeIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.MAX_DECIMALV2_TYPE,
                        "16knuth_var_removeEPN9doris_udf15FunctionContextERKNS1_12DecimalV2ValEPNS1_9StringValE")
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
                .put(Type.MAX_DECIMALV2_TYPE,
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
                .put(Type.MAX_DECIMALV2_TYPE,
                        "31decimalv2_knuth_stddev_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .build();

    private static final Map<Type, String> STDDEV_GET_VALUE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "22knuth_stddev_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.SMALLINT,
                        "22knuth_stddev_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.INT,
                        "22knuth_stddev_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.BIGINT,
                        "22knuth_stddev_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.FLOAT,
                        "22knuth_stddev_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DOUBLE,
                        "22knuth_stddev_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.MAX_DECIMALV2_TYPE,
                        "32decimalv2_knuth_stddev_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
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
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "35decimalv2_knuth_stddev_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                    .build();

    private static final Map<Type, String> STDDEV_POP_GET_VALUE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                "26knuth_stddev_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.SMALLINT,
                "26knuth_stddev_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.INT,
                "26knuth_stddev_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.BIGINT,
                "26knuth_stddev_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.FLOAT,
                "26knuth_stddev_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DOUBLE,
                "26knuth_stddev_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.MAX_DECIMALV2_TYPE,
                "36decimalv2_knuth_stddev_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
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
                .put(Type.MAX_DECIMALV2_TYPE,
                        "28decimalv2_knuth_var_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .build();

    private static final Map<Type, String> VAR_GET_VALUE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "19knuth_var_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.SMALLINT,
                        "19knuth_var_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.INT,
                        "19knuth_var_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.BIGINT,
                        "19knuth_var_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.FLOAT,
                        "19knuth_var_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DOUBLE,
                        "19knuth_var_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.MAX_DECIMALV2_TYPE,
                        "29decimalv2_knuth_var_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
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
                .put(Type.MAX_DECIMALV2_TYPE,
                        "32decimalv2_knuth_var_pop_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .build();

    private static final Map<Type, String> VAR_POP_GET_VALUE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                .put(Type.TINYINT,
                        "23knuth_var_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.SMALLINT,
                        "23knuth_var_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.INT,
                        "23knuth_var_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.BIGINT,
                        "23knuth_var_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.FLOAT,
                        "23knuth_var_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.DOUBLE,
                        "23knuth_var_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .put(Type.MAX_DECIMALV2_TYPE,
                        "33decimalv2_knuth_var_pop_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE")
                .build();

    public static final String HLL_HASH = "hll_hash";
    public static final String HLL_UNION = "hll_union";
    public static final String HLL_UNION_AGG = "hll_union_agg";
    public static final String HLL_RAW_AGG = "hll_raw_agg";
    public static final String HLL_CARDINALITY = "hll_cardinality";

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
                .put(Type.STRING,
                    "10hll_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
                .put(Type.DATE,
                    "10hll_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.DATETIME,
                    "10hll_updateIN9doris_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.MAX_DECIMALV2_TYPE,
                    "10hll_updateIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .put(Type.LARGEINT,
                    "10hll_updateIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                .build();


    private static final Map<Type, String> HLL_UNION_AGG_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                .put(Type.VARCHAR,
                        "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .put(Type.STRING,
                        "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .put(Type.HLL,
                        "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_")
                .build();

    private static final Map<Type, String> OFFSET_FN_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                     "14offset_fn_initIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.MAX_DECIMALV2_TYPE,
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
                .put(Type.STRING,
                     "14offset_fn_initIN9doris_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                .put(Type.LARGEINT,
                     "14offset_fn_initIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextEPT_")

                .build();

    private static final Map<Type, String> OFFSET_FN_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                .put(Type.BOOLEAN,
                     "16offset_fn_updateIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.MAX_DECIMALV2_TYPE,
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
                .put(Type.STRING,
                     "16offset_fn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .put(Type.LARGEINT,
                     "16offset_fn_updateIN9doris_udf11LargeIntValEEEvPNS2_15"
                     + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                .build();

    private static final Map<Type, String> LAST_VALUE_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "15last_val_updateIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.MAX_DECIMALV2_TYPE,
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
                    //         "15last_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "15last_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.STRING,
                            "15last_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.LARGEINT,
                            "15last_val_updateIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .build();

    private static final Map<Type, String> FIRST_VALUE_REWRITE_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "24first_val_rewrite_updateIN9doris_udf10BooleanValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.MAX_DECIMALV2_TYPE,
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
                            "24first_val_rewrite_updateIN9doris_udf6IntValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.FLOAT,
                            "24first_val_rewrite_updateIN9doris_udf8FloatValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.BIGINT,
                            "24first_val_rewrite_updateIN9doris_udf9BigIntValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKS3_PS6_")
                    .put(Type.DOUBLE,
                            "24first_val_rewrite_updateIN9doris_udf9DoubleValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.VARCHAR,
                            "24first_val_rewrite_updateIN9doris_udf9StringValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.STRING,
                            "24first_val_rewrite_updateIN9doris_udf9StringValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.LARGEINT,
                            "24first_val_rewrite_updateIN9doris_udf11LargeIntValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    // .put(Type.VARCHAR,
                    //         "15last_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .build();

    private static final Map<Type, String> LAST_VALUE_REMOVE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "15last_val_removeIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.MAX_DECIMALV2_TYPE,
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
                    //         "15last_val_removeIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "15last_val_removeIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.STRING,
                            "15last_val_removeIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.LARGEINT,
                            "15last_val_removeIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .build();

    private static final Map<Type, String> FIRST_VALUE_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "16first_val_updateIN9doris_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.MAX_DECIMALV2_TYPE,
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
                    //         "16first_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "16first_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.STRING,
                            "16first_val_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.LARGEINT,
                            "16first_val_updateIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .build();

    public static final String TO_BITMAP = "to_bitmap";
    public static final String TO_BITMAP_WITH_CHECK = "to_bitmap_with_check";
    public static final String BITMAP_UNION = "bitmap_union";
    public static final String BITMAP_UNION_COUNT = "bitmap_union_count";
    public static final String BITMAP_UNION_INT = "bitmap_union_int";
    public static final String BITMAP_COUNT = "bitmap_count";
    public static final String INTERSECT_COUNT = "intersect_count";
    public static final String BITMAP_INTERSECT = "bitmap_intersect";
    public static final String ORTHOGONAL_BITMAP_INTERSECT = "orthogonal_bitmap_intersect";
    public static final String ORTHOGONAL_BITMAP_INTERSECT_COUNT = "orthogonal_bitmap_intersect_count";
    public static final String ORTHOGONAL_BITMAP_UNION_COUNT = "orthogonal_bitmap_union_count";
    public static final String ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT = "orthogonal_bitmap_expr_calculate_count";
    public static final String ORTHOGONAL_BITMAP_EXPR_CALCULATE = "orthogonal_bitmap_expr_calculate";

    public static final String QUANTILE_UNION = "quantile_union";
    //TODO(weixiang): is quantile_percent can be replaced by approx_percentile?
    public static final String QUANTILE_PERCENT = "quantile_percent";
    public static final String TO_QUANTILE_STATE = "to_quantile_state";
    public static final String COLLECT_LIST = "collect_list";
    public static final String COLLECT_SET = "collect_set";
    public static final String COUNT_BY_ENUM = "count_by_enum";

    private static final Map<Type, String> ORTHOGONAL_BITMAP_INTERSECT_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions32orthogonal_bitmap_intersect_initIaN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions32orthogonal_bitmap_intersect_initIsN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions32orthogonal_bitmap_intersect_initIiN9doris_udf6IntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.BIGINT,
                            "_ZN5doris15BitmapFunctions32orthogonal_bitmap_intersect_initIlN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.FLOAT,
                            "_ZN5doris15BitmapFunctions32orthogonal_bitmap_intersect_initIfN9doris_udf8FloatValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "_ZN5doris15BitmapFunctions32orthogonal_bitmap_intersect_initIdN9doris_udf9DoubleValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions32orthogonal_bitmap_intersect_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions32orthogonal_bitmap_intersect_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .put(Type.STRING,
                            "_ZN5doris15BitmapFunctions32orthogonal_bitmap_intersect_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .build();

    private static final Map<Type, String> ORTHOGONAL_BITMAP_INTERSECT_SERIALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions37orthogonal_bitmap_intersect_serializeIaEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions37orthogonal_bitmap_intersect_serializeIsEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions37orthogonal_bitmap_intersect_serializeIiEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.BIGINT,
                            "_ZN5doris15BitmapFunctions37orthogonal_bitmap_intersect_serializeIlEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.FLOAT,
                            "_ZN5doris15BitmapFunctions37orthogonal_bitmap_intersect_serializeIfEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.DOUBLE,
                            "_ZN5doris15BitmapFunctions37orthogonal_bitmap_intersect_serializeIdEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions37orthogonal_bitmap_intersect_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions37orthogonal_bitmap_intersect_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.STRING,
                            "_ZN5doris15BitmapFunctions37orthogonal_bitmap_intersect_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .build();
    private static final Map<Type, String> ORTHOGONAL_BITMAP_INTERSECT_COUNT_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions38orthogonal_bitmap_intersect_count_initIaN9doris_udf10TinyIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions38orthogonal_bitmap_intersect_count_initIsN9doris_udf11SmallIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions38orthogonal_bitmap_intersect_count_initIiN9doris_udf6IntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.BIGINT,
                            "_ZN5doris15BitmapFunctions38orthogonal_bitmap_intersect_count_initIlN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.FLOAT,
                            "_ZN5doris15BitmapFunctions38orthogonal_bitmap_intersect_count_initIfN9doris_udf8FloatValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "_ZN5doris15BitmapFunctions38orthogonal_bitmap_intersect_count_initIdN9doris_udf9DoubleValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions38orthogonal_bitmap_intersect_count_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions38orthogonal_bitmap_intersect_count_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .put(Type.STRING,
                            "_ZN5doris15BitmapFunctions38orthogonal_bitmap_intersect_count_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .build();
    private static final Map<Type, String> ORTHOGONAL_BITMAP_INTERSECT_COUNT_SERIALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN5doris15BitmapFunctions43orthogonal_bitmap_intersect_count_serializeIaEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.SMALLINT,
                            "_ZN5doris15BitmapFunctions43orthogonal_bitmap_intersect_count_serializeIsEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.INT,
                            "_ZN5doris15BitmapFunctions43orthogonal_bitmap_intersect_count_serializeIiEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.BIGINT,
                            "_ZN5doris15BitmapFunctions43orthogonal_bitmap_intersect_count_serializeIlEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.FLOAT,
                            "_ZN5doris15BitmapFunctions43orthogonal_bitmap_intersect_count_serializeIfEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.DOUBLE,
                            "_ZN5doris15BitmapFunctions43orthogonal_bitmap_intersect_count_serializeIdEEN9doris_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions43orthogonal_bitmap_intersect_count_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions43orthogonal_bitmap_intersect_count_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.STRING,
                            "_ZN5doris15BitmapFunctions43orthogonal_bitmap_intersect_count_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .build();

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
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initINS_14DecimalV2ValueEN9doris_udf12DecimalV2ValEEEvPNS3_15FunctionContextEPNS3_9StringValE")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions21bitmap_intersect_initINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .put(Type.STRING,
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
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateINS_14DecimalV2ValueEN9doris_udf12DecimalV2ValEEEvPNS3_15FunctionContextERKNS3_9StringValERKT0_iPSB_PS8_")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextERKS4_RKT0_iPSA_PS7_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions23bitmap_intersect_updateINS_11StringValueEN9doris_udf9StringValEEEvPNS3_15FunctionContextERKS4_RKT0_iPSA_PS7_")
                    .put(Type.STRING,
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
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeINS_14DecimalV2ValueEEEvPN9doris_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeINS_11StringValueEEEvPN9doris_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions22bitmap_intersect_mergeINS_11StringValueEEEvPN9doris_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.STRING,
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
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeINS_14DecimalV2ValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions26bitmap_intersect_serializeINS_11StringValueEEEN9doris_udf9StringValEPNS3_15FunctionContextERKS4_")
                     .put(Type.STRING,
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
                    .put(Type.MAX_DECIMALV2_TYPE,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeINS_14DecimalV2ValueEEEN9doris_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.CHAR,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeINS_11StringValueEEEN9doris_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.VARCHAR,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeINS_11StringValueEEEN9doris_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.STRING,
                            "_ZN5doris15BitmapFunctions25bitmap_intersect_finalizeINS_11StringValueEEEN9doris_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .build();

    private static final Map<Type, String> TOPN_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.CHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPS3_")
                    .put(Type.VARCHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPS3_")
                    .put(Type.STRING,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPS3_")
                    .build();

    private static final Map<Type, String> TOPN_UPDATE_MORE_PARAM_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.CHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PS3_")
                    .put(Type.VARCHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PS3_")
                    .put(Type.STRING,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValESB_PS3_")
                    .build();

    public Function getFunction(Function desc, Function.CompareMode mode) {
        return getFunction(desc, mode, false);
    }

    public Function getFunction(Function desc, Function.CompareMode mode, boolean isTableFunction) {
        List<Function> fns;
        if (isTableFunction) {
            fns = tableFunctions.get(desc.functionName());
        } else if (desc.isVectorized()) {
            fns = vectorizedFunctions.get(desc.functionName());
        } else {
            fns = functions.get(desc.functionName());
        }
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
        if (!(descArgTypes[0] instanceof ScalarType)
                || !(candicateArgTypes[0] instanceof ScalarType)) {
            if (candicateArgTypes[0] instanceof ArrayType) {
                return descArgTypes[0].matchesType(candicateArgTypes[0]);
            }

            return false;
        }
        final ScalarType descArgType = (ScalarType) descArgTypes[0];
        final ScalarType candicateArgType = (ScalarType) candicateArgTypes[0];
        if (functionName.equalsIgnoreCase("hex")
                || functionName.equalsIgnoreCase("greast")
                || functionName.equalsIgnoreCase("least")
                || functionName.equalsIgnoreCase("lead")
                || functionName.equalsIgnoreCase("lag")) {
            if (!descArgType.isStringType() && candicateArgType.isStringType()) {
                // The implementations of hex for string and int are different.
                return false;
            }
        }
        // If set `roundPreciseDecimalV2Value`, only use decimalv3 as target type to execute round function
        if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().roundPreciseDecimalV2Value
                && FunctionCallExpr.ROUND_FUNCTION_SET.contains(desc.functionName())
                && descArgType.isDecimalV2() && candicateArgType.getPrimitiveType() != PrimitiveType.DECIMAL128) {
            return false;
        } else if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().roundPreciseDecimalV2Value
                && FunctionCallExpr.ROUND_FUNCTION_SET.contains(desc.functionName())
                && descArgType.isDecimalV2() && candicateArgType.getPrimitiveType() == PrimitiveType.DECIMAL128) {
            return true;
        }
        if ((descArgType.isDecimalV3() && candicateArgType.isDecimalV2())
                || (descArgType.isDecimalV2() && candicateArgType.isDecimalV3())) {
            return false;
        }
        return true;
    }

    public Function getFunction(String signatureString, boolean vectorized) {
        for (List<Function> fns : vectorized ? vectorizedFunctions.values() : functions.values()) {
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
        List<Function> fns = fn.isVectorized() ? vectorizedFunctions.get(fn.functionName()) : functions.get(fn.functionName());
        if (fns == null) {
            fns = Lists.newArrayList();
            if (fn.isVectorized()) {
                vectorizedFunctions.put(fn.functionName(), fns);
            } else {
                functions.put(fn.functionName(), fns);
            }
        }
        fns.add(fn);
        return true;
    }

    /**
     * Add a builtin with the specified name and signatures to this db.
     */
    public void addScalarBuiltin(String fnName, String symbol, boolean userVisible,
                                 String prepareFnSymbol, String closeFnSymbol,
                                 Function.NullableMode nullableMode, Type retType,
                                 boolean varArgs, Type ... args) {
        ArrayList<Type> argsType = new ArrayList<Type>();
        for (Type type : args) {
            argsType.add(type);
        }
        addBuiltin(ScalarFunction.createBuiltin(
                fnName, retType, nullableMode, argsType, varArgs,
                symbol, prepareFnSymbol, closeFnSymbol, userVisible));
    }

    public void addScalarAndVectorizedBuiltin(String fnName, String symbol, boolean userVisible,
                                              String prepareFnSymbol, String closeFnSymbol,
                                              Function.NullableMode nullableMode, Type retType,
                                              boolean varArgs, Type ... args) {
        ArrayList<Type> argsType = new ArrayList<Type>();
        for (Type type : args) {
            // only to prevent olap scan node use array expr to find a fake symbol
            // TODO: delete the code after we remove origin exec engine
            if (type.isArrayType()) {
                symbol = "_ZN5doris19array_fake_functionEPN9doris_udf15FunctionContextE";
            }
            argsType.add(type);
        }
        addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltin(
                fnName, retType, nullableMode, argsType, varArgs,
                symbol, prepareFnSymbol, closeFnSymbol, userVisible));
    }

    /**
     * Adds a builtin to this database. The function must not already exist.
     */
    public void addBuiltin(Function fn) {
        addFunction(fn, true);
    }

    /**
     * Adds a function both in FunctionSet and VecFunctionSet to this database.
     * The function must not already exist and need to be not vectorized
     */
    public void addBuiltinBothScalaAndVectorized(Function fn) {
        if (getFunction(fn, Function.CompareMode.IS_INDISTINGUISHABLE) != null) {
            return;
        }
        Preconditions.checkState(!fn.isVectorized());

        // add scala function
        List<Function> fns = functions.get(fn.functionName());
        if (fns == null) {
            fns = Lists.newArrayList();
            functions.put(fn.functionName(), fns);
        }
        fns.add(fn);

        // add vectorized function
        List<Function> vecFns = vectorizedFunctions.get(fn.functionName());
        if (vecFns == null) {
            vecFns = Lists.newArrayList();
            vectorizedFunctions.put(fn.functionName(), vecFns);
        }
        ScalarFunction scalarFunction = (ScalarFunction) fn;
        vecFns.add(ScalarFunction.createVecBuiltin(scalarFunction.functionName(), scalarFunction.getPrepareFnSymbol(),
                scalarFunction.getSymbolName(), scalarFunction.getCloseFnSymbol(),
                    Lists.newArrayList(scalarFunction.getArgs()), scalarFunction.hasVarArgs(),
                    scalarFunction.getReturnType(), scalarFunction.isUserVisible(),
                    scalarFunction.getNullableMode()));
    }


    public static final String COUNT = "count";
    public static final String WINDOW_FUNNEL = "window_funnel";

    public static final String RETENTION = "retention";

    public static final String SEQUENCE_MATCH = "sequence_match";

    public static final String SEQUENCE_COUNT = "sequence_count";

    public static final String GROUP_UNIQ_ARRAY = "group_uniq_array";

    public static final String GROUP_ARRAY = "group_array";

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
                prefix + "18init_zero_not_nullIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                prefix + "17count_star_updateEPN9doris_udf15FunctionContextEPNS1_9BigIntValE",
                prefix + "11count_mergeEPN9doris_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                null, null,
                prefix + "17count_star_removeEPN9doris_udf15FunctionContextEPNS1_9BigIntValE",
                null, false, true, true));
        // vectorized
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
                prefix + "18init_zero_not_nullIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                prefix + "17count_star_updateEPN9doris_udf15FunctionContextEPNS1_9BigIntValE",
                prefix + "11count_mergeEPN9doris_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                null, null,
                prefix + "17count_star_removeEPN9doris_udf15FunctionContextEPNS1_9BigIntValE",
                null, false, true, true, true));

        // windowFunnel
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.WINDOW_FUNNEL,
                Lists.newArrayList(Type.BIGINT, Type.STRING, Type.DATETIME, Type.BOOLEAN),
                Type.INT,
                Type.VARCHAR,
                true,
                prefix + "18window_funnel_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "20window_funnel_updateEPN9doris_udf15FunctionContextERKNS1_9BigIntValERKNS1_9StringValERKNS1_11DateTimeValEiPKNS1_10BooleanValEPS7_",
                prefix + "19window_funnel_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "23window_funnel_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                "",
                "",
                prefix + "22window_funnel_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        // Vectorization does not need symbol any more, we should clean it in the future.
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.WINDOW_FUNNEL,
                Lists.newArrayList(Type.BIGINT, Type.STRING, Type.DATETIME, Type.BOOLEAN),
                Type.INT,
                Type.VARCHAR,
                true,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.WINDOW_FUNNEL,
                Lists.newArrayList(Type.BIGINT, Type.STRING, Type.DATETIMEV2, Type.BOOLEAN),
                Type.INT,
                Type.VARCHAR,
                true,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

        // retention vectorization
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.RETENTION,
                Lists.newArrayList(Type.BOOLEAN),
                // Type.BOOLEAN will return non-numeric results so we use Type.TINYINT
                new ArrayType(Type.TINYINT),
                Type.VARCHAR,
                true,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

        // sequenceMatch
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.SEQUENCE_MATCH,
                Lists.newArrayList(Type.STRING, Type.DATEV2, Type.BOOLEAN),
                Type.BOOLEAN,
                Type.VARCHAR,
                true,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.SEQUENCE_MATCH,
                Lists.newArrayList(Type.STRING, Type.DATETIME, Type.BOOLEAN),
                Type.BOOLEAN,
                Type.VARCHAR,
                true,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.SEQUENCE_MATCH,
                Lists.newArrayList(Type.STRING, Type.DATETIMEV2, Type.BOOLEAN),
                Type.BOOLEAN,
                Type.VARCHAR,
                true,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

        // sequenceCount
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.SEQUENCE_COUNT,
                Lists.newArrayList(Type.STRING, Type.DATEV2, Type.BOOLEAN),
                Type.BIGINT,
                Type.VARCHAR,
                true,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.SEQUENCE_COUNT,
                Lists.newArrayList(Type.STRING, Type.DATETIME, Type.BOOLEAN),
                Type.BIGINT,
                Type.VARCHAR,
                true,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.SEQUENCE_COUNT,
                Lists.newArrayList(Type.STRING, Type.DATETIMEV2, Type.BOOLEAN),
                Type.BIGINT,
                Type.VARCHAR,
                true,
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

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
                    prefix + "18init_zero_not_nullIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                    prefix + "12count_updateEPN9doris_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
                    prefix + "11count_mergeEPN9doris_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                    null, null,
                    prefix + "12count_removeEPN9doris_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
                    null, false, true, true));
            // vectorized
            addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                    Lists.newArrayList(t), Type.BIGINT, Type.BIGINT,
                    prefix + "18init_zero_not_nullIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                    prefix + "12count_updateEPN9doris_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
                    prefix + "11count_mergeEPN9doris_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                    null, null,
                    prefix + "12count_removeEPN9doris_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
                    null, false, true, true, true));

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
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
            } else if (t.equals(Type.STRING)) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.STRING,
                        prefix + "26count_distinct_string_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                        prefix + "28count_distinct_string_updateEPN9doris_udf15FunctionContextERNS1_9StringValEPS4_",
                        prefix + "27count_distinct_string_mergeEPN9doris_udf15FunctionContextERNS1_9StringValEPS4_",
                        prefix + "31count_distinct_string_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        null,
                        null,
                        prefix + "30count_distinct_string_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        false, true, true));
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.STRING,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
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
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        t,
                        prefix + MULTI_DISTINCT_INIT_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_UPDATE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_MERGE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_SERIALIZE_SYMBOL.get(t),
                        null,
                        null,
                        prefix + MULTI_DISTINCT_COUNT_FINALIZE_SYMBOL.get(t),
                        false, true, true, true));
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
                // vectorized
                // now we don't support datetime distinct
            } else if (t.equals(Type.MAX_DECIMALV2_TYPE)) {
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
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.MAX_DECIMALV2_TYPE,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL32)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.DECIMAL32,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL64)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.DECIMAL64,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL128)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.DECIMAL128,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
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

                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        t,
                        t,
                        prefix + MULTI_DISTINCT_INIT_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_UPDATE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_MERGE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_SERIALIZE_SYMBOL.get(t),
                        null,
                        null,
                        prefix + MULTI_DISTINCT_SUM_FINALIZE_SYMBOL.get(t),
                        false, true, true, true));
            }  else if (t.equals(Type.MAX_DECIMALV2_TYPE)) {
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
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.MAX_DECIMALV2_TYPE,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL32)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.DECIMAL32,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL64)) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.DECIMAL64,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL128)) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.DECIMAL128,
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        prefix + "",
                        null,
                        null,
                        prefix + "",
                        false, true, true, true));
            }
            // Min
            String minMaxSerializeOrFinalize = t.isStringType() ? stringValSerializeOrFinalize : null;
            String minMaxGetValue = t.isStringType() ? stringValGetValue : null;
            addBuiltin(AggregateFunction.createBuiltin("min",
                    Lists.newArrayList(t), t, t, prefix + MIN_INIT_SYMBOL.get(t),
                    prefix + MIN_UPDATE_SYMBOL.get(t),
                    prefix + MIN_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false));
            // vectorized
            addBuiltin(AggregateFunction.createBuiltin("min",
                    Lists.newArrayList(t), t, t, prefix + MIN_INIT_SYMBOL.get(t),
                    prefix + MIN_UPDATE_SYMBOL.get(t),
                    prefix + MIN_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false, true));

            // Max
            addBuiltin(AggregateFunction.createBuiltin("max",
                    Lists.newArrayList(t), t, t, prefix + MAX_INIT_SYMBOL.get(t),
                    prefix + MAX_UPDATE_SYMBOL.get(t),
                    prefix + MAX_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false));
            // vectorized
            addBuiltin(AggregateFunction.createBuiltin("max",
                    Lists.newArrayList(t), t, t, prefix + MAX_INIT_SYMBOL.get(t),
                    prefix + MAX_UPDATE_SYMBOL.get(t),
                    prefix + MAX_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false, true));

            // Any
            addBuiltin(AggregateFunction.createBuiltin("any",
                    Lists.newArrayList(t), t, t, prefix + ANY_INIT_SYMBOL.get(t),
                    prefix + ANY_UPDATE_SYMBOL.get(t),
                    prefix + ANY_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false));
            // vectorized
            addBuiltin(AggregateFunction.createBuiltin("any", Lists.newArrayList(t), t, t, null, null, null, null, null,
                    null, null, true, false, false, true));
            // Any_Value
            addBuiltin(AggregateFunction.createBuiltin("any_value",
                    Lists.newArrayList(t), t, t, prefix + ANY_INIT_SYMBOL.get(t),
                    prefix + ANY_UPDATE_SYMBOL.get(t),
                    prefix + ANY_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false));
            // vectorized
            addBuiltin(AggregateFunction.createBuiltin("any_value", Lists.newArrayList(t), t, t, null, null, null, null,
                    null, null, null, true, false, false, true));

            // vectorized
            for (Type kt : Type.getSupportedTypes()) {
                if (kt.isNull()) {
                    continue;
                }
                addBuiltin(AggregateFunction.createBuiltin("max_by", Lists.newArrayList(t, kt), t, Type.VARCHAR,
                        "", "", "", "", "", null, "",
                        true, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("min_by", Lists.newArrayList(t, kt), t, Type.VARCHAR,
                        "", "", "", "", "", null, "",
                        true, true, false, true));
            }


            // NDV
            // ndv return string
            addBuiltin(AggregateFunction.createBuiltin("ndv", Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                            "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                            "_ZN5doris12HllFunctions" + HLL_UPDATE_SYMBOL.get(t),
                            "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                            "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                            "_ZN5doris12HllFunctions12hll_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                            true, false, true));

            // vectorized
            addBuiltin(AggregateFunction.createBuiltin("ndv", Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                            "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                            "_ZN5doris12HllFunctions" + HLL_UPDATE_SYMBOL.get(t),
                            "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                            "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                            "_ZN5doris12HllFunctions12hll_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                            true, true, true, true));

            // APPROX_COUNT_DISTINCT
            // alias of ndv, compute approx count distinct use HyperLogLog
            addBuiltin(AggregateFunction.createBuiltin("approx_count_distinct", Lists.newArrayList(t), Type.BIGINT,
                            Type.VARCHAR,
                            "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                            "_ZN5doris12HllFunctions" + HLL_UPDATE_SYMBOL.get(t),
                            "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                            "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                            "_ZN5doris12HllFunctions12hll_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                            true, false, true));

            // vectorized
            addBuiltin(AggregateFunction.createBuiltin("approx_count_distinct", Lists.newArrayList(t), Type.BIGINT,
                            Type.VARCHAR,
                            "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                            "_ZN5doris12HllFunctions" + HLL_UPDATE_SYMBOL.get(t),
                            "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                            "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                            "_ZN5doris12HllFunctions12hll_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                            true, true, true, true));

            // BITMAP_UNION_INT
            addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_INT,
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    "_ZN5doris15BitmapFunctions11bitmap_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    BITMAP_UNION_INT_SYMBOL.get(t),
                    "_ZN5doris15BitmapFunctions12bitmap_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris15BitmapFunctions15bitmap_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));
            // vectorized
            addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_INT,
                    Lists.newArrayList(t), Type.BIGINT, t,
                    "",
                    BITMAP_UNION_INT_SYMBOL.get(t),
                    "",
                    "",
                    "",
                    true, false, true, true));


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

            // VEC_INTERSECT_COUNT
            addBuiltin(
                    AggregateFunction.createBuiltin(INTERSECT_COUNT, Lists.newArrayList(Type.BITMAP, t, t), Type.BIGINT,
                            Type.VARCHAR, true, BITMAP_INTERSECT_INIT_SYMBOL.get(t),
                            BITMAP_INTERSECT_UPDATE_SYMBOL.get(t), BITMAP_INTERSECT_MERGE_SYMBOL.get(t),
                            BITMAP_INTERSECT_SERIALIZE_SYMBOL.get(t), null, null,
                            BITMAP_INTERSECT_FINALIZE_SYMBOL.get(t), true, false, true, true));

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

            // HLL_UNION_AGG vectorized
            addBuiltin(AggregateFunction.createBuiltin("hll_union_agg",
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    HLL_UNION_AGG_UPDATE_SYMBOL.get(t),
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions13hll_get_valueEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    null,
                    "_ZN5doris12HllFunctions12hll_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, true, true, true));

            // HLL_UNION
            addBuiltin(AggregateFunction.createBuiltin(HLL_UNION,
                    Lists.newArrayList(t), Type.HLL, Type.HLL,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            // HLL_UNION vectorized
            addBuiltin(AggregateFunction.createBuiltin(HLL_UNION,
                    Lists.newArrayList(t), Type.HLL, Type.HLL,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true, true));

            // HLL_RAW_AGG is alias of HLL_UNION
            addBuiltin(AggregateFunction.createBuiltin("hll_raw_agg",
                    Lists.newArrayList(t), Type.HLL, Type.HLL,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            // HLL_RAW_AGG is alias of HLL_UNION vectorized
            addBuiltin(AggregateFunction.createBuiltin("hll_raw_agg",
                    Lists.newArrayList(t), Type.HLL, Type.HLL,
                    "_ZN5doris12HllFunctions8hll_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions9hll_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN5doris12HllFunctions13hll_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true, true));

            // TopN
            if (TOPN_UPDATE_SYMBOL.containsKey(t)) {
                addBuiltin(AggregateFunction.createBuiltin("topn", Lists.newArrayList(t, Type.INT), Type.VARCHAR,
                        Type.VARCHAR,
                        "_ZN5doris13TopNFunctions9topn_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                        TOPN_UPDATE_SYMBOL.get(t),
                        "_ZN5doris13TopNFunctions10topn_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                        "_ZN5doris13TopNFunctions14topn_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        "_ZN5doris13TopNFunctions13topn_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("topn", Lists.newArrayList(t, Type.INT, Type.INT),
                        Type.VARCHAR, Type.VARCHAR,
                        "_ZN5doris13TopNFunctions9topn_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                        TOPN_UPDATE_MORE_PARAM_SYMBOL.get(t),
                        "_ZN5doris13TopNFunctions10topn_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                        "_ZN5doris13TopNFunctions14topn_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        "_ZN5doris13TopNFunctions13topn_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        true, false, true));
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("topn", Lists.newArrayList(t, Type.INT), Type.VARCHAR,
                        Type.VARCHAR,
                        "_ZN5doris13TopNFunctions9topn_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                        TOPN_UPDATE_SYMBOL.get(t),
                        "_ZN5doris13TopNFunctions10topn_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                        "_ZN5doris13TopNFunctions14topn_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        "_ZN5doris13TopNFunctions13topn_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        true, false, true, true));
                addBuiltin(AggregateFunction.createBuiltin("topn", Lists.newArrayList(t, Type.INT, Type.INT),
                        Type.VARCHAR, Type.VARCHAR,
                        "_ZN5doris13TopNFunctions9topn_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                        TOPN_UPDATE_MORE_PARAM_SYMBOL.get(t),
                        "_ZN5doris13TopNFunctions10topn_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                        "_ZN5doris13TopNFunctions14topn_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        "_ZN5doris13TopNFunctions13topn_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                        true, false, true, true));
            }

            if (STDDEV_UPDATE_SYMBOL.containsKey(t)) {
                addBuiltin(AggregateFunction.createBuiltin("stddev",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, prefix + STDDEV_POP_GET_VALUE_SYMBOL.get(t), prefix + STDDEV_REMOVE_SYMBOL.get(t),
                        prefix + STDDEV_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false));

                addBuiltin(AggregateFunction.createBuiltin("stddev_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, prefix + STDDEV_GET_VALUE_SYMBOL.get(t), prefix + STDDEV_REMOVE_SYMBOL.get(t),
                        prefix + STDDEV_FINALIZE_SYMBOL.get(t),
                        false, true, false));

                addBuiltin(AggregateFunction.createBuiltin("stddev_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, prefix + STDDEV_POP_GET_VALUE_SYMBOL.get(t), prefix + STDDEV_REMOVE_SYMBOL.get(t),
                        prefix + STDDEV_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false));

                //vec stddev stddev_samp stddev_pop
                addBuiltin(AggregateFunction.createBuiltin("stddev",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, null, null,
                        prefix + STDDEV_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("stddev_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, null, null,
                        prefix + STDDEV_FINALIZE_SYMBOL.get(t),
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("stddev_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, null, null,
                        prefix + STDDEV_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false, true));

                //vec: variance variance_samp var_samp variance_pop var_pop
                addBuiltin(AggregateFunction.createBuiltin("variance",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, null, null,
                        prefix + VAR_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("variance_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, null, null,
                        prefix + VAR_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("var_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, null, null,
                        prefix + VAR_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("variance_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, null, null,
                        prefix + VAR_FINALIZE_SYMBOL.get(t),
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("var_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, null, null,
                        prefix + VAR_FINALIZE_SYMBOL.get(t),
                        false, true, false, true));

                addBuiltin(AggregateFunction.createBuiltin("variance",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, prefix + VAR_POP_GET_VALUE_SYMBOL.get(t), prefix + STDDEV_REMOVE_SYMBOL.get(t),
                        prefix + VAR_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false));

                addBuiltin(AggregateFunction.createBuiltin("variance_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, prefix + VAR_GET_VALUE_SYMBOL.get(t), prefix + STDDEV_REMOVE_SYMBOL.get(t),
                        prefix + VAR_FINALIZE_SYMBOL.get(t),
                        false, true, false));

                addBuiltin(AggregateFunction.createBuiltin("var_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, prefix + VAR_GET_VALUE_SYMBOL.get(t), prefix + STDDEV_REMOVE_SYMBOL.get(t),
                        prefix + VAR_FINALIZE_SYMBOL.get(t),
                        false, true, false));

                addBuiltin(AggregateFunction.createBuiltin("variance_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, prefix + VAR_POP_GET_VALUE_SYMBOL.get(t), prefix + STDDEV_REMOVE_SYMBOL.get(t),
                        prefix + VAR_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false));

                addBuiltin(AggregateFunction.createBuiltin("var_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), Type.VARCHAR,
                        prefix + STDDEV_INIT_SYMBOL.get(t),
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + STDDEV_MERGE_SYMBOL.get(t),
                        null, prefix + VAR_POP_GET_VALUE_SYMBOL.get(t), prefix + STDDEV_REMOVE_SYMBOL.get(t),
                        prefix + VAR_POP_FINALIZE_SYMBOL.get(t),
                        false, true, false));

                addBuiltin(AggregateFunction.createBuiltin("avg_weighted",
                        Lists.<Type>newArrayList(t, Type.DOUBLE), Type.DOUBLE, Type.DOUBLE,
                        "", "", "", "", "", "", "",
                        false, true, false, true));
            }
        }

        // Sum
        String []sumNames = {"sum", "sum_distinct"};
        for (String name : sumNames) {
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.BIGINT), Type.BIGINT, Type.BIGINT, prefix + "14init_zero_nullIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, Type.DOUBLE, prefix + "14init_zero_nullIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextEPT_",
                    prefix + "3sumIN9doris_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.MAX_DECIMALV2_TYPE), Type.MAX_DECIMALV2_TYPE, Type.MAX_DECIMALV2_TYPE, prefix + "14init_zero_nullIN9doris_udf12DecimalV2ValEEEvPNS2_15FunctionContextEPT_",
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.LARGEINT), Type.LARGEINT, Type.LARGEINT, prefix + "14init_zero_nullIN9doris_udf11LargeIntValEEEvPNS2_15FunctionContextEPT_",
                    prefix + "3sumIN9doris_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));

            // vectorized
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.TINYINT), Type.BIGINT, Type.BIGINT, initNull,
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.SMALLINT), Type.BIGINT, Type.BIGINT, initNull,
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, initNull,
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.BIGINT), Type.BIGINT, Type.BIGINT, initNull,
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, Type.DOUBLE, initNull,
                    prefix + "3sumIN9doris_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.MAX_DECIMALV2_TYPE), Type.MAX_DECIMALV2_TYPE, Type.MAX_DECIMALV2_TYPE, initNull,
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DECIMAL32), ScalarType.DECIMAL128, Type.DECIMAL128, initNull,
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DECIMAL64), Type.DECIMAL128, Type.DECIMAL128, initNull,
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DECIMAL128), Type.DECIMAL128, Type.DECIMAL128, initNull,
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.LARGEINT), Type.LARGEINT, Type.LARGEINT, initNull,
                    prefix + "3sumIN9doris_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN9doris_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN9doris_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false, true));
        }

        Type[] types = {Type.SMALLINT, Type.TINYINT, Type.INT, Type.BIGINT, Type.FLOAT, Type.DOUBLE, Type.CHAR,
                Type.VARCHAR, Type.STRING};
        for (Type t : types) {
            addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_INTERSECT,
                    Lists.newArrayList(Type.BITMAP, t, t),
                    Type.BITMAP,
                    Type.VARCHAR,
                    true,
                    ORTHOGONAL_BITMAP_INTERSECT_INIT_SYMBOL.get(t),
                    BITMAP_INTERSECT_UPDATE_SYMBOL.get(t),
                    "_ZN5doris15BitmapFunctions12bitmap_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    ORTHOGONAL_BITMAP_INTERSECT_SERIALIZE_SYMBOL.get(t),
                    "",
                    "",
                    "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_INTERSECT_COUNT,
                    Lists.newArrayList(Type.BITMAP, t, t),
                    Type.BIGINT,
                    Type.VARCHAR,
                    true,
                    ORTHOGONAL_BITMAP_INTERSECT_COUNT_INIT_SYMBOL.get(t),
                    BITMAP_INTERSECT_UPDATE_SYMBOL.get(t),
                    "_ZN5doris15BitmapFunctions29orthogonal_bitmap_count_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    ORTHOGONAL_BITMAP_INTERSECT_COUNT_SERIALIZE_SYMBOL.get(t),
                    "",
                    "",
                    "_ZN5doris15BitmapFunctions32orthogonal_bitmap_count_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            //vec ORTHOGONAL_BITMAP_INTERSECT and ORTHOGONAL_BITMAP_INTERSECT_COUNT
            addBuiltin(
                    AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_INTERSECT, Lists.newArrayList(Type.BITMAP, t, t),
                            Type.BITMAP, Type.BITMAP, true, "", "", "", "", "", "", "", true, false, true, true));

            addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_INTERSECT_COUNT,
                    Lists.newArrayList(Type.BITMAP, t, t), Type.BIGINT, Type.BITMAP, true, "", "", "", "", "", "", "",
                    true, false, true, true));
        }

        Type[] ntypes = {Type.CHAR, Type.VARCHAR, Type.STRING};
        for (Type t : ntypes) {
            addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_EXPR_CALCULATE,
                    Lists.newArrayList(Type.BITMAP, t, Type.STRING),
                    Type.BITMAP,
                    Type.VARCHAR,
                    true,
                    "_ZN5doris15BitmapFunctions37orthogonal_bitmap_expr_calculate_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris15BitmapFunctions39orthogonal_bitmap_expr_calculate_updateEPN9doris_udf15FunctionContextERKNS1_9StringValES6_iPS5_S7_",
                    "_ZN5doris15BitmapFunctions12bitmap_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris15BitmapFunctions42orthogonal_bitmap_expr_calculate_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "",
                    "",
                    "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT,
                    Lists.newArrayList(Type.BITMAP, t, Type.STRING),
                    Type.BIGINT,
                    Type.VARCHAR,
                    true,
                    "_ZN5doris15BitmapFunctions43orthogonal_bitmap_expr_calculate_count_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN5doris15BitmapFunctions39orthogonal_bitmap_expr_calculate_updateEPN9doris_udf15FunctionContextERKNS1_9StringValES6_iPS5_S7_",
                    "_ZN5doris15BitmapFunctions29orthogonal_bitmap_count_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN5doris15BitmapFunctions48orthogonal_bitmap_expr_calculate_count_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    "",
                    "",
                    "_ZN5doris15BitmapFunctions32orthogonal_bitmap_count_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            //vec ORTHOGONAL_BITMAP_EXPR_CALCULATE and ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT
            addBuiltin(
                    AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_EXPR_CALCULATE, Lists.newArrayList(Type.BITMAP, t, Type.STRING),
                            Type.BITMAP, Type.BITMAP, true, "", "", "", "", "", "", "", true, false, true, true));

            addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT,
                    Lists.newArrayList(Type.BITMAP, t, Type.STRING), Type.BIGINT, Type.BITMAP, true, "", "", "", "", "", "", "",
                    true, false, true, true));
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

        addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP,
                Type.BITMAP,
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));


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

        addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_COUNT, Lists.newArrayList(Type.BITMAP),
                Type.BIGINT,
                Type.BITMAP,
                "",
                "",
                "",
                "",
                "",
                null,
                "",
                true, true, true, true));

        addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_UNION_COUNT, Lists.newArrayList(Type.BITMAP),
                Type.BIGINT,
                Type.VARCHAR,
                "_ZN5doris15BitmapFunctions34orthogonal_bitmap_union_count_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                "_ZN5doris15BitmapFunctions12bitmap_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions29orthogonal_bitmap_count_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions33orthogonal_bitmap_count_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                null,
                null,
                "_ZN5doris15BitmapFunctions32orthogonal_bitmap_count_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                true, true, true));
        // ORTHOGONAL_BITMAP_UNION_COUNT vectorized
        addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_UNION_COUNT, Lists.newArrayList(Type.BITMAP),
                Type.BIGINT, Type.BITMAP, "", "", "", "", null, null, "", true, true, true, true));

        // TODO(ml): supply function symbol
        addBuiltin(AggregateFunction.createBuiltin(BITMAP_INTERSECT, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP, Type.VARCHAR,
                "_ZN5doris15BitmapFunctions20nullable_bitmap_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                "_ZN5doris15BitmapFunctions16bitmap_intersectEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions16bitmap_intersectEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        addBuiltin(AggregateFunction.createBuiltin(BITMAP_INTERSECT, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP, Type.BITMAP,
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

        addBuiltin(AggregateFunction.createBuiltin("group_bitmap_xor", Lists.newArrayList(Type.BITMAP),
                Type.BITMAP, Type.VARCHAR,
                "_ZN5doris15BitmapFunctions11bitmap_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                "_ZN5doris15BitmapFunctions16group_bitmap_xorEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions16group_bitmap_xorEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                "_ZN5doris15BitmapFunctions16bitmap_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        // vec group_bitmap_xor
        addBuiltin(AggregateFunction.createBuiltin("group_bitmap_xor", Lists.newArrayList(Type.BITMAP),
                Type.BITMAP, Type.BITMAP,
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));
        //group_bit_function
        for (Type t : Type.getIntegerTypes()) {
            addBuiltin(AggregateFunction.createBuiltin("group_bit_or",
                    Lists.newArrayList(t), t, t, "", "", "", "", "",
                    false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin("group_bit_and",
                    Lists.newArrayList(t), t, t, "", "", "", "", "",
                    false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin("group_bit_xor",
                    Lists.newArrayList(t), t, t, "", "", "", "", "",
                    false, true, false, true));
        }
        //quantile_state
        addBuiltin(AggregateFunction.createBuiltin(QUANTILE_UNION, Lists.newArrayList(Type.QUANTILE_STATE),
                Type.QUANTILE_STATE,
                Type.QUANTILE_STATE,
                "_ZN5doris22QuantileStateFunctions19quantile_state_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                "_ZN5doris22QuantileStateFunctions14quantile_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris22QuantileStateFunctions14quantile_unionEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN5doris22QuantileStateFunctions24quantile_state_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                "_ZN5doris22QuantileStateFunctions24quantile_state_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        addBuiltin(AggregateFunction.createBuiltin(QUANTILE_UNION, Lists.newArrayList(Type.QUANTILE_STATE),
                Type.QUANTILE_STATE,
                Type.QUANTILE_STATE,
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));
        //Percentile
        addBuiltin(AggregateFunction.createBuiltin("percentile",
                Lists.newArrayList(Type.BIGINT, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "15percentile_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "17percentile_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE",
                prefix + "16percentile_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "20percentile_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "19percentile_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));

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

        //vec percentile and percentile_approx
        addBuiltin(AggregateFunction.createBuiltin("percentile",
                Lists.newArrayList(Type.BIGINT, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "15percentile_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "17percentile_updateIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE",
                prefix + "16percentile_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "20percentile_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "19percentile_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, true, false, true));

        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "22percentile_approx_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "24percentile_approx_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKS3_PNS2_9StringValE",
                prefix + "23percentile_approx_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "27percentile_approx_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "26percentile_approx_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, true, false, true));

        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "22percentile_approx_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "24percentile_approx_updateIN9doris_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKS3_SA_PNS2_9StringValE",
                prefix + "23percentile_approx_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "27percentile_approx_serializeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                prefix + "26percentile_approx_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE",
                false, true, false, true));

        addBuiltin(AggregateFunction.createBuiltin("percentile_array",
                Lists.newArrayList(Type.BIGINT, new ArrayType(Type.DOUBLE)), new ArrayType(Type.DOUBLE), Type.VARCHAR,
                "", "", "", "", "",
                false, true, false, true));

        // collect_list
        for (Type t : Type.getArraySubTypes()) {
            addBuiltin(AggregateFunction.createBuiltin(COLLECT_LIST, Lists.newArrayList(t), new ArrayType(t), t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(AggregateFunction.createBuiltin(COLLECT_SET, Lists.newArrayList(t), new ArrayType(t), t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(AggregateFunction.createBuiltin(COLLECT_LIST, Lists.newArrayList(t, Type.INT), new ArrayType(t), t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(AggregateFunction.createBuiltin(COLLECT_SET, Lists.newArrayList(t, Type.INT), new ArrayType(t), t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(
                    AggregateFunction.createBuiltin("topn_array", Lists.newArrayList(t, Type.INT), new ArrayType(t), t,
                            "", "", "", "", "", true, false, true, true));
            addBuiltin(
                    AggregateFunction
                            .createBuiltin("topn_array", Lists.newArrayList(t, Type.INT, Type.INT), new ArrayType(t), t,
                                    "", "", "", "", "", true, false, true, true));
            addBuiltin(
                    AggregateFunction
                            .createBuiltin("topn_weighted", Lists.newArrayList(t, Type.BIGINT, Type.INT),
                                    new ArrayType(t),
                                    t,
                                    "", "", "", "", "", true, false, true, true));
            addBuiltin(
                    AggregateFunction
                            .createBuiltin("topn_weighted", Lists.newArrayList(t, Type.BIGINT, Type.INT, Type.INT),
                                    new ArrayType(t), t,
                                    "", "", "", "", "", true, false, true, true));
            addBuiltin(AggregateFunction.createBuiltin(GROUP_UNIQ_ARRAY, Lists.newArrayList(t), new ArrayType(t), t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(
                    AggregateFunction.createBuiltin(GROUP_UNIQ_ARRAY, Lists.newArrayList(t, Type.INT), new ArrayType(t),
                            t, "", "", "", "", "", true, false, true, true));
            addBuiltin(AggregateFunction.createBuiltin(GROUP_ARRAY, Lists.newArrayList(t), new ArrayType(t), t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(
                    AggregateFunction.createBuiltin(GROUP_ARRAY, Lists.newArrayList(t, Type.INT), new ArrayType(t),
                            t, "", "", "", "", "", true, false, true, true));
        }

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
                Lists.<Type>newArrayList(Type.MAX_DECIMALV2_TYPE), Type.MAX_DECIMALV2_TYPE, Type.VARCHAR,
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

        // vectorized avg
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.TINYINT), Type.DOUBLE, Type.TINYINT,
                "", "", "", "", "", "", "",
                false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.SMALLINT), Type.DOUBLE, Type.SMALLINT,
                "", "", "", "", "", "", "",
                false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.INT), Type.DOUBLE, Type.INT,
                "", "", "", "", "", "", "",
                false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.BIGINT), Type.DOUBLE, Type.BIGINT,
                "", "", "", "", "", "", "",
                false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, Type.DOUBLE,
                "", "", "", "", "", "", "",
                false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.MAX_DECIMALV2_TYPE), Type.MAX_DECIMALV2_TYPE, Type.MAX_DECIMALV2_TYPE,
                "", "", "", "", "", "", "",
                false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.DECIMAL32), Type.DECIMAL128, Type.DECIMAL128,
                "", "", "", "", "", "", "",
                false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.DECIMAL64), Type.DECIMAL128, Type.DECIMAL128,
                "", "", "", "", "", "", "",
                false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.DECIMAL128), Type.DECIMAL128, Type.DECIMAL128,
                "", "", "", "", "", "", "",
                false, true, false, true));

        // Group_concat(string)
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.VARCHAR), Type.VARCHAR,
                        Type.VARCHAR, initNullString,
                        prefix + "20string_concat_updateEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                        prefix + "19string_concat_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                        stringValSerializeOrFinalize,
                        prefix + "22string_concat_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE", false,
                        false, false));
        // Group_concat(string, string)
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                        Type.VARCHAR, Type.VARCHAR, initNullString,
                        prefix + "20string_concat_updateEPN9doris_udf15FunctionContextERKNS1_9StringValES6_PS4_",
                        prefix + "19string_concat_mergeEPN9doris_udf15FunctionContextERKNS1_9StringValEPS4_",
                        stringValSerializeOrFinalize,
                        prefix + "22string_concat_finalizeEPN9doris_udf15FunctionContextERKNS1_9StringValE", false,
                        false, false));
        // Group_concat(string) vectorized
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.VARCHAR), Type.VARCHAR,
                Type.VARCHAR, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.VARCHAR), Type.VARCHAR,
                Type.VARCHAR, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.CHAR), Type.CHAR,
                Type.CHAR, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.CHAR), Type.CHAR,
                Type.CHAR, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.STRING), Type.STRING,
                Type.STRING, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.STRING), Type.STRING,
                Type.STRING, initNullString, "", "", "", "", false, true, false, true));
        // Group_concat(string, string) vectorized
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.VARCHAR, Type.VARCHAR, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.VARCHAR, Type.VARCHAR, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.CHAR, Type.CHAR),
                Type.CHAR, Type.CHAR, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.CHAR, Type.CHAR),
                Type.CHAR, Type.CHAR, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.STRING, Type.STRING),
                Type.STRING, Type.STRING, initNullString, "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.STRING, Type.STRING),
                Type.STRING, Type.STRING, initNullString, "", "", "", "", false, true, false, true));
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
        //row_number
        addBuiltin(AggregateFunction.createAnalyticBuiltin("row_number",
                new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
                prefix + "18init_zero_not_nullIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                prefix + "17count_star_updateEPN9doris_udf15FunctionContextEPNS1_9BigIntValE",
                prefix + "11count_mergeEPN9doris_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                null, null));
        //ntile, we use rewrite sql for ntile, actually we don't really need this.
        addBuiltin(AggregateFunction.createAnalyticBuiltin("ntile",
                Collections.singletonList(Type.BIGINT), Type.BIGINT, Type.BIGINT, null, null, null, null, null));

        //vec Rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("rank",
                Lists.<Type>newArrayList(), Type.BIGINT, Type.VARCHAR,
                prefix + "9rank_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "11rank_updateEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                null,
                prefix + "14rank_get_valueEPN9doris_udf15FunctionContextERNS1_9StringValE",
                prefix + "13rank_finalizeEPN9doris_udf15FunctionContextERNS1_9StringValE", true));
        //vec Dense rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("dense_rank",
                Lists.<Type>newArrayList(), Type.BIGINT, Type.VARCHAR,
                prefix + "9rank_initEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                prefix + "17dense_rank_updateEPN9doris_udf15FunctionContextEPNS1_9StringValE",
                null,
                prefix + "20dense_rank_get_valueEPN9doris_udf15FunctionContextERNS1_9StringValE",
                prefix + "13rank_finalizeEPN9doris_udf15FunctionContextERNS1_9StringValE", true));
        //vec row_number
        addBuiltin(AggregateFunction.createAnalyticBuiltin("row_number",
                new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
                prefix + "18init_zero_not_nullIN9doris_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                prefix + "17count_star_updateEPN9doris_udf15FunctionContextEPNS1_9BigIntValE",
                prefix + "11count_mergeEPN9doris_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                null, null, true));
        //vec ntile
        addBuiltin(AggregateFunction.createAnalyticBuiltin("ntile",
                Collections.singletonList(Type.BIGINT), Type.BIGINT, Type.BIGINT, null, null, null, null, null, true));

        for (Type t : Type.getTrivialTypes()) {
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
                    t.isStringType()  ? stringValGetValue : null,
                    t.isStringType()  ? stringValSerializeOrFinalize : null));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "last_value", Lists.newArrayList(t), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + LAST_VALUE_UPDATE_SYMBOL.get(t),
                    prefix + LAST_VALUE_REMOVE_SYMBOL.get(t),
                    t.isStringType() ? stringValGetValue : null,
                    t.isStringType() ? stringValSerializeOrFinalize : null));

            //vec first_value
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "first_value", Lists.newArrayList(t), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + FIRST_VALUE_UPDATE_SYMBOL.get(t),
                    null,
                    t.isStringType()  ? stringValGetValue : null,
                    t.isStringType()  ? stringValSerializeOrFinalize : null, true));
            // Implements FIRST_VALUE for some windows that require rewrites during planning.
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "first_value_rewrite", Lists.newArrayList(t, Type.BIGINT), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + FIRST_VALUE_REWRITE_UPDATE_SYMBOL.get(t),
                    null,
                    t.isStringType() ? stringValGetValue : null,
                    t.isStringType() ? stringValSerializeOrFinalize : null,
                    false, false));
            //vec last_value
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "last_value", Lists.newArrayList(t), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + LAST_VALUE_UPDATE_SYMBOL.get(t),
                    prefix + LAST_VALUE_REMOVE_SYMBOL.get(t),
                    t.isStringType() ? stringValGetValue : null,
                    t.isStringType() ? stringValSerializeOrFinalize : null, true));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    prefix + OFFSET_FN_INIT_SYMBOL.get(t),
                    prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
                    null, t.isStringType() ? stringValGetValue : null, null));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    prefix + OFFSET_FN_INIT_SYMBOL.get(t),
                    prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
                    null, t.isStringType() ? stringValGetValue : null, null));
            //vec
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    prefix + OFFSET_FN_INIT_SYMBOL.get(t),
                    prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
                    null, null, null, true));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    prefix + OFFSET_FN_INIT_SYMBOL.get(t),
                    prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
                    null, null, null, true));

            // lead() and lag() the default offset and the default value should be
            // rewritten to call the overrides that take all parameters.
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t), t, t, false));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT), t, t, false));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t), t, t, false));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT), t, t, false));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                        "lag", Lists.newArrayList(t), t, t, true));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                        "lag", Lists.newArrayList(t, Type.BIGINT), t, t, true));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                        "lead", Lists.newArrayList(t), t, t, true));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                        "lead", Lists.newArrayList(t, Type.BIGINT), t, t, true));
        }

        // count_by_enum
        addBuiltin(AggregateFunction.createBuiltin(COUNT_BY_ENUM,
            Lists.newArrayList(Type.STRING),
            Type.STRING,
            Type.STRING,
            true,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            false, true, false, true));

    }

    public Map<String, List<Function>> getVectorizedFunctions() {
        return ImmutableMap.copyOf(vectorizedFunctions);
    }

    public List<Function> getBulitinFunctions() {
        List<Function> builtinFunctions = Lists.newArrayList();
        for (Map.Entry<String, List<Function>> entry : functions.entrySet()) {
            builtinFunctions.addAll(entry.getValue());
        }
        return builtinFunctions;
    }

    public static final String EXPLODE_SPLIT = "explode_split";
    public static final String EXPLODE_BITMAP = "explode_bitmap";
    public static final String EXPLODE_JSON_ARRAY_INT = "explode_json_array_int";
    public static final String EXPLODE_JSON_ARRAY_DOUBLE = "explode_json_array_double";
    public static final String EXPLODE_JSON_ARRAY_STRING = "explode_json_array_string";
    public static final String EXPLODE_NUMBERS = "explode_numbers";
    public static final String EXPLODE = "explode";

    private void addTableFunction(String name, Type retType, NullableMode nullableMode, ArrayList<Type> argTypes,
            boolean hasVarArgs, String symbol) {
        List<Function> functionList = tableFunctions.get(name);
        functionList.add(ScalarFunction.createBuiltin(name, retType, nullableMode, argTypes, hasVarArgs, symbol, null,
                null, true));
    }

    private void addTableFunctionWithCombinator(String name, Type retType, NullableMode nullableMode,
            ArrayList<Type> argTypes, boolean hasVarArgs, String symbol) {
        addTableFunction(name, retType, nullableMode, argTypes, hasVarArgs, symbol);
        addTableFunction(name + "_outer", retType, Function.NullableMode.ALWAYS_NULLABLE, argTypes, hasVarArgs, symbol);
    }

    private void initTableFunctionListWithCombinator(String name) {
        tableFunctions.put(name, Lists.newArrayList());
        tableFunctions.put(name + "_outer", Lists.newArrayList());
    }

    private void initTableFunction() {
        initTableFunctionListWithCombinator(EXPLODE_SPLIT);
        addTableFunctionWithCombinator(EXPLODE_SPLIT, Type.VARCHAR, Function.NullableMode.DEPEND_ON_ARGUMENT,
                Lists.newArrayList(Type.VARCHAR, Type.VARCHAR), false,
                "_ZN5doris19DummyTableFunctions13explode_splitEPN9doris_udf15FunctionContextERKNS1_9StringValES6_");

        initTableFunctionListWithCombinator(EXPLODE_BITMAP);
        addTableFunctionWithCombinator(EXPLODE_BITMAP, Type.BIGINT, Function.NullableMode.DEPEND_ON_ARGUMENT,
                Lists.newArrayList(Type.BITMAP), false,
                "_ZN5doris19DummyTableFunctions14explode_bitmapEPN9doris_udf15FunctionContextERKNS1_9StringValE");

        initTableFunctionListWithCombinator(EXPLODE_JSON_ARRAY_INT);
        addTableFunctionWithCombinator(EXPLODE_JSON_ARRAY_INT, Type.BIGINT, Function.NullableMode.DEPEND_ON_ARGUMENT,
                Lists.newArrayList(Type.VARCHAR), false,
                "_ZN5doris19DummyTableFunctions22explode_json_array_intEPN9doris_udf15FunctionContextERKNS1_9StringValE");

        initTableFunctionListWithCombinator(EXPLODE_JSON_ARRAY_DOUBLE);
        addTableFunctionWithCombinator(EXPLODE_JSON_ARRAY_DOUBLE, Type.DOUBLE, Function.NullableMode.DEPEND_ON_ARGUMENT,
                Lists.newArrayList(Type.VARCHAR), false,
                "_ZN5doris19DummyTableFunctions25explode_json_array_doubleEPN9doris_udf15FunctionContextERKNS1_9StringValE");

        initTableFunctionListWithCombinator(EXPLODE_JSON_ARRAY_STRING);
        addTableFunctionWithCombinator(EXPLODE_JSON_ARRAY_STRING, Type.VARCHAR,
                Function.NullableMode.DEPEND_ON_ARGUMENT, Lists.newArrayList(Type.VARCHAR), false,
                "_ZN5doris19DummyTableFunctions25explode_json_array_stringEPN9doris_udf15FunctionContextERKNS1_9StringValE");

        initTableFunctionListWithCombinator(EXPLODE_NUMBERS);
        addTableFunctionWithCombinator(EXPLODE_NUMBERS, Type.INT, Function.NullableMode.DEPEND_ON_ARGUMENT,
                Lists.newArrayList(Type.INT), false,
                "_ZN5doris19DummyTableFunctions22explode_numbersEPN9doris_udf15FunctionContextERKNS1_9IntValE");

        initTableFunctionListWithCombinator(EXPLODE);
        for (Type subType : Type.getArraySubTypes()) {
            addTableFunctionWithCombinator(EXPLODE, subType, Function.NullableMode.ALWAYS_NULLABLE,
                    Lists.newArrayList(new ArrayType(subType)), false,
                    "_ZN5doris19DummyTableFunctions7explodeEPN9doris_udf15FunctionContextERKNS1_13CollectionValE");
        }
    }
}
