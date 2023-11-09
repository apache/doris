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
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.builtins.ScalarBuiltins;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

    private HashSet<String> aggFunctionNames = new HashSet<>();

    private boolean inited = false;

    public FunctionSet() {
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
        MatchPredicate.initBuiltins(this);
        InPredicate.initBuiltins(this);
        AliasFunction.initBuiltins(this);

        // init table function
        initTableFunction();

        inited = true;
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

    public static final String HLL_HASH = "hll_hash";
    public static final String HLL_UNION = "hll_union";
    public static final String HLL_UNION_AGG = "hll_union_agg";
    public static final String HLL_RAW_AGG = "hll_raw_agg";
    public static final String HLL_CARDINALITY = "hll_cardinality";

    public static final String TO_BITMAP = "to_bitmap";
    public static final String TO_BITMAP_WITH_CHECK = "to_bitmap_with_check";
    public static final String BITMAP_HASH = "bitmap_hash";
    public static final String BITMAP_UNION = "bitmap_union";
    public static final String BITMAP_UNION_COUNT = "bitmap_union_count";
    public static final String BITMAP_UNION_INT = "bitmap_union_int";
    public static final String BITMAP_COUNT = "bitmap_count";
    public static final String INTERSECT_COUNT = "intersect_count";
    public static final String BITMAP_INTERSECT = "bitmap_intersect";
    public static final String ORTHOGONAL_BITMAP_INTERSECT = "orthogonal_bitmap_intersect";
    public static final String ORTHOGONAL_BITMAP_INTERSECT_COUNT = "orthogonal_bitmap_intersect_count";
    public static final String ORTHOGONAL_BITMAP_UNION_COUNT = "orthogonal_bitmap_union_count";
    public static final String APPROX_COUNT_DISTINCT = "approx_count_distinct";
    public static final String NDV = "ndv";
    public static final String ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT = "orthogonal_bitmap_expr_calculate_count";
    public static final String ORTHOGONAL_BITMAP_EXPR_CALCULATE = "orthogonal_bitmap_expr_calculate";

    public static final String QUANTILE_UNION = "quantile_union";
    //TODO(weixiang): is quantile_percent can be replaced by approx_percentile?
    public static final String QUANTILE_PERCENT = "quantile_percent";
    public static final String TO_QUANTILE_STATE = "to_quantile_state";
    public static final String COLLECT_LIST = "collect_list";
    public static final String COLLECT_SET = "collect_set";
    public static final String HISTOGRAM = "histogram";
    public static final String HIST = "hist";
    public static final String MAP_AGG = "map_agg";

    public static final String BITMAP_AGG = "bitmap_agg";
    public static final String COUNT_BY_ENUM = "count_by_enum";

    private static final Map<Type, String> TOPN_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.CHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPS3_")
                    .put(Type.VARCHAR,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPS3_")
                    .put(Type.STRING,
                            "_ZN5doris13TopNFunctions11topn_updateIN9doris_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPS3_")
                    .build();

    public Function getFunction(Function desc, Function.CompareMode mode) {
        return getFunction(desc, mode, false);
    }

    public Function getFunction(Function desc, Function.CompareMode mode, boolean isTableFunction) {
        List<Function> fns;
        if (isTableFunction) {
            fns = tableFunctions.get(desc.functionName());
        } else {
            fns = vectorizedFunctions.get(desc.functionName());
        }
        if (fns == null) {
            return null;
        }

        List<Function> normalFunctions = Lists.newArrayList();
        List<Function> templateFunctions = Lists.newArrayList();
        List<Function> variadicTemplateFunctions = Lists.newArrayList();
        List<Function> inferenceFunctions = Lists.newArrayList();
        for (Function fn : fns) {
            if (fn.isInferenceFunction()) {
                inferenceFunctions.add(fn);
                continue;
            }
            if (fn.hasTemplateArg()) {
                if (!fn.hasVariadicTemplateArg()) {
                    templateFunctions.add(fn);
                } else {
                    variadicTemplateFunctions.add(fn);
                }
            } else {
                normalFunctions.add(fn);
            }
        }

        // try normal functions first
        Function fn = getFunction(desc, mode, normalFunctions);
        if (fn != null) {
            return fn;
        }

        // then specialize template functions and try them
        List<Function> specializedTemplateFunctions = Lists.newArrayList();
        for (Function f : templateFunctions) {
            f = specializeTemplateFunction(f, desc, false);
            if (f != null) {
                specializedTemplateFunctions.add(f);
            }
        }

        // try template function second
        fn = getFunction(desc, mode, specializedTemplateFunctions);
        if (fn != null) {
            return fn;
        }

        // then specialize variadic template function and try them
        List<Function> specializedVariadicTemplateFunctions = Lists.newArrayList();
        for (Function f : variadicTemplateFunctions) {
            f = specializeTemplateFunction(f, desc, true);
            if (f != null) {
                specializedVariadicTemplateFunctions.add(f);
            }
        }

        // try variadic template function third
        fn = getFunction(desc, mode, specializedVariadicTemplateFunctions);
        if (fn != null) {
            return fn;
        }

        List<Function> inferredFunctions = Lists.newArrayList();
        for (Function f : inferenceFunctions) {
            if (f.hasTemplateArg()) {
                f = specializeTemplateFunction(f, desc, f.hasVariadicTemplateArg());
            }
            if (f != null && (f = resolveInferenceFunction(f, desc)) != null) {
                inferredFunctions.add(f);
            }
        }

        // try inference function at last
        return getFunction(desc, mode, inferredFunctions);
    }

    private Function getFunction(Function desc, Function.CompareMode mode, List<Function> fns) {
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

    public Function specializeTemplateFunction(Function templateFunction, Function requestFunction, boolean isVariadic) {
        try {
            boolean hasTemplateType = false;
            if (LOG.isDebugEnabled()) {
                LOG.debug("templateFunction signature: {}, return type: {}",
                            templateFunction.signatureString(), templateFunction.getReturnType());
                LOG.debug("requestFunction signature: {}, return type: {}",
                            requestFunction.signatureString(), requestFunction.getReturnType());
            }
            List<Type> newArgTypes = Lists.newArrayList();
            List<Type> newRetType = Lists.newArrayList();
            if (isVariadic) {
                Map<String, Integer> expandSizeMap = Maps.newHashMap();
                templateFunction.collectTemplateExpandSize(requestFunction.getArgs(), expandSizeMap);
                // expand the variadic template in arg types
                for (Type argType : templateFunction.getArgs()) {
                    if (argType.needExpandTemplateType()) {
                        newArgTypes.addAll(argType.expandVariadicTemplateType(expandSizeMap));
                    } else {
                        newArgTypes.add(argType);
                    }
                }

                // expand the variadic template in ret type
                if (templateFunction.getReturnType().needExpandTemplateType()) {
                    newRetType.addAll(templateFunction.getReturnType().expandVariadicTemplateType(expandSizeMap));
                    Preconditions.checkState(newRetType.size() == 1);
                } else {
                    newRetType.add(templateFunction.getReturnType());
                }
            } else {
                newArgTypes.addAll(Lists.newArrayList(templateFunction.getArgs()));
                newRetType.add(templateFunction.getReturnType());
            }
            Function specializedFunction = templateFunction;
            if (templateFunction instanceof ScalarFunction) {
                ScalarFunction f = (ScalarFunction) templateFunction;
                specializedFunction = new ScalarFunction(f.getFunctionName(), newArgTypes, newRetType.get(0), f.hasVarArgs(),
                        f.getSymbolName(), f.getBinaryType(), f.isUserVisible(), true, f.getNullableMode());
            } else {
                throw new TypeException(templateFunction
                                + " is not support for template since it's not a ScalarFunction");
            }
            Type[] args = specializedFunction.getArgs();
            Map<String, Type> specializedTypeMap = Maps.newHashMap();
            boolean enableDecimal256 = SessionVariable.getEnableDecimal256();
            for (int i = 0; i < args.length; i++) {
                if (args[i].hasTemplateType()) {
                    hasTemplateType = true;
                    args[i] = args[i].specializeTemplateType(requestFunction.getArgs()[i], specializedTypeMap, false,
                            enableDecimal256);
                }
            }
            if (specializedFunction.getReturnType().hasTemplateType()) {
                hasTemplateType = true;
                specializedFunction.setReturnType(
                        specializedFunction.getReturnType().specializeTemplateType(
                        requestFunction.getReturnType(), specializedTypeMap, true, enableDecimal256));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("specializedFunction signature: {}, return type: {}",
                            specializedFunction.signatureString(), specializedFunction.getReturnType());
            }
            return hasTemplateType ? specializedFunction : templateFunction;
        } catch (TypeException e) {
            if (inited && LOG.isDebugEnabled()) {
                LOG.debug("specializeTemplateFunction exception", e);
            }
            return null;
        }
    }

    public Function resolveInferenceFunction(Function inferenceFunction, Function requestFunction) {
        Type[] inputArgs = requestFunction.getArgs();
        Type[] inferArgs = inferenceFunction.getArgs();
        Type[] newTypes = Arrays.copyOf(inputArgs, inputArgs.length);
        for (int i = 0; i < inferArgs.length; ++i) {
            Type inferType = inferArgs[i];
            Type inputType = inputArgs[i];
            if (!inferType.isAnyType()) {
                newTypes[i] = inputType;
            }
        }
        Type newRetType = FunctionTypeDeducers.deduce(inferenceFunction.functionName(), newTypes);
        if (newRetType != null && inferenceFunction instanceof ScalarFunction) {
            ScalarFunction f = (ScalarFunction) inferenceFunction;
            return new ScalarFunction(f.getFunctionName(), Lists.newArrayList(newTypes), newRetType, f.hasVarArgs(),
                    f.getSymbolName(), f.getBinaryType(), f.isUserVisible(), true, f.getNullableMode());
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
            if (candicateArgTypes[0] instanceof ArrayType || candicateArgTypes[0] instanceof MapType) {
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
                && descArgType.isDecimalV2()) {
            return candicateArgType.getPrimitiveType() == PrimitiveType.DECIMAL128;
        }
        if ((descArgType.isDecimalV3() && candicateArgType.isDecimalV2())
                || (descArgType.isDecimalV2() && candicateArgType.isDecimalV3())) {
            return false;
        }
        return true;
    }

    public Function getFunction(String signatureString, boolean vectorized) {
        for (List<Function> fns : vectorizedFunctions.values()) {
            for (Function f : fns) {
                if (f.signatureString().equals(signatureString)) {
                    return f;
                }
            }
        }
        return null;
    }

    private boolean addFunction(Function fn, boolean isBuiltin) {
        if (fn instanceof AggregateFunction) {
            aggFunctionNames.add(fn.functionName());
        }

        // TODO: add this to persistent store
        if (getFunction(fn, Function.CompareMode.IS_INDISTINGUISHABLE) != null) {
            return false;
        }
        List<Function> fns = vectorizedFunctions.get(fn.functionName());
        if (fns == null) {
            fns = Lists.newArrayList();
            vectorizedFunctions.put(fn.functionName(), fns);
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

    public void addScalarAndVectorizedBuiltin(String fnName, boolean userVisible,
            Function.NullableMode nullableMode, Type retType,
            boolean varArgs, Type ... args) {
        ArrayList<Type> argsType = new ArrayList<Type>();
        for (Type type : args) {
            argsType.add(type);
        }
        addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltin(
                fnName, retType, nullableMode, argsType, varArgs,
                "", "", "", userVisible));
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

        // add vectorized function
        List<Function> vecFns = vectorizedFunctions.get(fn.functionName());
        if (vecFns == null) {
            vecFns = Lists.newArrayList();
            vectorizedFunctions.put(fn.functionName(), vecFns);
        }
        vecFns.add(fn);
    }


    public static final String COUNT = "count";
    public static final String WINDOW_FUNNEL = "window_funnel";

    public static final String RETENTION = "retention";

    public static final String SEQUENCE_MATCH = "sequence_match";

    public static final String SEQUENCE_COUNT = "sequence_count";

    public static final String GROUP_UNIQ_ARRAY = "group_uniq_array";

    public static final String GROUP_ARRAY = "group_array";

    public static final String ARRAY_AGG = "array_agg";

    // Populate all the aggregate builtins in the catalog.
    // null symbols indicate the function does not need that step of the evaluation.
    // An empty symbol indicates a TODO for the BE to implement the function.
    private void initAggregateBuiltins() {

        // Type stringType[] = {Type.CHAR, Type.VARCHAR};
        // count(*)
        // vectorized
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
                "",
                "",
                "",
                null, null,
                "",
                null, false, true, true, true));

        // count(array/map/struct)
        for (Type complexType : Lists.newArrayList(Type.ARRAY, Type.MAP, Type.GENERIC_STRUCT)) {
            addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                    Lists.newArrayList(complexType), Type.BIGINT, Type.BIGINT,
                    "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, true, true));

            addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                    Lists.newArrayList(complexType), Type.BIGINT, Type.BIGINT,
                    "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, true, true));
        }

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
                new ArrayType(Type.BOOLEAN),
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

        addBuiltin(AggregateFunction.createBuiltin("hll_union_agg",
                        Lists.newArrayList(Type.HLL), Type.BIGINT, Type.VARCHAR,
                        "",
                        "",
                        "",
                        "",
                        "",
                        null,
                        "",
                        true, true, true, true));
        addBuiltin(AggregateFunction.createBuiltin(HLL_UNION,
                        Lists.newArrayList(Type.HLL), Type.HLL, Type.HLL,
                        "",
                        "",
                        "",
                        "",
                        "",
                        true, false, true, true));
        addBuiltin(AggregateFunction.createBuiltin("hll_raw_agg",
                        Lists.newArrayList(Type.HLL), Type.HLL, Type.HLL,
                        "",
                        "",
                        "",
                        "",
                        "",
                        true, false, true, true));

        for (Type t : Type.getTrivialTypes()) {
            if (t.isNull()) {
                continue; // NULL is handled through type promotion.
            }
            if (t.isScalarType(PrimitiveType.CHAR)) {
                continue; // promoted to STRING
            }
            // Count
            // vectorized
            addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                    Lists.newArrayList(t), Type.BIGINT, Type.BIGINT,
                    "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, true, true));

            // count in multi distinct
            if (t.equals(Type.CHAR) || t.equals(Type.VARCHAR)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            } else if (t.equals(Type.STRING)) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.STRING,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            } else if (t.equals(Type.TINYINT) || t.equals(Type.SMALLINT) || t.equals(Type.INT)
                    || t.equals(Type.BIGINT) || t.equals(Type.LARGEINT) || t.equals(Type.DOUBLE)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        t,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            } else if (t.equals(Type.DATE) || t.equals(Type.DATETIME)) {
                // now we don't support datetime distinct
            } else if (t.equals(Type.MAX_DECIMALV2_TYPE)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.MAX_DECIMALV2_TYPE,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL32)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.DECIMAL32,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL64)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.DECIMAL64,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL128)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.DECIMAL128,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            }

            // sum in multi distinct
            if (t.equals(Type.BIGINT) || t.equals(Type.LARGEINT) || t.equals(Type.DOUBLE)) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        t,
                        t,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            }  else if (t.equals(Type.MAX_DECIMALV2_TYPE)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.MAX_DECIMALV2_TYPE,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL32)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.DECIMAL32,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL64)) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.DECIMAL64,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            } else if (t.equals(Type.DECIMAL128)) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.DECIMAL128,
                        "",
                        "",
                        "",
                        "",
                        null,
                        null,
                        "",
                        false, true, true, true));
            }
            // Min
            addBuiltin(AggregateFunction.createBuiltin("min",
                    Lists.newArrayList(t), t, t, "",
                    "",
                    "",
                    null, null,
                    null, null, true, true, false, true));

            // Max
            addBuiltin(AggregateFunction.createBuiltin("max",
                    Lists.newArrayList(t), t, t, "",
                    "",
                    "",
                    null, null,
                    null, null, true, true, false, true));

            // Any
            addBuiltin(AggregateFunction.createBuiltin("any", Lists.newArrayList(t), t, t, null, null, null, null, null,
                    null, null, true, false, false, true));
            // Any_Value
            addBuiltin(AggregateFunction.createBuiltin("any_value", Lists.newArrayList(t), t, t, null, null, null, null,
                    null, null, null, true, false, false, true));

            // vectorized
            for (Type kt : Type.getTrivialTypes()) {
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

            // vectorized
            addBuiltin(AggregateFunction.createBuiltin("ndv", Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                            "",
                            "",
                            "",
                            "",
                            "",
                            true, true, true, true));

            // vectorized
            addBuiltin(AggregateFunction.createBuiltin("approx_count_distinct", Lists.newArrayList(t), Type.BIGINT,
                            Type.VARCHAR,
                            "",
                            "",
                            "",
                            "",
                            "",
                            true, true, true, true));

            // vectorized
            addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_INT,
                    Lists.newArrayList(t), Type.BIGINT, t,
                    "",
                    "",
                    "",
                    "",
                    "",
                    true, false, true, true));

            // VEC_INTERSECT_COUNT
            addBuiltin(
                    AggregateFunction.createBuiltin(INTERSECT_COUNT, Lists.newArrayList(Type.BITMAP, t, t), Type.BIGINT,
                            Type.VARCHAR, true, "",
                            "", "",
                            "", null, null,
                            "", true, false, true, true));

            // TopN
            if (TOPN_UPDATE_SYMBOL.containsKey(t)) {
                // vectorized
                addBuiltin(AggregateFunction.createBuiltin("topn", Lists.newArrayList(t, Type.INT), Type.VARCHAR,
                        Type.VARCHAR,
                        "",
                        "",
                        "",
                        "",
                        "",
                        true, false, true, true));
                addBuiltin(AggregateFunction.createBuiltin("topn", Lists.newArrayList(t, Type.INT, Type.INT),
                        Type.VARCHAR, Type.VARCHAR,
                        "",
                        "",
                        "",
                        "",
                        "",
                        true, false, true, true));
            }

            if (!Type.JSONB.equals(t)) {
                for (Type valueType : Type.getMapSubTypes()) {
                    addBuiltin(AggregateFunction.createBuiltin(MAP_AGG, Lists.newArrayList(t, valueType),
                            new MapType(t, valueType),
                            Type.VARCHAR,
                            "", "", "", "", "", null, "",
                            true, true, false, true));
                }

                for (Type v : Type.getArraySubTypes()) {
                    addBuiltin(AggregateFunction.createBuiltin(MAP_AGG, Lists.newArrayList(t, new ArrayType(v)),
                            new MapType(t, new ArrayType(v)),
                            new MapType(t, new ArrayType(v)),
                            "", "", "", "", "", null, "",
                            true, true, false, true));
                }
            }

            if (STDDEV_UPDATE_SYMBOL.containsKey(t)) {
                //vec stddev stddev_samp stddev_pop
                addBuiltin(AggregateFunction.createBuiltin("stddev",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        "",
                        "",
                        "",
                        null, null, null,
                        "",
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("stddev_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        "",
                        "",
                        "",
                        null, null, null,
                        "",
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("stddev_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        "",
                        "",
                        "",
                        null, null, null,
                        "",
                        false, true, false, true));

                //vec: variance variance_samp var_samp variance_pop var_pop
                addBuiltin(AggregateFunction.createBuiltin("variance",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        "",
                        "",
                        "",
                        null, null, null,
                        "",
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("variance_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        "",
                        "",
                        "",
                        null, null, null,
                        "",
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("var_pop",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        "",
                        "",
                        "",
                        null, null, null,
                        "",
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("variance_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        "",
                        "",
                        "",
                        null, null, null,
                        "",
                        false, true, false, true));
                addBuiltin(AggregateFunction.createBuiltin("var_samp",
                        Lists.newArrayList(t), STDDEV_RETTYPE_SYMBOL.get(t), t,
                        "",
                        "",
                        "",
                        null, null, null,
                        "",
                        false, true, false, true));

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
                    Lists.<Type>newArrayList(Type.BOOLEAN), Type.BIGINT, Type.BIGINT, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));

            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.TINYINT), Type.BIGINT, Type.BIGINT, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.SMALLINT), Type.BIGINT, Type.BIGINT, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.BIGINT), Type.BIGINT, Type.BIGINT, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, Type.DOUBLE, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.MAX_DECIMALV2_TYPE), Type.MAX_DECIMALV2_TYPE, Type.MAX_DECIMALV2_TYPE, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DECIMAL32), ScalarType.DECIMAL128, Type.DECIMAL128, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DECIMAL64), Type.DECIMAL128, Type.DECIMAL128, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.DECIMAL128), Type.DECIMAL128, Type.DECIMAL128, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.<Type>newArrayList(Type.LARGEINT), Type.LARGEINT, Type.LARGEINT, "",
                    "",
                    "",
                    null, null,
                    "",
                    null, false, true, false, true));
        }

        Type[] types = {Type.SMALLINT, Type.TINYINT, Type.INT, Type.BIGINT, Type.FLOAT, Type.DOUBLE, Type.CHAR,
                Type.VARCHAR, Type.STRING};
        for (Type t : types) {
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
            //vec ORTHOGONAL_BITMAP_EXPR_CALCULATE and ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT
            addBuiltin(
                    AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_EXPR_CALCULATE, Lists.newArrayList(Type.BITMAP, t, Type.STRING),
                            Type.BITMAP, Type.BITMAP, true, "", "", "", "", "", "", "", true, false, true, true));

            addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_EXPR_CALCULATE_COUNT,
                    Lists.newArrayList(Type.BITMAP, t, Type.STRING), Type.BIGINT, Type.BITMAP, true, "", "", "", "", "", "", "",
                    true, false, true, true));
        }

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
                Type.BITMAP,
                "",
                "",
                "",
                "",
                "",
                null,
                "",
                true, true, true, true));
        // ORTHOGONAL_BITMAP_UNION_COUNT vectorized
        addBuiltin(AggregateFunction.createBuiltin(ORTHOGONAL_BITMAP_UNION_COUNT, Lists.newArrayList(Type.BITMAP),
                Type.BIGINT, Type.BITMAP, "", "", "", "", null, null, "", true, true, true, true));

        addBuiltin(AggregateFunction.createBuiltin(BITMAP_INTERSECT, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP, Type.BITMAP,
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

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
            if (!t.equals(Type.LARGEINT)) {
                addBuiltin(
                        AggregateFunction.createBuiltin("bitmap_agg", Lists.newArrayList(t), Type.BITMAP, Type.BITMAP,
                                "",
                                "",
                                "",
                                "",
                                "",
                                true, false, true, true));
            }
        }

        addBuiltin(AggregateFunction.createBuiltin(QUANTILE_UNION, Lists.newArrayList(Type.QUANTILE_STATE),
                Type.QUANTILE_STATE,
                Type.QUANTILE_STATE,
                "",
                "",
                "",
                "",
                "",
                true, false, true, true));

        //vec percentile and percentile_approx
        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                "",
                "",
                "",
                "",
                "",
                false, true, false, true));

        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                "",
                "",
                "",
                "",
                "",
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

            // histogram | hist
            addBuiltin(AggregateFunction.createBuiltin(HIST, Lists.newArrayList(t), Type.VARCHAR, t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(AggregateFunction.createBuiltin(HISTOGRAM, Lists.newArrayList(t), Type.VARCHAR, t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(AggregateFunction.createBuiltin(HIST, Lists.newArrayList(t, Type.INT), Type.VARCHAR, t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(AggregateFunction.createBuiltin(HISTOGRAM, Lists.newArrayList(t, Type.INT), Type.VARCHAR, t,
                    "", "", "", "", "", true, false, true, true));
            addBuiltin(AggregateFunction.createBuiltin(HISTOGRAM, Lists.newArrayList(t, Type.DOUBLE, Type.INT),
                    Type.VARCHAR, t,
                    "", "", "", "", "", true, false, true, true));

            // group array
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

            addBuiltin(AggregateFunction.createBuiltin(ARRAY_AGG, Lists.newArrayList(t), new ArrayType(t), t, "", "", "", "", "",
                    true, false, true, true));

            //first_value/last_value for array
            addBuiltin(AggregateFunction.createAnalyticBuiltin("first_value",
                    Lists.newArrayList(new ArrayType(t)), new ArrayType(t), Type.ARRAY,
                    "",
                    "",
                    null,
                    "",
                    "", true));

            addBuiltin(AggregateFunction.createAnalyticBuiltin("last_value",
                    Lists.newArrayList(new ArrayType(t)), new ArrayType(t), Type.ARRAY,
                    "",
                    "",
                    null,
                    "",
                    "", true));
        }

        // Avg
        // TODO: switch to CHAR(sizeof(AvgIntermediateType) when that becomes available

        // vectorized avg
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.<Type>newArrayList(Type.BOOLEAN), Type.DOUBLE, Type.TINYINT,
                "", "", "", "", "", "", "",
                false, true, false, true));
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
                Lists.<Type>newArrayList(Type.LARGEINT), Type.DOUBLE, Type.LARGEINT,
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

        // Group_concat(string) vectorized
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.VARCHAR), Type.VARCHAR,
                Type.VARCHAR, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.VARCHAR), Type.VARCHAR,
                Type.VARCHAR, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.CHAR), Type.CHAR,
                Type.CHAR, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.CHAR), Type.CHAR,
                Type.CHAR, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.STRING), Type.STRING,
                Type.STRING, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.STRING), Type.STRING,
                Type.STRING, "", "", "", "", "", false, true, false, true));
        // Group_concat(string, string) vectorized
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.VARCHAR, Type.VARCHAR, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.VARCHAR, Type.VARCHAR),
                Type.VARCHAR, Type.VARCHAR, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.CHAR, Type.CHAR),
                Type.CHAR, Type.CHAR, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.CHAR, Type.CHAR),
                Type.CHAR, Type.CHAR, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("group_concat", Lists.<Type>newArrayList(Type.STRING, Type.STRING),
                Type.STRING, Type.STRING, "", "", "", "", "", false, true, false, true));
        addBuiltin(AggregateFunction.createBuiltin("multi_distinct_group_concat", Lists.<Type>newArrayList(Type.STRING, Type.STRING),
                Type.STRING, Type.STRING, "", "", "", "", "", false, true, false, true));

        // analytic functions
        // Rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("rank",
                Lists.<Type>newArrayList(), Type.BIGINT, Type.VARCHAR,
                "",
                "",
                null,
                "",
                ""));
        // Dense rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("dense_rank",
                Lists.<Type>newArrayList(), Type.BIGINT, Type.VARCHAR,
                "",
                "",
                null,
                "",
                ""));
        //row_number
        addBuiltin(AggregateFunction.createAnalyticBuiltin("row_number",
                new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
                "",
                "",
                "",
                null, null));
        //ntile, we use rewrite sql for ntile, actually we don't really need this.
        addBuiltin(AggregateFunction.createAnalyticBuiltin("ntile",
                Collections.singletonList(Type.BIGINT), Type.BIGINT, Type.BIGINT, null, null, null, null, null));

        //vec Rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("rank",
                Lists.<Type>newArrayList(), Type.BIGINT, Type.VARCHAR,
                "",
                "",
                null,
                "",
                "", true));
        //vec Dense rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("dense_rank",
                Lists.<Type>newArrayList(), Type.BIGINT, Type.VARCHAR,
                "",
                "",
                null,
                "",
                "", true));
        //vec row_number
        addBuiltin(AggregateFunction.createAnalyticBuiltin("row_number",
                new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
                "",
                "",
                "",
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
                    "",
                    "",
                    null,
                    "",
                    ""));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "last_value", Lists.newArrayList(t), t, t,
                    "",
                    "",
                    "",
                    "",
                    ""));

            //vec first_value
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "first_value", Lists.newArrayList(t), t, t,
                    "",
                    "",
                    null,
                    "",
                    "", true));
            // Implements FIRST_VALUE for some windows that require rewrites during planning.
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "first_value_rewrite", Lists.newArrayList(t, Type.BIGINT), t, t,
                    "",
                    "",
                    null,
                    "",
                    "",
                    false, false));
            //vec last_value
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "last_value", Lists.newArrayList(t), t, t,
                    "",
                    "",
                    "",
                    "",
                    "", true));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    "",
                    "",
                    null, "", null));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    "",
                    "",
                    null, "", null));
            //vec
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    "",
                    "",
                    null, null, null, true));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    "",
                    "",
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
        for (Map.Entry<String, List<Function>> entry : vectorizedFunctions.entrySet()) {
            builtinFunctions.addAll(entry.getValue());
        }
        return builtinFunctions;
    }

    public List<Function> getAllFunctions() {
        List<Function> functions = Lists.newArrayList();
        vectorizedFunctions.forEach((k, v) -> functions.addAll(v));
        tableFunctions.forEach((k, v) -> functions.addAll(v));
        return functions;
    }

    public static final String EXPLODE_SPLIT = "explode_split";
    public static final String EXPLODE_BITMAP = "explode_bitmap";
    public static final String EXPLODE_JSON_ARRAY_INT = "explode_json_array_int";
    public static final String EXPLODE_JSON_ARRAY_DOUBLE = "explode_json_array_double";
    public static final String EXPLODE_JSON_ARRAY_STRING = "explode_json_array_string";
    public static final String EXPLODE_JSON_ARRAY_JSON = "explode_json_array_json";
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

        initTableFunctionListWithCombinator(EXPLODE_JSON_ARRAY_JSON);
        addTableFunctionWithCombinator(EXPLODE_JSON_ARRAY_JSON, Type.VARCHAR,
                Function.NullableMode.DEPEND_ON_ARGUMENT, Lists.newArrayList(Type.VARCHAR), false,
                "_ZN5doris19DummyTableFunctions25explode_json_array_jsonEPN9doris_udf15FunctionContextERKNS1_9StringValE");

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

    public boolean isAggFunctionName(String name) {
        return aggFunctionNames.contains(name);
    }
}
