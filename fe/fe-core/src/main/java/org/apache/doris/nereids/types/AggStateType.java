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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * AggStateType type in Nereids.
 */
public class AggStateType extends DataType {

    public static final int WIDTH = 16;

    private static final Map<String, String> aliasToName = ImmutableMap.<String, String>builder()
            .put("substr", "substring")
            .put("ifnull", "nvl")
            .put("rand", "random")
            .put("add_months", "months_add")
            .put("curdate", "current_date")
            .put("ucase", "upper")
            .put("lcase", "lower")
            .put("hll_raw_agg", "hll_union")
            .put("approx_count_distinct", "ndv")
            .put("any", "any_value")
            .put("char_length", "character_length")
            .put("stddev_pop", "stddev")
            .put("percentile_cont", "percentile")
            .put("var_pop", "variance")
            .put("variance_pop", "variance")
            .put("var_samp", "variance_samp")
            .put("hist", "histogram")
            .put("map_agg", "map_agg_v2")
            .put("window_funnel", "window_funnel_v2")
            .build();

    private final String functionName;
    private final List<DataType> subTypes;
    private final List<Boolean> subTypeNullables;
    private final boolean returnNullable;
    // Null until a SQL declaration is bound during analysis.
    private final DataType resultType;
    // Planning-only coercion policy; it is not part of the serialized AggState type identity.
    private final boolean looseTypeCoercionAllowed;

    /**
     * Constructor for AggStateType
     * @param functionName     nested function's name
     * @param subTypes         nested function's argument list
     * @param subTypeNullables nested nested function's argument's nullable list
     * @param returnNullable nested function's return nullable
     */
    public AggStateType(String functionName, List<DataType> subTypes,
            List<Boolean> subTypeNullables, boolean returnNullable) {
        this(functionName, subTypes, subTypeNullables, returnNullable, true);
    }

    /** Constructor with a planning-time type coercion policy. */
    public AggStateType(String functionName, List<DataType> subTypes,
            List<Boolean> subTypeNullables, boolean returnNullable, boolean looseTypeCoercionAllowed) {
        this(functionName, subTypes, subTypeNullables, returnNullable, looseTypeCoercionAllowed, null);
    }

    /** Constructor retaining the producer's result signature. */
    public AggStateType(String functionName, List<DataType> subTypes,
            List<Boolean> subTypeNullables, boolean returnNullable, boolean looseTypeCoercionAllowed,
            DataType resultType) {
        this.resultType = resultType;
        // be only supports lowercase function names
        Objects.requireNonNull(functionName, "functionName should not be null");
        functionName = functionName.toLowerCase(Locale.ROOT);
        this.functionName = aliasToName.getOrDefault(functionName, functionName);
        this.subTypes = ImmutableList.copyOf(Objects.requireNonNull(subTypes, "subTypes should not be null"));
        this.subTypeNullables = ImmutableList
                .copyOf(Objects.requireNonNull(subTypeNullables, "subTypeNullables should not be null"));
        Preconditions.checkState(subTypes.size() == subTypeNullables.size(),
                "AggStateType' subTypes.size()!=subTypeNullables.size()");
        this.returnNullable = returnNullable;
        this.looseTypeCoercionAllowed = looseTypeCoercionAllowed;
    }

    /** Retain the result type selected by FE for every aggregate state. */
    public static DataType resultTypeFor(AggregateFunction nested) {
        if (Config.be_exec_version < Config.AGG_STATE_RESULT_TYPE_MIN_BE_EXEC_VERSION) {
            throw new AnalysisException("Aggregate states require be_exec_version >= "
                    + Config.AGG_STATE_RESULT_TYPE_MIN_BE_EXEC_VERSION);
        }
        return nested.getDataType();
    }

    /** Record a declaration without binding functions or consulting the session in the parser. */
    public static AggStateType declaration(String name, List<DataType> inputs,
            List<Boolean> nullables, boolean returnNullable) {
        return new AggStateType(name, inputs, nullables, returnNullable, true, null);
    }

    /** Resolve new declarations only; catalog types retain their saved signatures. */
    public AggStateType resolveDeclaration() {
        if (resultType != null) {
            return this;
        }
        ImmutableList.Builder<Expression> arguments = ImmutableList.builderWithExpectedSize(subTypes.size());
        for (int i = 0; i < subTypes.size(); i++) {
            arguments.add(new SlotReference("input" + i, subTypes.get(i), subTypeNullables.get(i)));
        }
        List<Expression> inputs = arguments.build();
        FunctionBuilder builder = Env.getCurrentEnv().getFunctionRegistry().findFunctionBuilder(functionName, inputs);
        AggregateFunction nested = (AggregateFunction) builder.build(functionName, inputs).first;
        // Declarations contain types, not values: percentile's level, for example, is not a literal here.
        // Validate constant arguments when analyzing the state producer, and only bind types here.
        nested = (AggregateFunction) TypeCoercionUtils.implicitCastInputTypes(nested, nested.expectedInputTypes());
        return new AggStateType(functionName,
                nested.children().stream().map(Expression::getDataType).collect(Collectors.toList()),
                nested.children().stream().map(Expression::nullable).collect(Collectors.toList()),
                returnNullable, true, resultTypeFor(nested));
    }

    public DataType getResultType() {
        if (resultType == null) {
            throw new AnalysisException("Aggregate state is missing its result type; recreate the state");
        }
        return resultType;
    }

    public List<DataType> getSubTypes() {
        return subTypes;
    }

    public List<Boolean> getSubTypeNullables() {
        return subTypeNullables;
    }

    public String getFunctionName() {
        return functionName;
    }

    public boolean isLooseTypeCoercionAllowed() {
        return looseTypeCoercionAllowed;
    }

    @Override
    public Type toCatalogDataType() {
        List<Type> types = subTypes.stream().map(DataType::toCatalogDataType).collect(Collectors.toList());
        return new org.apache.doris.catalog.AggStateType(functionName, returnNullable, types, subTypeNullables,
                resultType == null ? null : resultType.toCatalogDataType());
    }

    @Override
    public DataType conversion() {
        return new AggStateType(functionName,
                subTypes.stream().map(DataType::conversion).collect(Collectors.toList()),
                subTypeNullables, returnNullable, looseTypeCoercionAllowed,
                resultType == null ? null : resultType.conversion());
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof AggStateType;
    }

    @Override
    public String simpleString() {
        return "agg_state";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AggStateType)) {
            return false;
        }

        AggStateType rhs = (AggStateType) o;
        return Objects.equals(resultType, rhs.resultType)
                && functionName.equals(rhs.functionName)
                && subTypes.equals(rhs.subTypes)
                && subTypeNullables.equals(rhs.subTypeNullables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), subTypes, subTypeNullables, functionName, resultType);
    }

    @Override
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return toCatalogDataType().toSql();
    }

    @Override
    public Optional<Expression> zeroValue() {
        return Optional.empty();
    }
}
