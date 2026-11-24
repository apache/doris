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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.LegacyAggStateResultTypeResolver;
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
    // Effective nested result type, or null for an unbound declaration.
    private final DataType resultType;
    // Legacy columns omit the result type in their catalog and wire representations.
    private final boolean resultTypeInferredFromLegacy;

    /**
     * Constructor for AggStateType
     * @param functionName     nested function's name
     * @param subTypes         nested function's argument list
     * @param subTypeNullables nested nested function's argument's nullable list
     * @param returnNullable nested function's return nullable
     */
    public AggStateType(String functionName, List<DataType> subTypes,
            List<Boolean> subTypeNullables, boolean returnNullable) {
        this(functionName, subTypes, subTypeNullables, returnNullable, null);
    }

    /** Constructor retaining the producer's result signature. */
    public AggStateType(String functionName, List<DataType> subTypes,
            List<Boolean> subTypeNullables, boolean returnNullable, DataType resultType) {
        this(functionName, subTypes, subTypeNullables, returnNullable, resultType, false);
    }

    private AggStateType(String functionName, List<DataType> subTypes, List<Boolean> subTypeNullables,
            boolean returnNullable, DataType resultType,
            boolean resultTypeInferredFromLegacy) {
        this.resultType = resultType;
        this.resultTypeInferredFromLegacy = resultTypeInferredFromLegacy;
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
    }

    /** Record a declaration without binding functions or consulting the session in the parser. */
    public static AggStateType declaration(String name, List<DataType> inputs,
            List<Boolean> nullables, boolean returnNullable) {
        return new AggStateType(name, inputs, nullables, returnNullable);
    }

    /** Resolve new declarations only; catalog types retain their saved signatures. */
    public AggStateType resolveDeclaration() {
        if (resultType != null) {
            return this;
        }
        List<DataType> inputs = subTypes.stream()
                .map(AggStateType::resolveNestedDeclaration)
                .collect(ImmutableList.toImmutableList());
        AggregateFunction nested = bindNestedAggregate(inputs);
        // SQL declarations can require implicit casts before their signature is saved.
        nested = (AggregateFunction) TypeCoercionUtils.implicitCastInputTypes(nested, nested.expectedInputTypes());
        return new AggStateType(functionName,
                nested.children().stream().map(Expression::getDataType).collect(Collectors.toList()),
                nested.children().stream().map(Expression::nullable).collect(Collectors.toList()),
                returnNullable, nested.getDataType());
    }

    private static DataType resolveNestedDeclaration(DataType type) {
        if (type instanceof AggStateType) {
            return ((AggStateType) type).resolveDeclaration();
        }
        if (type instanceof ArrayType) {
            return ArrayType.of(resolveNestedDeclaration(((ArrayType) type).getItemType()));
        }
        if (type instanceof MapType) {
            MapType map = (MapType) type;
            return MapType.of(resolveNestedDeclaration(map.getKeyType()), resolveNestedDeclaration(map.getValueType()));
        }
        if (type instanceof StructType) {
            return new StructType(((StructType) type).getFields().stream()
                    .map(field -> field.withDataType(resolveNestedDeclaration(field.getDataType())))
                    .collect(ImmutableList.toImmutableList()));
        }
        return type;
    }

    static AggStateType fromCatalogAggState(org.apache.doris.catalog.AggStateType catalogType) {
        List<DataType> inputs = catalogType.getSubTypes().stream().map(DataType::fromCatalogType)
                .collect(Collectors.toList());
        if (catalogType.getResultType() != null) {
            return new AggStateType(catalogType.getFunctionName(), inputs, catalogType.getSubTypeNullables(),
                    catalogType.getResultIsNullable(), fromCatalogType(catalogType.getResultType()));
        }
        AggStateType legacy = new AggStateType(catalogType.getFunctionName(), inputs,
                catalogType.getSubTypeNullables(), catalogType.getResultIsNullable());
        DataType inferred = legacy.getResultType(legacy.bindNestedAggregate(inputs));
        return new AggStateType(legacy.functionName, inputs, legacy.subTypeNullables, legacy.returnNullable,
                inferred, true);
    }

    private AggregateFunction bindNestedAggregate(List<DataType> argumentTypes) {
        ImmutableList.Builder<Expression> arguments = ImmutableList.builderWithExpectedSize(subTypes.size());
        for (int i = 0; i < subTypes.size(); i++) {
            arguments.add(new SlotReference("input" + i, argumentTypes.get(i), subTypeNullables.get(i)));
        }
        List<Expression> inputs = arguments.build();
        FunctionBuilder builder = Env.getCurrentEnv().getFunctionRegistry()
                .findBuiltinAggregateBuilder(functionName, inputs);
        // Only types are available here. Constant arguments are validated when binding the producer.
        return (AggregateFunction) builder.build(functionName, inputs).first;
    }

    /** Effective result signature used for type comparison and aggregate-state consumers. */
    public DataType getResultType() {
        return resultType;
    }

    /** Resolve a legacy consumer without changing the stored state signature or session settings. */
    public DataType getResultType(AggregateFunction nested) {
        if (resultType != null) {
            return resultType;
        }
        if (nested instanceof LegacyAggStateResultTypeResolver) {
            return ((LegacyAggStateResultTypeResolver) nested).resolveLegacyAggStateResultType(subTypes.get(0));
        }
        return nested.getDataType();
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

    @Override
    public Type toCatalogDataType() {
        List<Type> types = subTypes.stream().map(DataType::toCatalogDataType).collect(Collectors.toList());
        return new org.apache.doris.catalog.AggStateType(functionName, returnNullable, types, subTypeNullables,
                resultTypeInferredFromLegacy || resultType == null ? null : resultType.toCatalogDataType());
    }

    @Override
    public DataType conversion() {
        return new AggStateType(functionName,
                subTypes.stream().map(DataType::conversion).collect(Collectors.toList()),
                subTypeNullables, returnNullable,
                resultType == null ? null : resultType.conversion(), resultTypeInferredFromLegacy);
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
        return returnNullable == rhs.returnNullable
                && Objects.equals(resultType, rhs.resultType)
                && functionName.equals(rhs.functionName)
                && subTypes.equals(rhs.subTypes)
                && subTypeNullables.equals(rhs.subTypeNullables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), subTypes, subTypeNullables, functionName, resultType, returnNullable);
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
    public boolean allowsKeyUse(KeyUse keyUse) {
        return subTypes.stream().allMatch(subType -> subType.allowsKeyUse(keyUse));
    }

    @Override
    public Optional<Expression> zeroValue() {
        return Optional.empty();
    }
}
