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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.expressions.Expression;

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
    private final int beExecVersion;

    /**
     * Constructor for AggStateType
     * @param functionName     nested function's name
     * @param subTypes         nested function's argument list
     * @param subTypeNullables nested nested function's argument's nullable list
     * @param returnNullable nested function's return nullable
     */
    public AggStateType(String functionName, List<DataType> subTypes,
            List<Boolean> subTypeNullables, boolean returnNullable) {
        this(functionName, subTypes, subTypeNullables, returnNullable, Config.be_exec_version);
    }

    /** Preserve the codec version when reconstructing a stored state type. */
    public AggStateType(String functionName, List<DataType> subTypes,
            List<Boolean> subTypeNullables, boolean returnNullable, int beExecVersion) {
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
        this.beExecVersion = beExecVersion;
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

    public int getBeExecVersion() {
        return beExecVersion;
    }

    /** Keep the aggregate signature when choosing a producer's output codec. */
    public AggStateType withBeExecVersion(int version) {
        return new AggStateType(functionName, subTypes, subTypeNullables, returnNullable, version);
    }

    /** Compare the aggregate signature, without asserting that its state bytes are compatible. */
    public boolean hasSameAggregateSignature(AggStateType other) {
        return functionName.equals(other.functionName)
                && subTypes.equals(other.subTypes)
                && subTypeNullables.equals(other.subTypeNullables)
                && returnNullable == other.returnNullable;
    }

    /**
     * Allow the version 19 to 20 storage cast without changing its state bytes.
     * Version 20 keeps the version 19 AGG_STATE storage columns and accepts their payloads.
     * For example, nullable SUM still stores a flag followed by the nested SUM state.
     * This contract does not apply to native aggregation intermediate columns.
     *
     * Only allow the known forward transition with the same aggregate signature. Do not
     * infer reverse compatibility: version 20 can write a Bitmap no-input marker that the
     * old writer never produced. Keep equals version-aware so direct STATE producers still
     * get a cast and can use the sink's codec.
     */
    public boolean canUpgradeStoredStateFrom(AggStateType source) {
        return beExecVersion == Config.UNIFIED_NULLABLE_AGGREGATION_MIN_BE_EXEC_VERSION
                && source.beExecVersion == Config.UNIFIED_NULLABLE_AGGREGATION_MIN_BE_EXEC_VERSION - 1
                && hasSameAggregateSignature(source);
    }

    @Override
    public Type toCatalogDataType() {
        List<Type> types = subTypes.stream().map(DataType::toCatalogDataType).collect(Collectors.toList());
        return new org.apache.doris.catalog.AggStateType(
                functionName, returnNullable, types, subTypeNullables, beExecVersion);
    }

    @Override
    public DataType conversion() {
        return new AggStateType(functionName,
                subTypes.stream().map(DataType::conversion).collect(Collectors.toList()),
                subTypeNullables, returnNullable, beExecVersion);
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
        // A sink must not treat version 20 state bytes as version 19 bytes.
        // Keep the cast so that the STATE producer can use the target codec.
        if (beExecVersion != rhs.beExecVersion) {
            return false;
        }
        if (!Objects.equals(functionName, rhs.functionName)) {
            return false;
        }
        if ((subTypes == null) != (rhs.subTypes == null)) {
            return false;
        }
        if (subTypes == null) {
            return true;
        }
        if (subTypes.size() != rhs.subTypes.size()) {
            return false;
        }

        for (int i = 0; i < subTypes.size(); i++) {
            if (!subTypes.get(i).equals(rhs.subTypes.get(i))) {
                return false;
            }
            if (!subTypeNullables.get(i).equals(rhs.subTypeNullables.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), subTypes, subTypeNullables, functionName, beExecVersion);
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
