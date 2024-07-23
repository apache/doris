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

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
            .put("var_pop", "variance")
            .put("variance_pop", "variance")
            .put("var_samp", "variance_samp")
            .put("hist", "histogram")
            .build();

    private final List<DataType> subTypes;
    private final List<Boolean> subTypeNullables;
    private final String functionName;

    /**
     * Constructor for AggStateType
     * @param functionName     nested function's name
     * @param subTypes         nested function's argument list
     * @param subTypeNullables nested nested function's argument's nullable list
     */
    public AggStateType(String functionName, List<DataType> subTypes, List<Boolean> subTypeNullables) {
        this.subTypes = ImmutableList.copyOf(Objects.requireNonNull(subTypes, "subTypes should not be null"));
        this.subTypeNullables = ImmutableList
                .copyOf(Objects.requireNonNull(subTypeNullables, "subTypeNullables should not be null"));
        Preconditions.checkState(subTypes.size() == subTypeNullables.size(),
                "AggStateType' subTypes.size()!=subTypeNullables.size()");
        this.functionName = aliasToName.getOrDefault(functionName, functionName);
    }

    public List<Expression> getMockedExpressions() {
        List<Expression> result = new ArrayList<Expression>();
        for (int i = 0; i < subTypes.size(); i++) {
            result.add(new SlotReference("mocked", subTypes.get(i), subTypeNullables.get(i)));
        }
        return result;
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
        return Expr.createAggStateType(functionName, types, subTypeNullables);
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
    public int width() {
        return WIDTH;
    }

    @Override
    public String toSql() {
        return toCatalogDataType().toSql();
    }
}
