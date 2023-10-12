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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * AggStateType type in Nereids.
 */
public class AggStateType extends DataType {

    public static final AggStateType SYSTEM_DEFAULT = new AggStateType(null, ImmutableList.of(), ImmutableList.of());

    public static final int WIDTH = 16;

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
        this.functionName = functionName;
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

    @Override
    public Type toCatalogDataType() {
        List<Type> types = subTypes.stream().map(t -> t.toCatalogDataType()).collect(Collectors.toList());
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
    public DataType defaultConcreteType() {
        return SYSTEM_DEFAULT;
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
