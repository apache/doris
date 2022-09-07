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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.types.coercion.TypeCollection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** avg agg function. */
public class Avg extends AggregateFunction implements UnaryExpression, ImplicitCastInputTypes {

    // used in interface expectedInputTypes to avoid new list in each time it be called
    private static final List<AbstractDataType> EXPECTED_INPUT_TYPES = ImmutableList.of(
            new TypeCollection(NumericType.INSTANCE, DateTimeType.INSTANCE, DateType.INSTANCE)
    );

    public Avg(Expression child) {
        super("avg", child);
    }

    private Avg(Expression child, boolean isLocal) {
        super("avg", isLocal, child);
    }

    @Override
    public DataType getGlobalDataType() {
        if (child().getDataType() instanceof DecimalType) {
            return child().getDataType();
        } else if (child().getDataType().isDate()) {
            return DateType.INSTANCE;
        } else if (child().getDataType().isDateTime()) {
            return DateTimeType.INSTANCE;
        } else {
            return DoubleType.INSTANCE;
        }
    }

    @Override
    public DataType getLocalDataType() {
        DataType dataType = child().getDataType();
        if (dataType instanceof LargeIntType) {
            return dataType;
        } else if (dataType instanceof DecimalType) {
            // TODO: precision + 10
            return dataType;
        } else if (dataType instanceof IntegralType) {
            return BigIntType.INSTANCE;
        } else if (dataType instanceof FractionalType) {
            // TODO: precision + 10
            return DoubleType.INSTANCE;
        } else {
            throw new IllegalStateException("Unsupported sum type: " + dataType);
        }
    }

    @Override
    public boolean nullable() {
        return child().nullable();
    }

    @Override
    public Avg withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Avg(children.get(0), isLocal);
    }

    @Override
    public Avg withLocal(boolean isLocal) {
        return new Avg(child(), isLocal);
    }

    @Override
    public DataType getIntermediateType() {
        return VarcharType.createVarcharType(-1);
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return EXPECTED_INPUT_TYPES;
    }
}
