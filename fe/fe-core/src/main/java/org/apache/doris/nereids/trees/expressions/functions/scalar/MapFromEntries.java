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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.PreferPushDownProject;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecision;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Construct a Map from an Array of two-field Struct entries. */
public class MapFromEntries extends ScalarFunction
        implements UnaryExpression, ComputePrecision, CustomSignature, PropagateNullable, PreferPushDownProject {

    public MapFromEntries(Expression entries) {
        super("map_from_entries", entries);
    }

    protected MapFromEntries(String name, Expression entries) {
        super(name, entries);
    }

    protected MapFromEntries(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public MapFromEntries withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MapFromEntries(getFunctionParams(children));
    }

    @Override
    public FunctionSignature customSignature() {
        DataType inputType = getArgumentType(0);
        if (inputType.isNullType()) {
            inputType = ArrayType.of(defaultStructType());
        }
        if (!(inputType instanceof ArrayType)) {
            throw new AnalysisException(
                    "map_from_entries requires an array of structs with exactly two fields");
        }
        DataType itemType = ((ArrayType) inputType).getItemType();
        if (itemType.isNullType()) {
            inputType = ArrayType.of(defaultStructType());
        } else if (!(itemType instanceof StructType)) {
            throw new AnalysisException(
                    "map_from_entries requires an array of structs with exactly two fields");
        } else {
            inputType = TypeCoercionUtils.replaceSpecifiedType(
                    inputType, NullType.class, TinyIntType.INSTANCE);
        }
        List<StructField> fields = ((StructType) ((ArrayType) inputType).getItemType()).getFields();
        if (fields.size() != 2) {
            throw new AnalysisException(
                    "map_from_entries requires an array of structs with exactly two fields");
        }
        MapType resultType = MapType.of(fields.get(0).getDataType(), fields.get(1).getDataType());
        resultType.validateDataType();
        return FunctionSignature.ret(resultType).args(inputType);
    }

    private static StructType defaultStructType() {
        return new StructType(ImmutableList.of(
                new StructField("key", TinyIntType.INSTANCE, true, ""),
                new StructField("value", TinyIntType.INSTANCE, true, "")));
    }

    // Prevent STRUCT<DECIMAL(38,0), DECIMAL(38,38)> from being resolved as
    // STRUCT<DECIMAL(38,6), DECIMAL(38,6)>, and nested DATETIMEV2(6) as DATETIMEV2(0).
    @Override
    public FunctionSignature computePrecision(FunctionSignature signature) {
        return signature;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapFromEntries(this, context);
    }
}
