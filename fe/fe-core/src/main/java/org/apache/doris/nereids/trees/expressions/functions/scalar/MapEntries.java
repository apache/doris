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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'map_entries'. Converts map to array of struct with named
 * fields 'key' and 'value'.
 */
public class MapEntries extends ScalarFunction
        implements UnaryExpression, CustomSignature, PropagateNullable {

    /**
     * constructor with 1 argument.
     */
    public MapEntries(Expression arg) {
        super("map_entries", arg);
    }

    /** constructor for withChildren and reuse signature */
    public MapEntries(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public MapEntries withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MapEntries(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapEntries(this, context);
    }

    @Override
    public FunctionSignature customSignature() {
        DataType inputType = getArgument(0).getDataType();
        if (inputType.isNullType()) {
            StructType structType = new StructType(
                    ImmutableList.of(
                            new StructField("key", NullType.INSTANCE, true, ""),
                            new StructField("value", NullType.INSTANCE, true, "")));
            return FunctionSignature.ret(ArrayType.of(structType)).args(inputType);
        } else if (inputType.isMapType()) {
            MapType mapType = (MapType) inputType;
            DataType keyType = mapType.getKeyType();
            DataType valueType = mapType.getValueType();

            StructType structType = new StructType(
                    ImmutableList.of(
                            new StructField("key", keyType, true, ""),
                            new StructField("value", valueType, true, "")));
            return FunctionSignature.ret(ArrayType.of(structType)).args(inputType);
        } else {
            SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
            return null; // unreachable
        }
    }
}
