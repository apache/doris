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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'map_contains_entry'.
 */
public class MapContainsEntry extends ScalarFunction
        implements TernaryExpression, ExplicitlyCastableSignature {

    public static final List<FunctionSignature> FOLLOW_DATATYPE_SIGNATURE = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(MapType.of(new AnyDataType(0), new AnyDataType(1)),
                            new FollowToAnyDataType(0),
                            new FollowToAnyDataType(1))
    );

    public static final List<FunctionSignature> MIN_COMMON_TYPE_SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(MapType.of(new AnyDataType(0), new AnyDataType(1)),
                            new AnyDataType(0),
                            new AnyDataType(1))
    );

    /**
     * constructor with 3 arguments.
     */
    public MapContainsEntry(Expression arg0, Expression arg1, Expression arg2) {
        super("map_contains_entry", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    public MapContainsEntry(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public MapContainsEntry withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new MapContainsEntry(getFunctionParams(children));
    }

    @Override
    public boolean nullable() {
        return child(0).nullable();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapContainsEntry(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (getArgument(0).getDataType().isMapType()) {
            MapType mapType = (MapType) getArgument(0).getDataType();
            if (mapType.getKeyType().isSameTypeForComplexTypeParam(getArgument(1).getDataType())
                    && mapType.getValueType().isSameTypeForComplexTypeParam(getArgument(2).getDataType())) {
                // return least common type
                return MIN_COMMON_TYPE_SIGNATURES;
            }
        }
        return FOLLOW_DATATYPE_SIGNATURE;
    }
}
