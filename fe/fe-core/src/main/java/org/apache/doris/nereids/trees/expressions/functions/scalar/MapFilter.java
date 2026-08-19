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
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Scalar function map_filter.
 *
 * <p>The Map lambda is evaluated by an ArrayMap over the Map's key and value arrays:
 *
 * <pre>
 * map_filter((mapKey, mapValue) -> predicate, inputMap)
 *   ->
 * map_filter(
 *   inputMap,
 *   array_map(
 *     (mapKey, mapValue) -> predicate,
 *     map_keys(inputMap), map_values(inputMap)))
 * </pre>
 */
public class MapFilter extends ScalarFunction
        implements HighOrderFunction, PropagateNullable {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0).args(
                    MapType.of(new AnyDataType(0), new AnyDataType(1)),
                    ArrayType.of(BooleanType.INSTANCE)));

    private final boolean validateMapLambdaInput;

    // The argument is a bound Lambda.
    public MapFilter(Expression arg) {
        this(MapLambdaValidator.requireLambda("map_filter", arg));
    }

    public MapFilter(Expression map, Expression filter) {
        super("map_filter", map, filter);
        validateMapLambdaInput = false;
    }

    private MapFilter(Lambda lambda) {
        super("map_filter",
                MapLambdaValidator.extractMapExpression("map_filter", lambda),
                new MapEntryArrayMap(lambda));
        validateMapLambdaInput = true;
    }

    private MapFilter(ScalarFunctionParams functionParams, boolean validateMapLambdaInput) {
        super(functionParams);
        this.validateMapLambdaInput = validateMapLambdaInput;
    }

    public boolean shouldValidateMapLambdaInput() {
        return validateMapLambdaInput;
    }

    @Override
    public MapFilter withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new MapFilter(getFunctionParams(children), validateMapLambdaInput);
    }

    @Override
    public List<FunctionSignature> getImplSignature() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapFilter(this, context);
    }
}
