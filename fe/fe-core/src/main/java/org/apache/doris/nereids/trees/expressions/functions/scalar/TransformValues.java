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
import org.apache.doris.nereids.trees.expressions.PreferPushDownProject;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Scalar function transform_values.
 *
 * <p>The original keys are retained while the Map lambda produces the new value array:
 *
 * <pre>
 * transform_values((mapKey, mapValue) -> newValue, inputMap)
 *   ->
 * %map_from_arrays_unique%(
 *   map_keys(inputMap),
 *   array_map(
 *     (mapKey, mapValue) -> newValue,
 *     map_keys(inputMap), map_values(inputMap)))
 * </pre>
 */
public class TransformValues extends ScalarFunction
        implements CustomSignature, PropagateNullable, PreferPushDownProject, RewriteWhenAnalyze {

    public TransformValues(Expression arg) {
        this(MapLambdaValidator.requireLambda("transform_values", arg));
    }

    private TransformValues(Lambda lambda) {
        super("transform_values",
                MapLambdaValidator.extractMapExpression("transform_values", lambda),
                new MapEntryArrayMap(lambda));
    }

    private TransformValues(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        MapType inputMapType = (MapType) getArgument(0).getDataType();
        ArrayType transformedValuesType = (ArrayType) getArgument(1).getDataType();
        DataType resultValueType = MapLambdaValidator.mergeNestedNullTypes(
                transformedValuesType.getItemType(), inputMapType.getValueType());
        transformedValuesType = ArrayType.of(resultValueType);
        MapType resultType = MapType.of(inputMapType.getKeyType(), resultValueType);
        resultType.validateDataType();
        return FunctionSignature.ret(resultType).args(inputMapType, transformedValuesType);
    }

    @Override
    public TransformValues withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new TransformValues(getFunctionParams(children));
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        return new MapFromArraysUnique(new MapKeys(getArgument(0)), getArgument(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTransformValues(this, context);
    }
}
