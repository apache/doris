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
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Scalar function transform_values.
 *
 * <p>The original keys are retained while the Map lambda produces the new value array:
 *
 * <pre>
 * transform_values((mapKey, mapValue) -> newValue, inputMap)
 *   ->
 * %map_from_entries_unique%(
 *   array_map(
 *     entry -> struct(entry[1], newValue(entry[1], entry[2])),
 *     map_entries(inputMap)))
 * </pre>
 */
public class TransformValues extends ScalarFunction
        implements CustomSignature, PropagateNullable, PreferPushDownProject, RewriteWhenAnalyze {

    public TransformValues(Expression arg) {
        this(MapLambdaFunctionUtils.requireLambda("transform_values", arg));
    }

    private TransformValues(Lambda lambda) {
        this(MapLambdaFunctionUtils.rewrite(lambda,
                (body, key, value, entry) -> new CreateStruct(key, body)));
    }

    private TransformValues(MapLambdaFunctionUtils.RewrittenMapLambda rewrittenLambda) {
        super("transform_values",
                rewrittenLambda.getMapExpression(), rewrittenLambda.toArrayMap());
    }

    private TransformValues(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        MapType inputMapType = (MapType) getArgument(0).getDataType();
        ArrayType mappedEntriesType = (ArrayType) getArgument(1).getDataType();
        StructType entryType = (StructType) mappedEntriesType.getItemType();
        List<StructField> fields = entryType.getFields();
        DataType resultValueType = MapLambdaFunctionUtils.mergeNestedNullTypes(
                fields.get(1).getDataType(), inputMapType.getValueType());
        StructType resolvedEntryType = new StructType(ImmutableList.of(
                fields.get(0), fields.get(1).withDataType(resultValueType)));
        MapType resultType = MapType.of(inputMapType.getKeyType(), resultValueType);
        resultType.validateDataType();
        return FunctionSignature.ret(resultType).args(inputMapType, ArrayType.of(resolvedEntryType));
    }

    @Override
    public TransformValues withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new TransformValues(getFunctionParams(children));
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        return new MapFromEntriesUnique(getArgument(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTransformValues(this, context);
    }
}
