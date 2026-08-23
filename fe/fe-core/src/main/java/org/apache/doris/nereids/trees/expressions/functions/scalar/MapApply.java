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
 * Scalar function map_apply.
 *
 * <p>The lambda produces a two-field Struct for each Map entry. After analysis, the mapped entry
 * array is converted directly to Map:
 *
 * <pre>
 * map_apply((mapKey, mapValue) -> struct(newKey, newValue), inputMap)
 *   ->
 * map_from_entries(array_map(
 *   entry -> struct(newKey(entry[1], entry[2]), newValue(entry[1], entry[2])),
 *   map_entries(inputMap)))
 * </pre>
 */
public class MapApply extends ScalarFunction
        implements CustomSignature, PropagateNullable, PreferPushDownProject,
        RewriteWhenAnalyze {

    public MapApply(Expression arg) {
        this(MapLambdaFunctionUtils.requireLambda("map_apply", arg));
    }

    private MapApply(Lambda lambda) {
        this(validateAndRewrite(lambda));
    }

    private MapApply(MapLambdaFunctionUtils.RewrittenMapLambda rewrittenLambda) {
        super("map_apply", rewrittenLambda.getMapExpression(), rewrittenLambda.toArrayMap());
    }

    private MapApply(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        DataType mappedEntriesType = getArgument(1).getDataType();
        if (!(mappedEntriesType instanceof ArrayType)
                || !(((ArrayType) mappedEntriesType).getItemType() instanceof StructType)) {
            throw invalidReturnType();
        }
        StructType structType = (StructType) ((ArrayType) mappedEntriesType).getItemType();
        if (structType.getFields().size() != 2) {
            throw invalidReturnType();
        }
        MapType inputMapType = (MapType) getArgument(0).getDataType();
        StructType resolvedStructType = resolveNullFieldTypes(structType, inputMapType);
        List<StructField> fields = resolvedStructType.getFields();
        MapType resultType = MapType.of(fields.get(0).getDataType(), fields.get(1).getDataType());
        resultType.validateDataType();
        return FunctionSignature.ret(resultType).args(inputMapType, ArrayType.of(resolvedStructType));
    }

    @Override
    public MapApply withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new MapApply(getFunctionParams(children));
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        return new MapFromEntries(getArgument(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapApply(this, context);
    }

    private static MapLambdaFunctionUtils.RewrittenMapLambda validateAndRewrite(Lambda lambda) {
        MapLambdaFunctionUtils.RewrittenMapLambda rewrittenLambda = MapLambdaFunctionUtils.rewrite(
                lambda, (body, key, value, entry) -> body);
        validateLambdaReturn(lambda, (MapType) rewrittenLambda.getMapExpression().getDataType());
        return rewrittenLambda;
    }

    private static void validateLambdaReturn(Lambda lambda, MapType inputMapType) {
        Expression lambdaBody = lambda.getLambdaFunction();
        if (!(lambdaBody.getDataType() instanceof StructType)
                || ((StructType) lambdaBody.getDataType()).getFields().size() != 2
                || lambdaBody.nullable()) {
            throw invalidReturnType();
        }
        StructType structType = (StructType) lambdaBody.getDataType();
        StructType resolvedStructType = resolveNullFieldTypes(structType, inputMapType);
        MapType.of(resolvedStructType.getFields().get(0).getDataType(),
                resolvedStructType.getFields().get(1).getDataType()).validateDataType();
    }

    // Resolve only untyped fields in the two-field struct returned by the lambda. For
    // map_apply((k, v) -> struct(cast(k as bigint), []), map(1, [10])), the result is
    // MAP<BIGINT, ARRAY<TINYINT>>. Keep the explicit BIGINT type and infer only the empty array
    // from the input value type.
    private static StructType resolveNullFieldTypes(StructType structType, MapType inputMapType) {
        List<StructField> fields = structType.getFields();
        StructField keyField = fields.get(0);
        StructField valueField = fields.get(1);
        keyField = keyField.withDataType(MapLambdaFunctionUtils.mergeNestedNullTypes(
                keyField.getDataType(), inputMapType.getKeyType()));
        valueField = valueField.withDataType(MapLambdaFunctionUtils.mergeNestedNullTypes(
                valueField.getDataType(), inputMapType.getValueType()));
        return new StructType(ImmutableList.of(keyField, valueField));
    }

    private static AnalysisException invalidReturnType() {
        return new AnalysisException(
                "Lambda of map_apply must return a non-nullable struct with exactly two fields");
    }
}
