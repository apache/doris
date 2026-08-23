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

/** Scalar function transform_keys. */
public class TransformKeys extends ScalarFunction
        implements CustomSignature, PropagateNullable, PreferPushDownProject, RewriteWhenAnalyze {

    public TransformKeys(Expression arg) {
        this(MapLambdaFunctionUtils.requireLambda("transform_keys", arg));
    }

    private TransformKeys(Lambda lambda) {
        this(MapLambdaFunctionUtils.rewrite(lambda,
                (body, key, value, entry) -> new CreateStruct(body, value)));
    }

    private TransformKeys(MapLambdaFunctionUtils.RewrittenMapLambda rewrittenLambda) {
        super("transform_keys",
                rewrittenLambda.getMapExpression(), rewrittenLambda.toArrayMap());
    }

    private TransformKeys(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        MapType inputMapType = (MapType) getArgument(0).getDataType();
        ArrayType mappedEntriesType = (ArrayType) getArgument(1).getDataType();
        StructType entryType = (StructType) mappedEntriesType.getItemType();
        List<StructField> fields = entryType.getFields();
        // transform_keys((k, v) -> null, map(1, 10))
        // res_type should be: MAP<TINYINT,TINYINT> instead of MAP<NULL,TINYINT>
        DataType resultKeyType = MapLambdaFunctionUtils.mergeNestedNullTypes(
                fields.get(0).getDataType(), inputMapType.getKeyType());
        StructType resolvedEntryType = new StructType(ImmutableList.of(
                fields.get(0).withDataType(resultKeyType), fields.get(1)));
        MapType resultType = MapType.of(resultKeyType, inputMapType.getValueType());
        resultType.validateDataType();
        return FunctionSignature.ret(resultType).args(inputMapType, ArrayType.of(resolvedEntryType));
    }

    @Override
    public TransformKeys withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new TransformKeys(getFunctionParams(children));
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        return new MapFromEntries(getArgument(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTransformKeys(this, context);
    }
}
