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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ChildDerivedSignature;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * ScalarFunction 'map'.
 */
public class CreateMap extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNotNullable, ChildDerivedSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(MapType.SYSTEM_DEFAULT).args()
    );

    /**
     * constructor with 0 or more arguments.
     */
    public CreateMap(Expression... varArgs) {
        super("map", varArgs);
    }

    /** constructor for withChildren and reuse signature */
    private CreateMap(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (arity() % 2 != 0) {
            throw new AnalysisException("map can't be odd parameters, need even parameters " + this.toSql());
        }
        children.forEach(child -> {
            if (child.getDataType().isJsonType() || child.getDataType().isVariantType()) {
                throw new AnalysisException("map does not support jsonb/variant type");
            }
        });
    }

    /**
     * withChildren.
     */
    @Override
    public CreateMap withChildren(List<Expression> children) {
        return new CreateMap(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMap(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (arity() == 0) {
            return SIGNATURES;
        } else {
            List<Expression> keys = Lists.newArrayList();
            List<Expression> values = Lists.newArrayList();
            for (int i = 0; i < arity(); i++) {
                if (i % 2 == 0) {
                    keys.add(child(i));
                } else {
                    values.add(child(i));
                }
            }
            // TODO: use the find common type to get key and value type after we redefine type coercion in Doris.
            Array keyArray = new Array(keys.toArray(new Expression[0]));
            Array valueArray = new Array(values.toArray(new Expression[0]));
            keyArray = (Array) TypeCoercionUtils.implicitCastInputTypes(keyArray, keyArray.expectedInputTypes());
            valueArray = (Array) TypeCoercionUtils.implicitCastInputTypes(valueArray, valueArray.expectedInputTypes());
            DataType keyType = ((ArrayType) (keyArray.getDataType())).getItemType();
            DataType valueType = ((ArrayType) (valueArray.getDataType())).getItemType();
            ImmutableList.Builder<DataType> childTypes = ImmutableList.builder();
            for (int i = 0; i < arity(); i++) {
                if (i % 2 == 0) {
                    childTypes.add(keyType);
                } else {
                    childTypes.add(valueType);
                }
            }
            return ImmutableList.of(FunctionSignature.of(
                    MapType.of(keyType, valueType),
                    childTypes.build())
            );
        }
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        return signature;
    }

    @Override
    public FunctionSignature deriveSignatureFromChildren(
            FunctionSignature resolvedSignature, List<Expression> immediateOriginArguments,
            List<Expression> currentArguments) {
        int argumentCount = currentArguments.size();
        if (argumentCount % 2 != 0) {
            throw new AnalysisException("Cannot safely refresh map with an odd argument count");
        }
        if (argumentCount != immediateOriginArguments.size()
                || argumentCount != resolvedSignature.argumentsTypes.size()) {
            throw new AnalysisException(
                    "Cannot safely reuse map signature after changing its argument count");
        }
        if (argumentCount == 0) {
            return resolvedSignature;
        }
        if (!(resolvedSignature.returnType instanceof MapType)) {
            throw new AnalysisException("Cannot safely reuse map signature with a non-map return type");
        }
        MapType resolvedMapType = (MapType) resolvedSignature.returnType;
        List<DataType> currentKeyTypes = new ArrayList<>(argumentCount / 2);
        List<DataType> currentValueTypes = new ArrayList<>(argumentCount / 2);
        List<DataType> originKeyTypes = new ArrayList<>(argumentCount / 2);
        List<DataType> originValueTypes = new ArrayList<>(argumentCount / 2);
        for (int i = 0; i < argumentCount; i += 2) {
            DataType originKeyType = immediateOriginArguments.get(i).getDataType();
            DataType originValueType = immediateOriginArguments.get(i + 1).getDataType();
            currentKeyTypes.add(normalizeFoldedStringType(
                    resolvedMapType.getKeyType(), currentArguments.get(i).getDataType(), originKeyType));
            currentValueTypes.add(normalizeFoldedStringType(
                    resolvedMapType.getValueType(), currentArguments.get(i + 1).getDataType(), originValueType));
            originKeyTypes.add(originKeyType);
            originValueTypes.add(originValueType);
        }
        DataType keyType = ChildDerivedSignature.mergeNestedTypeMetadata(
                resolvedMapType.getKeyType(), currentKeyTypes, originKeyTypes)
                .orElseThrow(() -> new AnalysisException(
                        "Cannot safely reuse map signature with incompatible key metadata"));
        DataType valueType = ChildDerivedSignature.mergeNestedTypeMetadata(
                resolvedMapType.getValueType(), currentValueTypes, originValueTypes)
                .orElseThrow(() -> new AnalysisException(
                        "Cannot safely reuse map signature with incompatible value metadata"));
        ImmutableList.Builder<DataType> argumentTypes = ImmutableList.builderWithExpectedSize(argumentCount);
        for (int i = 0; i < argumentCount; i++) {
            argumentTypes.add(i % 2 == 0 ? keyType : valueType);
        }
        return resolvedSignature.withArgumentTypes(false, argumentTypes.build())
                .withReturnType(MapType.of(keyType, valueType));
    }

    private static DataType normalizeFoldedStringType(
            DataType resolvedType, DataType currentType, DataType originType) {
        // Constant folding can replace CAST('world' AS VARCHAR(10)) with a VARCHAR(5)
        // literal. All string-like types have the same physical payload here, so retain
        // the frozen resolved binding without relaxing Decimal or temporal precision.
        return resolvedType.isStringLikeType()
                && currentType.isStringLikeType()
                && originType.isStringLikeType() ? originType : currentType;
    }
}
