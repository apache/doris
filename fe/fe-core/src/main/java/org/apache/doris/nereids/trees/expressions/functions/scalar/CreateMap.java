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
            FunctionSignature resolvedSignature, List<Expression> immediateOriginArguments) {
        if (arity() % 2 != 0) {
            throw new AnalysisException("Cannot safely refresh map with an odd argument count");
        }
        if (arity() != immediateOriginArguments.size()
                || arity() != resolvedSignature.argumentsTypes.size()) {
            throw new AnalysisException(
                    "Cannot safely reuse map signature after changing its argument count");
        }
        if (arity() == 0) {
            return resolvedSignature;
        }
        if (!(resolvedSignature.returnType instanceof MapType)) {
            throw new AnalysisException("Cannot safely reuse map signature with a non-map return type");
        }
        List<DataType> currentKeyTypes = new ArrayList<>(arity() / 2);
        List<DataType> currentValueTypes = new ArrayList<>(arity() / 2);
        List<DataType> originKeyTypes = new ArrayList<>(arity() / 2);
        List<DataType> originValueTypes = new ArrayList<>(arity() / 2);
        for (int i = 0; i < arity(); i += 2) {
            currentKeyTypes.add(getArgument(i).getDataType());
            currentValueTypes.add(getArgument(i + 1).getDataType());
            originKeyTypes.add(immediateOriginArguments.get(i).getDataType());
            originValueTypes.add(immediateOriginArguments.get(i + 1).getDataType());
        }
        MapType resolvedMapType = (MapType) resolvedSignature.returnType;
        DataType keyType = ChildDerivedSignature.mergeNestedTypeMetadata(
                resolvedMapType.getKeyType(), currentKeyTypes, originKeyTypes)
                .orElseThrow(() -> new AnalysisException(
                        "Cannot safely reuse map signature with incompatible key metadata"));
        DataType valueType = ChildDerivedSignature.mergeNestedTypeMetadata(
                resolvedMapType.getValueType(), currentValueTypes, originValueTypes)
                .orElseThrow(() -> new AnalysisException(
                        "Cannot safely reuse map signature with incompatible value metadata"));
        ImmutableList.Builder<DataType> argumentTypes = ImmutableList.builderWithExpectedSize(arity());
        for (int i = 0; i < arity(); i++) {
            argumentTypes.add(i % 2 == 0 ? keyType : valueType);
        }
        return resolvedSignature.withArgumentTypes(false, argumentTypes.build())
                .withReturnType(MapType.of(keyType, valueType));
    }
}
