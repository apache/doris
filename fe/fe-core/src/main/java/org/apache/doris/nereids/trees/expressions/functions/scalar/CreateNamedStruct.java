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
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.PreserveChildTypePrecision;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * ScalarFunction 'named_struct'.
 */
public class CreateNamedStruct extends ScalarFunction
        implements CustomSignature, PreserveChildTypePrecision, AlwaysNotNullable, ChildDerivedSignature {

    public static final FunctionSignature SIGNATURE = FunctionSignature.ret(StructType.SYSTEM_DEFAULT).args();

    /**
     * constructor with 0 or more arguments.
     */
    public CreateNamedStruct(Expression... varArgs) {
        super("named_struct", varArgs);
    }

    /** constructor for withChildren and reuse signature */
    private CreateNamedStruct(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (arity() < 2) {
            throw new AnalysisException("named_struct requires at least two arguments, like: named_struct('a', 1)");
        }
        if (arity() % 2 != 0) {
            throw new AnalysisException("named_struct can't be odd parameters, need even parameters " + this.toSql());
        }
        Set<String> names = Sets.newHashSet();
        for (int i = 0; i < arity(); i = i + 2) {
            if (!(child(i) instanceof StringLikeLiteral)) {
                throw new AnalysisException("named_struct only allows"
                        + " constant string parameter in odd position: " + this);
            } else {
                String name = ((StringLikeLiteral) child(i)).getStringValue().toLowerCase(Locale.ROOT);
                if (names.contains(name)) {
                    throw new AnalysisException("The name of the struct field cannot be repeated."
                            + " same name fields are " + name);
                } else {
                    names.add(name);
                }
            }
            // i+1 is value, check if it is not jsonb/variant type
            if (child(i + 1).getDataType().isJsonType() || child(i + 1).getDataType().isVariantType()) {
                throw new AnalysisException("named_struct does not support jsonb/variant type");
            }
        }
    }

    /**
     * withChildren.
     */
    @Override
    public CreateNamedStruct withChildren(List<Expression> children) {
        return new CreateNamedStruct(getFunctionParams(children));
    }

    @Override
    public FunctionSignature customSignature() {
        if (arity() == 0) {
            return SIGNATURE;
        } else {
            return FunctionSignature.ret(computeStructType(children))
                    .args(children.stream().map(ExpressionTrait::getDataType).toArray(DataType[]::new));
        }
    }

    @Override
    public FunctionSignature deriveSignatureFromChildren(
            FunctionSignature resolvedSignature, List<Expression> immediateOriginArguments,
            List<Expression> currentArguments) {
        if (currentArguments.size() % 2 != 0) {
            throw new AnalysisException("Cannot safely refresh named_struct with an odd argument count");
        }
        ImmutableList.Builder<DataType> argumentTypeBuilder = ImmutableList.builderWithExpectedSize(
                currentArguments.size());
        for (int i = 0; i < currentArguments.size(); i++) {
            DataType currentType = currentArguments.get(i).getDataType();
            boolean existingPosition = i < immediateOriginArguments.size();
            // Name literals define field metadata rather than scalar payload. New value positions have no prior
            // binding; existing value positions must retain the resolved or immediate-origin leaf.
            argumentTypeBuilder.add(i % 2 == 0 || !existingPosition
                    ? currentType
                    : ChildDerivedSignature.refreshNestedTypeMetadata(
                            resolvedSignature.getArgType(i), currentType,
                            immediateOriginArguments.get(i).getDataType()));
        }
        ImmutableList<DataType> argumentTypes = argumentTypeBuilder.build();
        StructType currentReturnType = computeStructType(currentArguments);
        ImmutableList.Builder<StructField> fieldBuilder = ImmutableList.builderWithExpectedSize(
                currentArguments.size() / 2);
        for (int i = 0; i < currentReturnType.getFields().size(); i++) {
            StructField currentField = currentReturnType.getFields().get(i);
            fieldBuilder.add(currentField.withDataTypeAndNullable(
                    argumentTypes.get(i * 2 + 1), currentField.isNullable()));
        }
        return resolvedSignature.withArgumentTypes(false, argumentTypes)
                .withReturnType(new StructType(fieldBuilder.build()));
    }

    private StructType computeStructType(List<Expression> arguments) {
        ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
        for (int i = 0; i < arguments.size(); i = i + 2) {
            StringLikeLiteral nameLiteral = (StringLikeLiteral) arguments.get(i);
            // A named struct has the same value-nullability contract as struct(...); keeping
            // the field nullable here would reject safe casts into required target fields.
            structFields.add(new StructField(nameLiteral.getStringValue(),
                    arguments.get(i + 1).getDataType(),
                    StructLiteral.computeFieldNullable(arguments.get(i + 1)), ""));
        }
        return new StructType(structFields.build());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCreateNamedStruct(this, context);
    }

}
