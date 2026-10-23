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
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/** Parse JSON text into a Variant value. */
public class ParseToVariant extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(VariantType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT)
    );

    private final VariantType returnType;

    public ParseToVariant(Expression argument) {
        this(argument, VariantType.INSTANCE);
    }

    /**
     * A parse typed with a specific VARIANT type. When that type carries a Schema Template, the BE parses
     * the text with the template's DECIMAL paths in hand (see VariantType#toThrift).
     */
    public ParseToVariant(Expression argument, VariantType returnType) {
        super("parse_to_variant", argument);
        this.returnType = returnType;
    }

    private ParseToVariant(ScalarFunctionParams functionParams, VariantType returnType) {
        super(functionParams);
        this.returnType = returnType;
    }

    /** The same call typed with {@code type}; the signature is recomputed for the new return type. */
    public ParseToVariant withReturnType(VariantType type) {
        return new ParseToVariant(child(), type);
    }

    public VariantType getReturnType() {
        return returnType;
    }

    @Override
    public ParseToVariant withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new ParseToVariant(getFunctionParams(children), returnType);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(FunctionSignature.ret(returnType).args(VarcharType.SYSTEM_DEFAULT));
    }

    @Override
    protected boolean extraEquals(Expression that) {
        return super.extraEquals(that) && returnType.equals(((ParseToVariant) that).returnType);
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(super.computeHashCode(), returnType);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScalarFunction(this, context);
    }
}
