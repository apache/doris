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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.GeographyType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'st_dwithin'.
 */
public class StDWithin extends ScalarFunction
        implements TernaryExpression, ExplicitlyCastableSignature, AlwaysNullable, PropagateNullLiteral {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(new GeographyType("OGC:CRS84", "spherical"),
                            new GeographyType("OGC:CRS84", "spherical"), DoubleType.INSTANCE)
    );

    /**
     * constructor with 3 arguments.
     */
    public StDWithin(Expression arg0, Expression arg1, Expression arg2) {
        super("st_dwithin", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private StDWithin(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public StDWithin withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new StDWithin(getFunctionParams(children));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        SpatialFunctionSignature.checkSpatialArguments(getName(), getArgument(0), getArgument(1));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        FunctionSignature signature = SpatialFunctionSignature.calculationBinary(
                child(0), child(1), BooleanType.INSTANCE);
        return signature != null
                ? FunctionSignature.ret(BooleanType.INSTANCE).args(signature.getArgType(0), signature.getArgType(1),
                        DoubleType.INSTANCE)
                : ExplicitlyCastableSignature.super.searchSignature(signatures);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStDWithin(this, context);
    }
}
