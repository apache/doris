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
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Scalar Function 'microsecond_from_unixtime'
 * Optimized version of `MICROSECOND(FROM_UNIXTIME(ts))`
 */
public class MicrosecondFromUnixtime extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(IntegerType.INSTANCE).args(DecimalV3Type.createDecimalV3Type(18, 6))
    );

    /**
     * constructor with 1 argument.
     */
    public MicrosecondFromUnixtime(Expression arg) {
        super("microsecond_from_unixtime", arg);
    }

    /** constructor for withChildren and reuse signature */
    private MicrosecondFromUnixtime(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public MicrosecondFromUnixtime withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MicrosecondFromUnixtime(getFunctionParams(children));
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        // skip super.computeSignature() to avoid changing the decimal precision
        // manually set decimal argument's type to always decimal(18, 6), same with FROM_UNIXTIME
        return FunctionSignature.ret(IntegerType.INSTANCE)
                .args(DecimalV3Type.createDecimalV3Type(18, 6));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMicrosecondFromUnixtime(this, context);
    }

}
