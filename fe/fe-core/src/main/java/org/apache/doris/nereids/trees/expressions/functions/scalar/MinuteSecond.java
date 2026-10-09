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
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'minute_second'.
 */
public class MinuteSecond extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable, RewriteWhenAnalyze {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(DateTimeV2Type.WILDCARD),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(TimeStampNsType.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(TimeV2Type.WILDCARD));

    public MinuteSecond(Expression arg) {
        super("minute_second", arg);
    }

    private MinuteSecond(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public MinuteSecond withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MinuteSecond(getFunctionParams(children));
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        Expression argument = getArgument(0);
        if (!(argument.getDataType() instanceof TimeV2Type)) {
            return this;
        }
        return TypeCoercionUtils.processBoundFunction(new TimeFormat(argument, new VarcharLiteral("%i:%s")));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMinuteSecond(this, context);
    }
}
