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
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullableOnDateOrTimeLikeV2Args;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'next_day'.
 * next_day(expr, dayOfWeek)
 * - expr: A DATE expression.
 * - dayOfWeek: A STRING expression identifying a day of the week.
 * Returns the first DATE that is later than expr and has the same day of the
 * week as dayOfWeek.
 * dayOfWeek must be one of the following (case insensitive):
 * 'SU', 'SUN', 'SUNDAY'
 * 'MO', 'MON', 'MONDAY'
 * 'TU', 'TUE', 'TUESDAY'
 * 'WE', 'WED', 'WEDNESDAY'
 * 'TH', 'THU', 'THURSDAY'
 * 'FR', 'FRI', 'FRIDAY'
 * 'SA', 'SAT', 'SATURDAY'
 */
public class NextDay extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullableOnDateOrTimeLikeV2Args {
    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DateV2Type.INSTANCE).args(DateV2Type.INSTANCE, StringType.INSTANCE));

    public NextDay(Expression arg0, Expression arg1) {
        super("next_day", arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    private NextDay(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public NextDay withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new NextDay(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNextDay(this, context);
    }
}
