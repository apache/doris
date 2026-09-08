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
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TimeStampNsType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** ScalarFunction 'nanosecond'. */
public class Nanosecond extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable, Monotonic {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(IntegerType.INSTANCE).args(DateTimeV2Type.WILDCARD),
            FunctionSignature.ret(IntegerType.INSTANCE).args(DateV2Type.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).args(TimeStampNsType.INSTANCE)
    );

    public Nanosecond(Expression arg) {
        super("nanosecond", arg);
    }

    /** Constructor for withChildren and signature reuse. */
    private Nanosecond(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public Nanosecond withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Nanosecond(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNanosecond(this, context);
    }

    @Override
    public boolean isPositive() {
        return true;
    }

    @Override
    public int getMonotonicFunctionChildIndex() {
        return 0;
    }

    @Override
    public Expression withConstantArgs(Expression literal) {
        return new Nanosecond(literal);
    }

    @Override
    public boolean isMonotonic(Literal lower, Literal upper) {
        if (lower instanceof TimeStampNsLiteral && upper instanceof TimeStampNsLiteral) {
            TimeStampNsLiteral lowerTimestamp = (TimeStampNsLiteral) lower;
            TimeStampNsLiteral upperTimestamp = (TimeStampNsLiteral) upper;
            return lowerTimestamp.getYear() == upperTimestamp.getYear()
                    && lowerTimestamp.getMonth() == upperTimestamp.getMonth()
                    && lowerTimestamp.getDay() == upperTimestamp.getDay()
                    && lowerTimestamp.getHour() == upperTimestamp.getHour()
                    && lowerTimestamp.getMinute() == upperTimestamp.getMinute()
                    && lowerTimestamp.getSecond() == upperTimestamp.getSecond();
        }
        if (lower instanceof DateTimeLiteral && upper instanceof DateTimeLiteral) {
            DateTimeLiteral lowerDateTime = (DateTimeLiteral) lower;
            DateTimeLiteral upperDateTime = (DateTimeLiteral) upper;
            return lowerDateTime.getYear() == upperDateTime.getYear()
                    && lowerDateTime.getMonth() == upperDateTime.getMonth()
                    && lowerDateTime.getDay() == upperDateTime.getDay()
                    && lowerDateTime.getHour() == upperDateTime.getHour()
                    && lowerDateTime.getMinute() == upperDateTime.getMinute()
                    && lowerDateTime.getSecond() == upperDateTime.getSecond();
        }
        return lower instanceof DateLiteral && upper instanceof DateLiteral;
    }
}
