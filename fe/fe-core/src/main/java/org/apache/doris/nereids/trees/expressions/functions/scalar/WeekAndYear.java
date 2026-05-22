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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'week_and_year'.
 */
public class WeekAndYear extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE,
                StringType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE,
                StringType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE)
    );

    /**
     * constructor with 1 argument.
     */
    public WeekAndYear(Expression dateStr) {
        super("week_and_year", dateStr);
    }

    /**
     * constructor with 2 arguments.
     */
    public WeekAndYear(Expression dateStr, Expression formatStr) {
        super("week_and_year", dateStr, formatStr);
    }

    /**
     * constructor with 3 arguments.
     */
    public WeekAndYear(Expression dateStr, Expression formatStr, Expression firstDay) {
        super("week_and_year", dateStr, formatStr, firstDay);
    }

    /**
     * constructor with 4 arguments.
     */
    public WeekAndYear(Expression dateStr, Expression formatStr, Expression firstDay, Expression minDays) {
        super("week_and_year", dateStr, formatStr, firstDay, minDays);
    }

    /** constructor for withChildren and reuse signature */
    private WeekAndYear(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public WeekAndYear withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1 && children.size() <= 4);
        return new WeekAndYear(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitWeekAndYear(this, context);
    }
}
