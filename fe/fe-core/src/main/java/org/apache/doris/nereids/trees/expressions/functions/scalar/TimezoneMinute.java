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
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.TimeStampTzType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'timezone_minute'.
 *
 * <p>Returns the minute part of the UTC offset of the session time zone at the
 * given instant. Note: Doris TIMESTAMPTZ values are stored as UTC instants
 * without the input zone, so unlike Trino's timezone_minute, this function
 * extracts the session time zone offset.</p>
 */
public class TimezoneMinute extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).args(TimeStampTzType.WILDCARD));

    /**
     * constructor with 1 argument.
     */
    public TimezoneMinute(Expression arg) {
        super("timezone_minute", arg);
    }

    /** constructor for withChildren and reuse signature */
    private TimezoneMinute(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public TimezoneMinute withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new TimezoneMinute(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimezoneMinute(this, context);
    }

    @Override
    public boolean isDeterministic() {
        // The result depends on the session time_zone, which may change between
        // executions, so this function must not be folded into prepared plans
        // or used in materialized views.
        return false;
    }
}
