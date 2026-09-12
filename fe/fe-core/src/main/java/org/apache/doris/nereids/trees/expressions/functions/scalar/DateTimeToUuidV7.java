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
import org.apache.doris.nereids.trees.expressions.VolatileIdentity;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.UuidType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Generate a time-ordered version 7 UUID. */
public class DateTimeToUuidV7 extends UniqueFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNullable, PropagateNullLiteral {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(UuidType.INSTANCE).args(DateTimeV2Type.WILDCARD));

    public DateTimeToUuidV7(Expression argument) {
        this(VolatileIdentity.newVolatileIdentity(), argument);
    }

    private DateTimeToUuidV7(VolatileIdentity volatileIdentity, Expression argument) {
        super("datetime_to_uuid_v7", volatileIdentity, argument);
    }

    private DateTimeToUuidV7(UniqueFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public DateTimeToUuidV7 withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new DateTimeToUuidV7(getFunctionParams(children));
    }

    @Override
    public DateTimeToUuidV7 withIgnoreUniqueId(boolean ignoreUniqueId) {
        return new DateTimeToUuidV7(volatileIdentity.withIgnoreUniqueId(ignoreUniqueId), getArgument(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDateTimeToUuidV7(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
