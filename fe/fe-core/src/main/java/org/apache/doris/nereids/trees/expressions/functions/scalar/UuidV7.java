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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.UuidType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Generate a time-ordered version 7 UUID. */
public class UuidV7 extends UniqueFunction
        implements LeafExpression, ExplicitlyCastableSignature, AlwaysNotNullable {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(UuidType.INSTANCE).args());

    public UuidV7() {
        this(VolatileIdentity.newVolatileIdentity());
    }

    private UuidV7(VolatileIdentity volatileIdentity) {
        super("uuid_v7", volatileIdentity);
    }

    private UuidV7(UniqueFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public UuidV7 withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.isEmpty());
        return new UuidV7(getFunctionParams(children));
    }

    @Override
    public UuidV7 withIgnoreUniqueId(boolean ignoreUniqueId) {
        return new UuidV7(volatileIdentity.withIgnoreUniqueId(ignoreUniqueId));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUuidV7(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
