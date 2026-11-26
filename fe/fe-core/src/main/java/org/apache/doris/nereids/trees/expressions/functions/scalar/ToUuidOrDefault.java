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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** UUID conversion function to_uuid_or_default. */
public class ToUuidOrDefault extends ScalarFunction implements ExplicitlyCastableSignature {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(UuidType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(UuidType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT, UuidType.INSTANCE));

    public ToUuidOrDefault(Expression arg0) {
        super("to_uuid_or_default", arg0);
    }

    public ToUuidOrDefault(Expression arg0, Expression arg1) {
        super("to_uuid_or_default", arg0, arg1);
    }

    private ToUuidOrDefault(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public ToUuidOrDefault withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1 && children.size() <= 2);
        return new ToUuidOrDefault(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitToUuidOrDefault(this, context);
    }

    @Override
    public boolean nullable() {
        return arity() == 2 && getArgument(1).nullable();
    }
}
