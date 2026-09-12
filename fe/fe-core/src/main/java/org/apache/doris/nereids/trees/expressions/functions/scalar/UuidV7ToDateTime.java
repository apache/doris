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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** UUID conversion function uuid_v7_to_datetime. */
public class UuidV7ToDateTime extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNullable, PropagateNullLiteral {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DateTimeV2Type.of(3)).args(UuidType.INSTANCE),
            FunctionSignature.ret(DateTimeV2Type.of(3)).args(UuidType.INSTANCE, VarcharType.SYSTEM_DEFAULT));

    public UuidV7ToDateTime(Expression arg0) {
        super("uuid_v7_to_datetime", arg0);
    }

    public UuidV7ToDateTime(Expression arg0, Expression arg1) {
        super("uuid_v7_to_datetime", arg0, arg1);
    }

    private UuidV7ToDateTime(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public UuidV7ToDateTime withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1 && children.size() <= 2);
        return new UuidV7ToDateTime(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUuidV7ToDateTime(this, context);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (arity() == 2 && !getArgument(1).isConstant()) {
            throw new AnalysisException("UUIDv7ToDateTime timezone must be constant");
        }
    }
}
