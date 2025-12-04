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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * scalar function ipv6_from_uint128_string_or_null
 * args just accept varchar as uint128 string
 * return ipv6 or null
 * sql : select ipv6_from_uint128_string_or_null('340282366920938463463374607431768211455');
 */
public class Ipv6FromUInt128StringOrNull extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(IPv6Type.INSTANCE).args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(IPv6Type.INSTANCE).args(StringType.INSTANCE));

    public Ipv6FromUInt128StringOrNull(Expression arg0) {
        super("ipv6_from_uint128_string_or_null", arg0);
    }

    /** constructor for withChildren and reuse signature */
    private Ipv6FromUInt128StringOrNull(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public Ipv6FromUInt128StringOrNull withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1,
                "ipv6_from_uint128_string_or_null accept 1 args, but got %s (%s)",
                children.size(),
                children);
        return new Ipv6FromUInt128StringOrNull(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIpv6FromUInt128StringOrNull(this, context);
    }
}
