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
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * scalar function ipv4_to_ipv6
 */
public class Ipv4ToIpv6 extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(IPv6Type.INSTANCE).args(IPv4Type.INSTANCE));

    public Ipv4ToIpv6(Expression arg0) {
        super("ipv4_to_ipv6", arg0);
    }

    @Override
    public Ipv4ToIpv6 withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1,
                "ipv4_to_ipv6 accept 1 args, but got %s (%s)",
                children.size(),
                children);
        return new Ipv4ToIpv6(children.get(0));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIpv4ToIpv6(this, context);
    }
}
