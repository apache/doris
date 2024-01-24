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
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * scalar function IPv4CIDRToRange
 */
public class Ipv4CIDRToRange extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES;

    static {
        ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
        structFields.add(new StructField("min", IPv4Type.INSTANCE, false, ""));
        structFields.add(new StructField("max", IPv4Type.INSTANCE, false, ""));
        StructType retType = new StructType(structFields.build());
        SIGNATURES = ImmutableList.of(
                FunctionSignature.ret(retType).args(IPv4Type.INSTANCE, SmallIntType.INSTANCE));
    }

    public Ipv4CIDRToRange(Expression arg0, Expression arg1) {
        super("ipv4_cidr_to_range", arg0, arg1);
    }

    @Override
    public Ipv4CIDRToRange withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2,
                "ipv4_cidr_to_range accept 2 args, but got %s (%s)",
                children.size(),
                children);
        return new Ipv4CIDRToRange(children.get(0), children.get(1));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIpv4CIDRToRange(this, context);
    }
}
