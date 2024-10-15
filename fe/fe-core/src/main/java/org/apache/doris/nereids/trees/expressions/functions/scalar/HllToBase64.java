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
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'hll_to_base64'.
 */
public class HllToBase64 extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).args(HllType.INSTANCE)
    );

    /**
     * constructor with 1 argument.
     */
    public HllToBase64(Expression arg) {
        super("hll_to_base64", arg);
    }

    /**
     * withChildren.
     */
    @Override
    public HllToBase64 withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new HllToBase64(children.get(0));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitHllToBase64(this, context);
    }
}
