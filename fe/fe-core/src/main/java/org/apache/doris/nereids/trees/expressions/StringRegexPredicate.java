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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * string regex expression.
 * Such as: like, regexp
 */
public abstract class StringRegexPredicate extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullable {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT)
    );

    protected StringRegexPredicate(String name, List<Expression> children) {
        super(name, children);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public String computeToSql() {
        return '(' + left().toSql() + ' ' + getName() + ' ' + right().toSql() + ')';
    }

    @Override
    public String toString() {
        return "(" + left() + " " + getName() + " " + right() + ")";
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStringRegexPredicate(this, context);
    }
}
