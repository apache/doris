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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Window function: Lead()
 */
public class Lead extends WindowFunction implements TernaryExpression, PropagateNullable, ExplicitlyCastableSignature {

    public Lead(Expression child) {
        this(child, Literal.of(1), Literal.of(null));
    }

    public Lead(Expression child, Expression offset) {
        this(child, offset, Literal.of(null));
    }

    public Lead(Expression child, Expression offset, Expression defaultValue) {
        super("lead", child, offset, defaultValue);
    }

    public Lead(List<Expression> children) {
        super("lead", children);
    }

    public Expression getOffset() {
        return child(1);
    }

    public Expression getDefaultValue() {
        return child(2);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLead(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return ImmutableList.of(FunctionSignature.ret(getArgument(0).getDataType())
            .args(getArgument(0).getDataType(), IntegerType.INSTANCE, getArgument(0).getDataType()));
    }

    @Override
    public Lead withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1 && children.size() <= 3);
        return new Lead(children);
    }

    @Override
    public DataType getDataType() {
        return child(0).getDataType();
    }
}
