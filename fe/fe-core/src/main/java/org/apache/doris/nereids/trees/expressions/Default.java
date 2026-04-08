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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Default value expression.
 */
public class Default extends Expression
        implements UnaryExpression, AlwaysNullable {

    public Default(Expression child) {
        super(ImmutableList.of(child));
    }

    /**
     * constructor with 1 argument.
     */
    public Default(Expression arg, DataType targetType) {
        super(ImmutableList.of(arg));
    }

    @Override
    public Default withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Default(children.get(0));
    }

    @Override
    public DataType getDataType() {
        return child().getDataType();
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        Expression arg = child();
        if (!(arg instanceof SlotReference)) {
            throw new AnalysisException("DEFAULT requires a column reference");
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        checkLegalityBeforeTypeCoercion();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDefault(this, context);
    }
}
