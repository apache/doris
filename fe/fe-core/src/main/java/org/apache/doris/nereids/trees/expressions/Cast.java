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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * cast function.
 */
public class Cast extends Expression implements BinaryExpression {

    public Cast(Expression child, String type) {
        super(child, new StringLiteral(type));
    }

    public Cast(Expression child, StringLiteral type) {
        super(child, type);
    }

    @Override
    public StringLiteral right() {
        return (StringLiteral) BinaryExpression.super.right();
    }

    @Override
    public DataType getDataType() {
        StringLiteral type = right();
        return DataType.convertFromString(type.getValue());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCast(this, context);
    }

    @Override
    public boolean nullable() {
        return left().nullable();
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        Preconditions.checkArgument(children.get(1) instanceof StringLiteral);
        return new Cast(children.get(0), ((StringLiteral) children.get(1)).getValue());
    }

    @Override
    public String toSql() throws UnboundException {
        return "CAST(" + left().toSql() + " AS " + right().getValue() + ")";
    }

    @Override
    public String toString() {
        return toSql();
    }
}
