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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Expression for unbound alias.
 */
public class UnboundAlias extends NamedExpression implements UnaryExpression, Unbound, PropagateNullable {

    private Optional<String> alias;

    public UnboundAlias(Expression child) {
        super(ImmutableList.of(child));
        this.alias = Optional.empty();
    }

    public UnboundAlias(Expression child, String alias) {
        super(ImmutableList.of(child));
        this.alias = Optional.of(alias);
    }

    private UnboundAlias(List<Expression> children, Optional<String> alias) {
        super(children);
        this.alias = alias;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return child().getDataType();
    }

    @Override
    public String computeToSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("(" + child() + ")");
        alias.ifPresent(name -> stringBuilder.append(" AS " + name));
        return stringBuilder.toString();

    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("UnboundAlias(" + child() + ")");
        alias.ifPresent(name -> stringBuilder.append(" AS " + name));
        return stringBuilder.toString();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundAlias(this, context);
    }

    @Override
    public UnboundAlias withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new UnboundAlias(children, alias);
    }

    public Optional<String> getAlias() {
        return alias;
    }
}
