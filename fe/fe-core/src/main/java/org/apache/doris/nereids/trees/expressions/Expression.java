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
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Abstract class for all Expression in Nereids.
 */
public abstract class Expression extends AbstractTreeNode<Expression> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public Expression(Expression... children) {
        super(children);
    }

    public DataType getDataType() throws UnboundException {
        throw new UnboundException("dataType");
    }

    public String toSql() throws UnboundException {
        throw new UnboundException("sql");
    }

    public boolean nullable() throws UnboundException {
        throw new UnboundException("nullable");
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
    public List<Expression> children() {
        return children;
    }

    @Override
    public Expression child(int index) {
        return children.get(index);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        throw new RuntimeException();
    }

    public final Expression withChildren(Expression... children) {
        return withChildren(ImmutableList.copyOf(children));
    }

    /**
     * Whether the expression is a constant.
     */
    public boolean isConstant() {
        return children().stream().anyMatch(Expression::isConstant);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Expression that = (Expression) o;
        return Objects.equals(children(), that.children());
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
