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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Expression for alias, such as col1 as c1.
 */
public class Alias extends NamedExpression implements UnaryExpression {

    private final ExprId exprId;
    private final String name;
    private final List<String> qualifier;

    /**
     * constructor of Alias.
     *
     * @param child expression that alias represents for
     * @param name alias name
     */
    public Alias(Expression child, String name) {
        this(NamedExpressionUtil.newExprId(), child, name);
    }

    @VisibleForTesting
    Alias(ExprId exprId, Expression child, String name) {
        super(child);
        this.exprId = exprId;
        this.name = name;
        this.qualifier = ImmutableList.of();
    }

    @Override
    public Slot toSlot() throws UnboundException {
        return new SlotReference(exprId, name, child().getDataType(), child().nullable(), qualifier);
    }

    @Override
    public String getName() throws UnboundException {
        return name;
    }

    @Override
    public ExprId getExprId() throws UnboundException {
        return exprId;
    }

    @Override
    public List<String> getQualifier() {
        return qualifier;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return child().getDataType();
    }

    @Override
    public String toSql() {
        return child().toSql() + " AS `" + name + "`";
    }

    @Override
    public boolean nullable() throws UnboundException {
        return child().nullable();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Alias that = (Alias) o;
        return exprId.equals(that.exprId)
                && name.equals(that.name)
                && qualifier.equals(that.qualifier)
                && child().equals(that.child());
    }

    @Override
    public int hashCode() {
        return Objects.hash(exprId, name, qualifier, children());
    }

    @Override
    public String toString() {
        return child().toString() + " AS `" + name + "`#" + exprId;
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Alias(exprId, children.get(0), name);
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAlias(this, context);
    }
}
