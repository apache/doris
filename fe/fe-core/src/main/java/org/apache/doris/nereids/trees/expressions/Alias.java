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
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Expression for alias, such as col1 as c1.
 */
public class Alias extends NamedExpression implements UnaryExpression {

    private final ExprId exprId;
    private final Supplier<String> name;
    private final List<String> qualifier;
    private final boolean nameFromChild;

    /**
     * constructor of Alias.
     *
     * @param child expression that alias represents for
     * @param name alias name
     */
    public Alias(Expression child, String name) {
        this(StatementScopeIdGenerator.newExprId(), child, name, false);
    }

    public Alias(Expression child) {
        this(StatementScopeIdGenerator.newExprId(), ImmutableList.of(child),
                Suppliers.memoize(child::toSql), ImmutableList.of(), true);
    }

    public Alias(ExprId exprId, Expression child, String name) {
        this(exprId, ImmutableList.of(child), name, ImmutableList.of(), false);
    }

    public Alias(ExprId exprId, Expression child, String name, boolean nameFromChild) {
        this(exprId, ImmutableList.of(child), name, ImmutableList.of(), nameFromChild);
    }

    public Alias(ExprId exprId, List<Expression> child, String name, List<String> qualifier, boolean nameFromChild) {
        this(exprId, child, Suppliers.memoize(() -> name), qualifier, nameFromChild);
    }

    private Alias(ExprId exprId, List<Expression> child, Supplier<String> name,
            List<String> qualifier, boolean nameFromChild) {
        super(child);
        this.exprId = exprId;
        this.name = name;
        this.qualifier = qualifier;
        this.nameFromChild = nameFromChild;
    }

    @Override
    public Slot toSlot() throws UnboundException {
        SlotReference slotReference = child() instanceof SlotReference
                ? (SlotReference) child() : null;

        Supplier<Optional<String>> internalName = nameFromChild
                        ? Suppliers.memoize(() -> Optional.of(child().toString()))
                        : () -> Optional.of(name.get());
        return new SlotReference(exprId, name, child().getDataType(), child().nullable(), qualifier,
                slotReference != null
                        ? ((SlotReference) child()).getTable().orElse(null)
                        : null,
                slotReference != null
                        ? slotReference.getColumn().orElse(null)
                        : null,
                internalName,
                slotReference != null
                        ? slotReference.getSubPath()
                        : ImmutableList.of(), Optional.empty()
        );
    }

    @Override
    public String getName() throws UnboundException {
        return name.get();
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
        return child().toSql() + " AS `" + name.get() + "`";
    }

    @Override
    public boolean nullable() throws UnboundException {
        return child().nullable();
    }

    @Override
    protected boolean extraEquals(Expression other) {
        Alias that = (Alias) other;
        if (!exprId.equals(that.exprId) || !qualifier.equals(that.qualifier)) {
            return false;
        }

        return nameFromChild || name.get().equals(that.name.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(exprId, qualifier);
    }

    @Override
    public String toString() {
        return child().toString() + " AS `" + name.get() + "`#" + exprId;
    }

    @Override
    public Alias withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        if (nameFromChild) {
            return new Alias(exprId, children,
                    Suppliers.memoize(() -> children.get(0).toSql()), qualifier, nameFromChild);
        } else {
            return new Alias(exprId, children, name, qualifier, nameFromChild);
        }
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAlias(this, context);
    }

    public boolean isNameFromChild() {
        return nameFromChild;
    }
}
