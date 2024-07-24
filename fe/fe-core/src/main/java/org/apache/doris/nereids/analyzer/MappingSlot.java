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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * MappingSlot.
 * mapping slot to an expression, use to replace slot to this expression.
 * this class only use in Scope, and **NEVER** appear in the expression tree
 */
public class MappingSlot extends Slot {
    private final Slot slot;
    private final Expression mappingExpression;

    public MappingSlot(Slot slot, Expression mappingExpression) {
        super(Optional.empty());
        this.slot = slot;
        this.mappingExpression = mappingExpression;
    }

    public Slot getRealSlot() {
        return slot;
    }

    @Override
    public List<String> getQualifier() {
        return slot.getQualifier();
    }

    public Expression getMappingExpression() {
        return mappingExpression;
    }

    @Override
    public ExprId getExprId() {
        return slot.getExprId();
    }

    @Override
    public String getName() throws UnboundException {
        return slot.getName();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSlot(this, context);
    }

    @Override
    public boolean nullable() {
        return slot.nullable();
    }

    @Override
    public String toSql() {
        return slot.toSql();
    }

    @Override
    public String toString() {
        return slot.toString();
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return slot.getDataType();
    }

    @Override
    public String getInternalName() {
        return slot.getInternalName();
    }

    @Override
    public Slot withName(String name) {
        return this;
    }

    @Override
    public Slot withNullable(boolean nullable) {
        return this;
    }

    @Override
    public Slot withNullableAndDataType(boolean nullable, DataType dataType) {
        return this;
    }

    @Override
    public Slot withExprId(ExprId exprId) {
        return this;
    }

    @Override
    public Slot withQualifier(List<String> qualifier) {
        return this;
    }

    @Override
    public Slot withIndexInSql(Pair<Integer, Integer> index) {
        return this;
    }
}
