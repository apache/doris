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
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Compound predicate expression.
 * Such as AND,OR.
 */
public abstract class CompoundPredicate extends Expression implements ExpectsInputTypes {
    protected final List<Expression> flattenChildren = new ArrayList<>();
    private String symbol;

    public CompoundPredicate(List<Expression> children, String symbol) {
        super(children);
        this.symbol = symbol;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return children.stream().anyMatch(ExpressionTrait::nullable);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCompoundPredicate(this, context);
    }

    @Override
    public List<DataType> expectedInputTypes() {
        return children.stream().map(c -> BooleanType.INSTANCE).collect(Collectors.toList());
    }

    /**
     * Flip logical `and` and `or` operator with original children.
     */
    public abstract CompoundPredicate flip();

    /**
     * Flip logical `and` and `or` operator with new children.
     */
    public abstract CompoundPredicate flip(List<Expression> children);

    public abstract Class<? extends CompoundPredicate> flipType();

    protected abstract List<Expression> extract();

    @Override
    public boolean equals(Object o) {
        if (compareWidthAndDepth) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            List<Expression> thisChildren = this.children();
            List<Expression> thatChildren = ((CompoundPredicate) o).children();
            if (thisChildren.size() != thatChildren.size()) {
                return false;
            }
            for (int i = 0; i < thisChildren.size(); i++) {
                if (!thisChildren.get(i).equals(thatChildren.get(i))) {
                    return false;
                }
            }
            return true;
        } else {
            return super.equals(o);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        children().forEach(c -> sb.append(c.toSql()).append(","));
        sb.deleteCharAt(sb.length() - 1);
        return symbol + "[" + sb + "]";
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        children().forEach(c -> sb.append(c.toString()).append(","));
        sb.deleteCharAt(sb.length() - 1);
        return symbol + "[" + sb + "]";
    }

    @Override
    public String shapeInfo() {
        StringBuilder sb = new StringBuilder();
        children().forEach(c -> sb.append(c.shapeInfo()).append(","));
        sb.deleteCharAt(sb.length() - 1);
        return symbol + "[" + sb + "]";
    }

    @Override
    public int arity() {
        // get flattern children
        return children().size();
    }

    @Override
    public Expression child(int index) {
        return children().get(index);
    }
}
