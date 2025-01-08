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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * expr is null predicate.
 */
public class IsNull extends Expression implements UnaryExpression, AlwaysNotNullable {

    public IsNull(Expression e) {
        super(ImmutableList.of(e));
    }

    private IsNull(List<Expression> children) {
        super(children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIsNull(this, context);
    }

    @Override
    public IsNull withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new IsNull(children);
    }

    @Override
    public String computeToSql() throws UnboundException {
        return child().toSql() + " IS NULL";
    }

    @Override
    public String toString() {
        return child().toString() + " IS NULL";
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        IsNull other = (IsNull) o;
        return Objects.equals(child(), other.child());
    }

    @Override
    public int hashCode() {
        return child().hashCode();
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }
}
