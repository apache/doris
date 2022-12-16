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

import java.util.List;
import java.util.Objects;

/**
 * cast function.
 */
public class Cast extends Expression implements UnaryExpression {

    private final DataType targetType;

    public Cast(Expression child, DataType targetType) {
        super(child);
        this.targetType = Objects.requireNonNull(targetType, "targetType can not be null");
    }

    @Override
    public DataType getDataType() {
        return targetType;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCast(this, context);
    }

    @Override
    public boolean nullable() {
        return child().nullable();
    }

    @Override
    public Cast withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Cast(children.get(0), getDataType());
    }

    @Override
    public String toSql() throws UnboundException {
        return "cast(" + child().toSql() + " as " + targetType + ")";
    }

    @Override
    public String toString() {
        return "cast(" + child() + " as " + targetType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        Cast cast = (Cast) o;
        return Objects.equals(targetType, cast.targetType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), targetType);
    }
}
