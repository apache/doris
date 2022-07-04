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
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * Not expression: not a.
 */
public class Not<CHILD_TYPE extends Expression> extends Expression
        implements UnaryExpression<CHILD_TYPE> {

    public Not(CHILD_TYPE child) {
        super(NodeType.NOT, child);
    }

    @Override
    public boolean nullable() throws UnboundException {
        return child().nullable();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNot(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Not<Expression> other = (Not) o;
        return Objects.equals(child(), other.child());
    }

    @Override
    public String toString() {
        return "( not " + child() + ")";
    }

    @Override
    public Not<Expression> withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Not<>(children.get(0));
    }
}
