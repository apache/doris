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
import org.apache.doris.nereids.trees.expressions.ExpressionType;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Joiner;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Expression for unbound function.
 */
public class UnboundFunction extends Expression implements Unbound {

    private final String name;
    private final boolean isDistinct;

    public UnboundFunction(String name, boolean isDistinct, List<Expression> arguments) {
        super(ExpressionType.UNBOUND_FUNCTION, arguments.toArray(new Expression[0]));
        this.name = Objects.requireNonNull(name, "name can not be null");
        this.isDistinct = isDistinct;
    }

    public String getName() {
        return name;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public List<Expression> getArguments() {
        return children();
    }

    @Override
    public String toSql() throws UnboundException {
        String params = children.stream()
                .map(Expression::toSql)
                .collect(Collectors.joining(", "));
        return name + "(" + (isDistinct ? "DISTINCT " : "")  + params + ")";
    }

    @Override
    public String toString() {
        String params = Joiner.on(", ").join(children);
        return "'" + name + "(" + (isDistinct ? "DISTINCT " : "")  + params + ")";
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundFunction(this, context);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        return new UnboundFunction(name, isDistinct, children);
    }
}
