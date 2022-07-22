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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The internal representation of
 * CASE [expr] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
 * Each When/Then is stored as two consecutive children (whenExpr, thenExpr).
 * If a case expr is given, convert it to equalTo(caseExpr, whenExpr) and set it to whenExpr.
 * If an else expr is given then it is the last child.
 */
public class CaseWhen extends Expression {
    /**
     * If default value exists, then defaultValueIndex is the index of the last element in children,
     * otherwise it is -1
     */
    private final int defaultValueIndex;

    public CaseWhen(List<WhenClause> whenClauses) {
        super(whenClauses.toArray(new Expression[0]));
        defaultValueIndex = -1;
    }

    public CaseWhen(List<WhenClause> whenClauses, Expression defaultValue) {
        super(ImmutableList.builder().addAll(whenClauses).add(defaultValue).build().toArray(new Expression[0]));
        defaultValueIndex = children().size() - 1;
    }

    public List<WhenClause> getWhenClauses() {
        List<WhenClause> whenClauses = children().stream()
                .filter(e -> e instanceof WhenClause)
                .map(e -> (WhenClause) e)
                .collect(Collectors.toList());
        return whenClauses;
    }

    public Optional<Expression> getDefaultValue() {
        if (defaultValueIndex == -1) {
            return Optional.empty();
        }
        return Optional.of(child(defaultValueIndex));
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCaseWhen(this, context);
    }

    @Override
    public DataType getDataType() {
        return child(0).getDataType();
    }

    @Override
    public boolean nullable() {
        for (Expression child : children()) {
            if (child.nullable()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() throws UnboundException {
        StringBuilder output = new StringBuilder("CASE ");
        for (Expression child : children()) {
            if (child instanceof WhenClause) {
                output.append(child.toSql());
            } else {
                output.append(" ELSE " + child.toSql());
            }
        }
        output.append(" END");
        return output.toString();
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1);
        List<WhenClause> whenClauseList = new ArrayList<>();
        Expression defaultValue = null;
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) instanceof WhenClause) {
                whenClauseList.add((WhenClause) children.get(i));
            } else if (children.size() - 1 == i) {
                defaultValue = children.get(i);
            } else {
                throw new IllegalArgumentException("The children format needs to be [WhenClause*, DefaultValue+]");
            }
        }
        if (defaultValue == null) {
            return new CaseWhen(whenClauseList);
        }
        return new CaseWhen(whenClauseList, defaultValue);
    }
}
