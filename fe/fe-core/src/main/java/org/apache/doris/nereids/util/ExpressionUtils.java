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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Expression rewrite helper class.
 */
public class ExpressionUtils {

    public static List<Expression> extractConjunct(Expression expr) {
        return extract(ExpressionType.AND, expr);
    }

    public static List<Expression> extractDisjunct(Expression expr) {
        return extract(ExpressionType.OR, expr);
    }

    public static List<Expression> extract(CompoundPredicate expr) {
        return extract(expr.getType(), expr);
    }

    private static List<Expression> extract(ExpressionType op, Expression expr) {
        List<Expression> result = Lists.newArrayList();
        extract(op, expr, result);
        return result;
    }

    private static void extract(ExpressionType op, Expression expr, List<Expression> result) {
        if (expr instanceof CompoundPredicate && expr.getType() == op) {
            CompoundPredicate predicate = (CompoundPredicate) expr;
            extract(op, predicate.left(), result);
            extract(op, predicate.right(), result);
        } else {
            result.add(expr);
        }
    }

    public static Expression and(List<Expression> expressions) {
        return combine(ExpressionType.AND, expressions);
    }

    public static Expression and(Expression... expressions) {
        return combine(ExpressionType.AND, Lists.newArrayList(expressions));
    }

    public static Expression or(Expression... expressions) {
        return combine(ExpressionType.OR, Lists.newArrayList(expressions));
    }

    public static Expression or(List<Expression> expressions) {
        return combine(ExpressionType.OR, expressions);
    }

    /**
     * Use AND/OR to combine expressions together.
     */
    public static Expression combine(ExpressionType op, List<Expression> expressions) {
        Preconditions.checkArgument(op == ExpressionType.AND || op == ExpressionType.OR);
        Objects.requireNonNull(expressions, "expressions is null");

        Expression shortCircuit = (op == ExpressionType.AND ? BooleanLiteral.FALSE : BooleanLiteral.TRUE);
        Expression skip = (op == ExpressionType.AND ? BooleanLiteral.TRUE : BooleanLiteral.FALSE);
        LinkedHashSet<Expression> distinctExpressions = Sets.newLinkedHashSetWithExpectedSize(expressions.size());
        for (Expression expression : expressions) {
            if (expression.equals(shortCircuit)) {
                return shortCircuit;
            } else if (!expression.equals(skip)) {
                distinctExpressions.add(expression);
            }
        }

        Optional<Expression> result =
                distinctExpressions.stream().reduce((left, right) -> new CompoundPredicate(op, left, right));
        return result.orElse(new BooleanLiteral(op == ExpressionType.AND));
    }
}
