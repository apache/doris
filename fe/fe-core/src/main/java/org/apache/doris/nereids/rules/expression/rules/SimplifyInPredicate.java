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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * SimplifyInPredicate
 */
public class SimplifyInPredicate extends AbstractExpressionRewriteRule {

    public static final SimplifyInPredicate INSTANCE = new SimplifyInPredicate();

    @Override
    public Expression visitInPredicate(InPredicate expr, ExpressionRewriteContext context) {
        if (expr.children().size() > 1) {
            if (expr.getCompareExpr() instanceof Cast) {
                Cast cast = (Cast) expr.getCompareExpr();
                if (cast.child().getDataType().isDateV2Type()
                        && expr.child(1) instanceof DateTimeV2Literal) {
                    List<Expression> literals = expr.children().subList(1, expr.children().size());
                    if (literals.stream().allMatch(literal -> literal instanceof DateTimeV2Literal
                            && canLosslessConvertToDateV2Literal((DateTimeV2Literal) literal))) {
                        List<Expression> children = Lists.newArrayList();
                        children.add(cast.child());
                        literals.stream().forEach(
                                l -> children.add(convertToDateV2Literal((DateTimeV2Literal) l)));
                        return expr.withChildren(children);
                    }
                }
            }
        }
        return expr;
    }

    /*
    derive tree:
    DateLiteral
      |
      +--->DateTimeLiteral
      |        |
      |        +----->DateTimeV2Literal
      +--->DateV2Literal
    */
    private static boolean canLosslessConvertToDateV2Literal(DateTimeV2Literal literal) {
        return (literal.getHour() | literal.getMinute() | literal.getSecond()
                | literal.getMicroSecond()) == 0L;
    }

    private DateV2Literal convertToDateV2Literal(DateTimeV2Literal literal) {
        return new DateV2Literal(literal.getYear(), literal.getMonth(), literal.getDay());
    }
}
