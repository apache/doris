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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 rewrite predicate for partition prune
 */
public class PredicateRewriteForPartitionPrune
        extends DefaultExpressionRewriter<CascadesContext> {
    public static Expression rewrite(Expression expression,
                                     CascadesContext cascadesContext) {
        PredicateRewriteForPartitionPrune rewriter = new PredicateRewriteForPartitionPrune();
        return expression.accept(rewriter, cascadesContext);
    }

    /* F: a DateTime or DateTimeV2 column
     * Date(F) in (2020-01-02, 2020-01-01) =>
     *              (2020-01-01 24:00:00 >= F >= 2020-01-01 00:00:00)
     *              or (2020-01-02 24:00:00 >= F >= 2020-01-02 00:00:00)
     */
    @Override
    public Expression visitInPredicate(InPredicate in, CascadesContext context) {
        if (in.getCompareExpr() instanceof Date) {
            Expression dateChild = in.getCompareExpr().child(0);
            boolean convertable = true;
            List<Expression> splitIn = new ArrayList<>();
            // V1
            if (dateChild.getDataType() instanceof DateTimeType) {
                for (Expression opt : in.getOptions()) {
                    if (opt instanceof DateLiteral) {
                        GreaterThanEqual ge = new GreaterThanEqual(dateChild, ((DateLiteral) opt).toBeginOfTheDay());
                        LessThanEqual le = new LessThanEqual(dateChild, ((DateLiteral) opt).toEndOfTheDay());
                        splitIn.add(new And(ge, le));
                    } else {
                        convertable = false;
                        break;
                    }
                }
                if (convertable) {
                    Expression or = ExpressionUtils.combineAsLeftDeepTree(Or.class, splitIn);
                    return or;
                }
            } else if (dateChild.getDataType() instanceof DateTimeV2Type) {
                // V2
                convertable = true;
                for (Expression opt : in.getOptions()) {
                    if (opt instanceof DateLiteral) {
                        GreaterThanEqual ge = new GreaterThanEqual(dateChild, ((DateV2Literal) opt).toBeginOfTheDay());
                        LessThanEqual le = new LessThanEqual(dateChild, ((DateV2Literal) opt).toEndOfTheDay());
                        splitIn.add(new And(ge, le));
                    } else {
                        convertable = false;
                        break;
                    }
                }
                if (convertable) {
                    Expression or = ExpressionUtils.combineAsLeftDeepTree(Or.class, splitIn);
                    return or;
                }
            }
        }
        return in;
    }

}
