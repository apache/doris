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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.BooleanType;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * NOTICE:
 * null in (1, null) => NullLiteral
 * null not in (1, null) => NullLiteral
 * k1 in (1, null) => k1 in (1)
 * k1 in (null) => NullLiteral
 * k1 not in (1, null) => NullLiteral
 * 1 not in (1, null) => FalseLiteral
 * 2 not in (1, null) => NullLiteral
 * NOTICE: run before InPredicateToEqualToRule.
 */
public class NormalizeInPredicateRule extends AbstractExpressionRewriteRule {
    public static NormalizeInPredicateRule INSTANCE = new NormalizeInPredicateRule();

    @Override
    public Expression visitNot(Not not, ExpressionRewriteContext context) {
        if (not.child() instanceof InPredicate) {
            InPredicate inPredicate = ((InPredicate) not.child());
            Expression compareExpr = inPredicate.getCompareExpr();
            // case 2:
            if (compareExpr.isNullLiteral()) {
                return new NullLiteral(BooleanType.INSTANCE);
            }
            List<Expression> options = inPredicate.getOptions();
            for (Expression option : options) {
                // case 6:
                if (compareExpr.equals(option)) {
                    return BooleanLiteral.FALSE;
                }
                // case 5, 7:
                if (option.isNullLiteral()) {
                    return new NullLiteral(BooleanType.INSTANCE);
                }
            }
            return not;
        }
        return not;
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        Expression compareExpr = inPredicate.getCompareExpr();
        // case 1:
        if (compareExpr.isNullLiteral()) {
            return new NullLiteral(BooleanType.INSTANCE);
        }
        List<Expression> options = inPredicate.getOptions();
        List<Expression> newOptions = Lists.newArrayList(inPredicate.getCompareExpr());
        // case 3:
        for (Expression option : options) {
            if (!option.isNullLiteral()) {
                newOptions.add(option);
            }
        }
        // case 4:
        if (newOptions.size() == 1) {
            return new NullLiteral(BooleanType.INSTANCE);
        }
        return inPredicate.withChildren(newOptions);
    }
}
