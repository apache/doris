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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;

import java.util.List;

/**
 * Rewrite InPredicate to an EqualTo Expression, if there exists exactly one element in InPredicate.options
 * Examples:
 * where A in (x) ==> where A = x
 * where A not in (x) ==> where not A = x (After ExpressionTranslator, "not A = x" will be translated to "A != x")
 */
public class InPredicateToEqualToRule extends AbstractExpressionRewriteRule {

    public static InPredicateToEqualToRule INSTANCE = new InPredicateToEqualToRule();

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        Expression left = inPredicate.getCompareExpr();
        List<Expression> right = inPredicate.getOptions();
        if (right.size() != 1) {
            return new InPredicate(left.accept(this, context), right);
        }
        return new EqualTo(left.accept(this, context), right.get(0).accept(this, context));
    }
}
