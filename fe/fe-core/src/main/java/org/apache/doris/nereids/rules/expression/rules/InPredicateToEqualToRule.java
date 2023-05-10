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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Paper: Quantifying TPC-H Choke Points and Their Optimizations
 * - Figure 14:
 * <p>
 * Rewrite InPredicate to disjunction, if there exists < 3 elements in InPredicate
 * Examples:
 * where A in (x, y) ==> where A = x or A = y
 * Examples:
 * where A in (x) ==> where A = x
 * where A not in (x) ==> where not A = x (After ExpressionTranslator, "not A = x" will be translated to "A != x")
 * <p>
 * NOTICE: it's related with `SimplifyRange`.
 * They are same processes, so must change synchronously.
 */
public class InPredicateToEqualToRule extends AbstractExpressionRewriteRule {

    public static InPredicateToEqualToRule INSTANCE = new InPredicateToEqualToRule();

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        Expression cmpExpr = inPredicate.getCompareExpr();
        List<Expression> options = inPredicate.getOptions();
        Preconditions.checkArgument(options.size() > 0, "InPredicate.options should not be empty");
        if (options.size() > 2) {
            return new InPredicate(cmpExpr.accept(this, context), options);
        }
        Expression newCmpExpr = cmpExpr.accept(this, context);
        List<Expression> disjunction = options.stream()
                .map(option -> new EqualTo(newCmpExpr, option.accept(this, context)))
                .collect(Collectors.toList());
        return ExpressionUtils.or(disjunction);
    }
}
