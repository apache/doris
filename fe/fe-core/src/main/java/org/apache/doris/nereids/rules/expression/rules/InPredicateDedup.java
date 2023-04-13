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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Deduplicate InPredicate, For example:
 * where A in (x, x) ==> where A in (x)
 */
public class InPredicateDedup extends AbstractExpressionRewriteRule {

    public static InPredicateDedup INSTANCE = new InPredicateDedup();

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        // In many BI scenarios, the sql is auto-generated, and hence there may be thousands of options.
        // It takes a long time to apply this rule. So set a threshold for the max number.
        if (inPredicate.getOptions().size() > 200) {
            return inPredicate;
        }
        Set<Expression> dedupExpr = new HashSet<>();
        List<Expression> newOptions = new ArrayList<>();
        for (Expression option : inPredicate.getOptions()) {
            if (dedupExpr.contains(option)) {
                continue;
            }
            dedupExpr.add(option);
            newOptions.add(option);
        }
        return new InPredicate(inPredicate.getCompareExpr(), newOptions);
    }
}
