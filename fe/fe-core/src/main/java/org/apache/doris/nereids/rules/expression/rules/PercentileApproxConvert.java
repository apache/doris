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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.PercentileApprox;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Automatically appends the default compression argument to percentile_approx(c, p),
 * rewriting it as percentile_approx(c, p, ${percentile_approx_default_compression}).
 * The rewrite only happens when the session variable value is greater than 0.
 * If the third argument is explicitly provided, no rewrite is performed.
 */
public class PercentileApproxConvert implements ExpressionPatternRuleFactory {

    public static final PercentileApproxConvert INSTANCE = new PercentileApproxConvert();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(PercentileApprox.class)
                        .when(p -> p.arity() == 2)
                        .then(PercentileApproxConvert::rewrite)
                    .toRule(ExpressionRuleType.PERCENTILE_APPROX_CONVERT)
        );
    }

    private static Expression rewrite(PercentileApprox origin) {
        int compression = getDefaultCompression();
        if (compression <= 0) {
            return origin;
        }
        return origin.withDistinctAndChildren(
                origin.isDistinct(),
                ImmutableList.of(
                        origin.child(0),
                        origin.child(1),
                        new IntegerLiteral(compression)
                )
        );
    }

    private static int getDefaultCompression() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable() != null) {
            return ctx.getSessionVariable().percentileApproxDefaultCompression;
        }
        return -1;
    }
}
