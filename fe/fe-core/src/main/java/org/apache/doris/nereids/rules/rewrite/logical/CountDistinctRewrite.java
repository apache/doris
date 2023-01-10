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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrite count distinct for bitmap and hll type value.
 * <p>
 * count(distinct bitmap_col) -> bitmap_union_count(bitmap col)
 */
public class CountDistinctRewrite extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(agg -> {
            List<NamedExpression> output = agg.getOutputExpressions()
                    .stream()
                    .map(CountDistinctRewriter::rewrite)
                    .map(NamedExpression.class::cast)
                    .collect(ImmutableList.toImmutableList());
            return agg.withAggOutput(output);
        }).toRule(RuleType.COUNT_DISTINCT_REWRITE);
    }

    private static class CountDistinctRewriter extends DefaultExpressionRewriter<Void> {
        private static final CountDistinctRewriter INSTANCE = new CountDistinctRewriter();

        public static Expression rewrite(Expression expr) {
            return expr.accept(INSTANCE, null);
        }

        @Override
        public Expression visitCount(Count count, Void context) {
            if (count.isDistinct() && count.arity() == 1) {
                Expression child = count.child(0);
                if (child.getDataType().isBitmap()) {
                    return new BitmapUnionCount(child);
                }
                if (child.getDataType().isHll()) {
                    return new HllUnionAgg(child);
                }
            }
            return count;
        }
    }
}
