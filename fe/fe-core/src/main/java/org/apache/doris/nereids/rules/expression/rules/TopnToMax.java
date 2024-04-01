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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopN;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Convert topn(x, 1) to max(x)
 */
public class TopnToMax implements ExpressionPatternRuleFactory {

    public static final TopnToMax INSTANCE = new TopnToMax();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(TopN.class).then(TopnToMax::rewrite)
        );
    }

    /** rewrite */
    public static Expression rewrite(TopN topN) {
        if (topN.arity() == 2 && topN.child(1) instanceof IntegerLikeLiteral
                && ((IntegerLikeLiteral) topN.child(1)).getIntValue() == 1) {
            return new Max(topN.child(0));
        } else {
            return topN;
        }
    }
}
