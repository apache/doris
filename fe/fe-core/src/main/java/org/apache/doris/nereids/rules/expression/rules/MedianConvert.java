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
import org.apache.doris.nereids.trees.expressions.functions.agg.Median;
import org.apache.doris.nereids.trees.expressions.functions.agg.Percentile;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * median(col) -> percentile(col, 0.5)
 */
public class MedianConvert implements ExpressionPatternRuleFactory {
    public static MedianConvert INSTANCE = new MedianConvert();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Median.class).then(median ->
                    new Percentile(median.child(0), DoubleLiteral.of(0.5))
                ).toRule(ExpressionRuleType.MEDIAN_CONVERT)
        );
    }
}
