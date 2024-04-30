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
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Normalizes binary predicates of the form 'expr' op 'slot' so that the slot is on the left-hand side.
 * For example:
 * 5 > id -> id < 5
 */
public class NormalizeBinaryPredicatesRule implements ExpressionPatternRuleFactory {
    public static final NormalizeBinaryPredicatesRule INSTANCE = new NormalizeBinaryPredicatesRule();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(ComparisonPredicate.class).then(NormalizeBinaryPredicatesRule::normalize)
        );
    }

    public static Expression normalize(ComparisonPredicate expr) {
        return expr.left().isConstant() && !expr.right().isConstant() ? expr.commute() : expr;
    }
}
