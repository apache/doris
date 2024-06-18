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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Paper: Quantifying TPC-H Choke Points and Their Optimizations
 * - Figure 14:
 * <p>
 * Examples:
 * where A in (x) ==> where A = x
 * where A not in (x) ==> where not A = x (After ExpressionTranslator, "not A = x" will be translated to "A != x")
 * <p>
 * NOTICE: it's related with `SimplifyRange`.
 * They are same processes, so must change synchronously.
 */
public class InPredicateToEqualToRule implements ExpressionPatternRuleFactory {
    public static final InPredicateToEqualToRule INSTANCE = new InPredicateToEqualToRule();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(InPredicate.class)
                    .when(in -> in.getOptions().size() == 1)
                    .then(in -> new EqualTo(in.getCompareExpr(), in.getOptions().get(0))
                )
        );
    }
}
