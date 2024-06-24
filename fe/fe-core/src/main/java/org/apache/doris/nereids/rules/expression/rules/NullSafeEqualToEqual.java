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
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * convert "A <=> null" to "A is null"
 * null <=> null : true
 * null <=> 1 : false
 * 1 <=> 2 : 1 = 2
 */
public class NullSafeEqualToEqual implements ExpressionPatternRuleFactory {
    public static final NullSafeEqualToEqual INSTANCE = new NullSafeEqualToEqual();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(NullSafeEqual.class).then(NullSafeEqualToEqual::rewrite)
        );
    }

    private static Expression rewrite(NullSafeEqual nullSafeEqual) {
        // because the nullable info hasn't been finalized yet, the optimization is limited
        if (nullSafeEqual.left().isNullLiteral() && nullSafeEqual.right().isNullLiteral()) {
            return BooleanLiteral.TRUE;
        } else if (nullSafeEqual.left().isNullLiteral()) {
            return nullSafeEqual.right().isLiteral() ? BooleanLiteral.FALSE : new IsNull(nullSafeEqual.right());
        } else if (nullSafeEqual.right().isNullLiteral()) {
            return nullSafeEqual.left().isLiteral() ? BooleanLiteral.FALSE : new IsNull(nullSafeEqual.left());
        } else if (nullSafeEqual.left().isLiteral() && nullSafeEqual.right().isLiteral()) {
            return new EqualTo(nullSafeEqual.left(), nullSafeEqual.right());
        }
        return nullSafeEqual;
    }
}
