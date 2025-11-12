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
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConcatWs;
import org.apache.doris.nereids.types.ArrayType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
    * ConcatWsTMultiArrayToOne, convert ConcatWs with multiple array arguments to a single array argument.
    * This rule is useful for optimizing queries that use ConcatWs with multiple array arguments,
    * allowing them to be processed more efficiently by combining the arrays into a single array argument.
 */
public class ConcatWsMultiArrayToOne implements ExpressionPatternRuleFactory {

    public static final ConcatWsMultiArrayToOne INSTANCE = new ConcatWsMultiArrayToOne();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(ConcatWs.class).then(ConcatWsMultiArrayToOne::rewrite)
                        .toRule(ExpressionRuleType.CONCATWS_MULTI_ARRAY_TO_ONE));
    }

    /** rewrite */
    public static Expression rewrite(ConcatWs cat) {
        if (cat.arity() >= 3 && cat.child(2).getDataType() instanceof ArrayType) {
            return new ConcatWs(cat.child(0),
                    new ArrayConcat(cat.child(1), cat.children().subList(2, cat.arity()).toArray(new Expression[0])));
        } else {
            return cat;
        }
    }
}
