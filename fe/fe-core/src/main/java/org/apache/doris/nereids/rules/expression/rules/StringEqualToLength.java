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
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrite rule to convert EQUAL(string, "") to length(string, 0)
 * For example:
 * string == ""  ==>  length(string) == 0
 */
public class StringEqualToLength implements ExpressionPatternRuleFactory {

    public static StringEqualToLength INSTANCE = new StringEqualToLength();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
            matchesTopType(EqualPredicate.class).then(StringEqualToLength::rewrite)
        );
    }

    private static Expression rewrite(EqualPredicate equalPredicate) {
        Expression expr = equalPredicate;
        if (equalPredicate.getArgument(0).getDataType().isStringType()
                && equalPredicate.getArgument(1) instanceof StringLikeLiteral
                && ((StringLikeLiteral) equalPredicate.getArgument(1)).getStringValue().equals("")) {
            expr = new EqualTo(new Length(equalPredicate.getArgument(0)), new IntegerLiteral(0));
        }
        return expr;
    }
}
