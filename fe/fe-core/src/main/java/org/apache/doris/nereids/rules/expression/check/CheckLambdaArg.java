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

package org.apache.doris.nereids.rules.expression.check;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The current lambda arg can not reference parent's arg
 * Todo: support lambda arg reference parent's arg
 */
public class CheckLambdaArg implements ExpressionPatternRuleFactory {
    public static CheckLambdaArg INSTANCE = new CheckLambdaArg();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(Lambda.class).then(CheckLambdaArg::check)
        );
    }

    private static Expression check(Lambda lambda) {
        Set<ExprId> args = new HashSet<>();
        lambda.getLambdaArguments().forEach(arg -> args.add(arg.getExprId()));
        Set<Lambda> childLambdas = lambda.getLambdaFunction().collect(Lambda.class::isInstance);
        for (Lambda c : childLambdas) {
            Set<ArrayItemReference.ArrayItemSlot> slots
                    = c.getLambdaFunction().collect(ArrayItemReference.ArrayItemSlot.class::isInstance);
            for (ArrayItemReference.ArrayItemSlot slot : slots) {
                if (args.contains(slot.getExprId())) {
                    throw new AnalysisException("lambda can not reference parent's arg : " + slot.getName());
                }
            }
        }
        return lambda;
    }
}
