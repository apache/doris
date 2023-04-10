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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extract common expr for `CompoundPredicate`.
 * for example:
 * transform (a or b) and (a or c) to a or (b and c)
 * transform (a and b) or (a and c) to a and (b or c)
 */
@Developing
public class ExtractCommonFactorRule extends AbstractExpressionRewriteRule {

    public static final ExtractCommonFactorRule INSTANCE = new ExtractCommonFactorRule();

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate expr, ExpressionRewriteContext context) {

        Expression rewrittenChildren = ExpressionUtils.combine(expr.getClass(), ExpressionUtils.extract(expr).stream()
                .map(predicate -> rewrite(predicate, context)).collect(ImmutableList.toImmutableList()));
        if (!(rewrittenChildren instanceof CompoundPredicate)) {
            return rewrittenChildren;
        }

        CompoundPredicate compoundPredicate = (CompoundPredicate) rewrittenChildren;

        List<List<Expression>> partitions = ExpressionUtils.extract(compoundPredicate).stream()
                .map(predicate -> predicate instanceof CompoundPredicate ? ExpressionUtils.extract(
                        (CompoundPredicate) predicate) : Lists.newArrayList(predicate)).collect(Collectors.toList());

        Set<Expression> commons = partitions.stream()
                .<Set<Expression>>map(HashSet::new)
                .reduce(Sets::intersection)
                .orElse(Collections.emptySet());

        List<List<Expression>> uncorrelated = partitions.stream()
                .map(predicates -> predicates.stream().filter(p -> !commons.contains(p)).collect(Collectors.toList()))
                .collect(Collectors.toList());

        Expression combineUncorrelated = ExpressionUtils.combine(compoundPredicate.getClass(),
                uncorrelated.stream()
                        .map(predicates -> ExpressionUtils.combine(compoundPredicate.flipType(), predicates))
                        .collect(Collectors.toList()));

        List<Expression> finalCompound = Lists.newArrayList(commons);
        finalCompound.add(combineUncorrelated);

        return ExpressionUtils.combine(compoundPredicate.flipType(), finalCompound);
    }
}
