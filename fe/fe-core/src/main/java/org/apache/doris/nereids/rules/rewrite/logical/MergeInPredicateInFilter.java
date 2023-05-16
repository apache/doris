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
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import org.apache.commons.compress.utils.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A in (a, b) or A in (c, d) => A in (a, b, c, d)
 */
public class MergeInPredicateInFilter extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter().then(filter -> {
            Set<Expression> conjuncts = filter.getConjuncts();
            boolean changed = false;
            Set<Expression> newConjuncts = new HashSet<>();
            for (Expression conj : conjuncts) {
                Map<Boolean, List<Expression>> exprGroup =
                        ExpressionUtils.extractDisjunction(conj).stream()
                                .collect(Collectors.groupingBy(
                                        expression -> expression instanceof InPredicate,
                                        Collectors.mapping(Function.identity(), Collectors.toList())));

                List<Expression> inPredicates = exprGroup.get(true);
                if (inPredicates != null && !inPredicates.isEmpty()) {
                    List<Expression> mergedExpressions = mergeInPredicates(inPredicates);
                    if (!mergedExpressions.isEmpty()) {
                        changed = true;
                        if (exprGroup.get(false) != null) {
                            mergedExpressions.addAll(exprGroup.get(false));
                        }
                        Expression afterMerge = ExpressionUtils.or(mergedExpressions);
                        newConjuncts.add(afterMerge);
                    } else {
                        newConjuncts.add(conj);
                    }
                } else {
                    newConjuncts.add(conj);
                }
            }
            if (changed) {
                return new LogicalFilter<>(newConjuncts, filter.child());
            } else {
                return null;
            }
        }).toRule(RuleType.MERGE_IN_PREDICATE_FILTER);
    }

    private List<Expression> mergeInPredicates(List<Expression> inPredicates) {
        Map<Expression, List<InPredicate>> inPredGroups =
                inPredicates.stream().map(expression -> (InPredicate) expression)
                        .collect(Collectors.groupingBy(
                                inPredicate -> inPredicate.getCompareExpr(),
                                Collectors.mapping(Function.identity(), Collectors.toList())));
        boolean changed = false;
        List<Expression> result = Lists.newArrayList();
        for (List<InPredicate> group : inPredGroups.values()) {
            if (group.size() > 1) {
                changed = true;
                //merge
                List<Expression> newOptions = Lists.newArrayList();
                for (InPredicate in : group) {
                    newOptions.addAll(in.getOptions());
                }
                InPredicate newIn = new InPredicate(group.get(0).getCompareExpr(), newOptions);
                result.add(newIn);
            } else {
                result.add(group.get(0));
            }
        }
        if (changed) {
            return result;
        } else {
            return Lists.newArrayList();
        }
    }
}
