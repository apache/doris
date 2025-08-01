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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Paper: Quantifying TPC-H Choke Points and Their Optimizations
 * 4.4 Join-Dependent Predicate Duplication
 * Example:
 * Two queries, Q7 and Q19, include predicates that operate
 * on multiple tables without being a join predicate. In Q17,
 * (n1.name = ’NATION1’ AND n2.name = ’NATION2’) OR (n1.name = ’NATION2’ AND n2.name = ’NATION1’)
 * =>
 * (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
 * and (n1.n_name = 'FRANCE' or n1.n_name='GERMANY') and (n2.n_name='GERMANY' or n2.n_name='FRANCE')
 * <p>
 * new generated expr is redundant, but they could be pushed down to reduce the cardinality of children output tuples.
 * <p>
 * Implementation note:
 * 1. This rule should only be applied ONCE to avoid generate same redundant expression.
 * 2. A redundant expression only contains slots from a single table.
 * 3. In old optimizer, there is `InferFilterRule` generates redundancy expressions. Its Nereid counterpart also need
 * `RemoveRedundantExpression`.
 * <p>
 */
public class ExtractSingleTableExpressionFromDisjunction implements RewriteRuleFactory {
    private static final ImmutableSet<JoinType> ALLOW_JOIN_TYPE = ImmutableSet.of(JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN, JoinType.RIGHT_OUTER_JOIN, JoinType.LEFT_SEMI_JOIN, JoinType.RIGHT_SEMI_JOIN,
            JoinType.LEFT_ANTI_JOIN, JoinType.RIGHT_ANTI_JOIN, JoinType.CROSS_JOIN, JoinType.FULL_OUTER_JOIN);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalFilter().then(filter -> {
                    List<Expression> dependentPredicates = extractDependentConjuncts(filter.getConjuncts(),
                            Lists.newArrayList());
                    if (dependentPredicates.isEmpty()) {
                        return null;
                    }
                    Set<Expression> newPredicates = ImmutableSet.<Expression>builder()
                            .addAll(filter.getConjuncts())
                            .addAll(dependentPredicates).build();
                    if (newPredicates.size() == filter.getConjuncts().size()) {
                        return null;
                    }
                    return new LogicalFilter<>(newPredicates, filter.child());
                }).toRule(RuleType.EXTRACT_SINGLE_TABLE_EXPRESSION_FROM_DISJUNCTION),
                logicalJoin().when(join -> ALLOW_JOIN_TYPE.contains(join.getJoinType())).then(join -> {
                    List<Set<String>> qualifierBatches = Lists.newArrayList();
                    // for join, also extract multiple tables: left tables and right tables
                    qualifierBatches.add(join.left().getOutputSet().stream().map(Slot::getJoinQualifier)
                            .collect(Collectors.toSet()));
                    qualifierBatches.add(join.right().getOutputSet().stream().map(Slot::getJoinQualifier)
                            .collect(Collectors.toSet()));
                    List<Expression> dependentOtherPredicates = extractDependentConjuncts(
                            ImmutableSet.copyOf(join.getOtherJoinConjuncts()), qualifierBatches);
                    if (dependentOtherPredicates.isEmpty()) {
                        return null;
                    }
                    Set<Expression> newOtherPredicates = ImmutableSet.<Expression>builder()
                            .addAll(join.getOtherJoinConjuncts())
                            .addAll(dependentOtherPredicates).build();
                    if (newOtherPredicates.size() == join.getOtherJoinConjuncts().size()) {
                        return null;
                    }
                    return join.withJoinConjuncts(join.getHashJoinConjuncts(),
                            ImmutableList.copyOf(newOtherPredicates),
                            join.getMarkJoinConjuncts(), join.getJoinReorderContext());
                }).toRule(RuleType.EXTRACT_SINGLE_TABLE_EXPRESSION_FROM_DISJUNCTION));
    }

    private List<Expression> extractDependentConjuncts(Set<Expression> conjuncts, List<Set<String>> qualifierBatches) {
        List<Expression> dependentPredicates = Lists.newArrayList();
        for (Expression conjunct : conjuncts) {
            // conjunct=(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
            //          or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            List<Expression> disjuncts = ExpressionUtils.extractDisjunction(conjunct);
            // disjuncts={ (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY'),
            //            (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')}
            if (disjuncts.size() == 1) {
                continue;
            }
            // only check table in first disjunct.
            // In our example, qualifiers = { n1, n2 }
            // try to extract
            Set<String> qualifiers = disjuncts.get(0).getInputSlots().stream()
                    .map(slot -> String.join(".", slot.getQualifier()))
                    .collect(Collectors.toCollection(Sets::newLinkedHashSet));
            List<Set<String>> includeSingleQualifierBatches = Lists.newArrayListWithExpectedSize(
                    qualifiers.size() + qualifierBatches.size());
            // extract single table's expression
            for (String qualifier : qualifiers) {
                includeSingleQualifierBatches.add(ImmutableSet.of(qualifier));
            }
            // for join, extract left tables and right tables
            for (Set<String> batch : qualifierBatches) {
                Set<String> newBatch = batch.stream().filter(qualifiers::contains).collect(Collectors.toSet());
                // if newBatch.size == 1, then it had put into includeSingleQualifierBatches
                if (newBatch.size() > 1) {
                    includeSingleQualifierBatches.add(newBatch);
                }
            }
            for (Set<String> batch : includeSingleQualifierBatches) {
                List<Expression> extractForAll = Lists.newArrayList();
                boolean success = true;
                for (Expression expr : disjuncts) {
                    Optional<Expression> extracted = extractMultipleTableExpression(expr, batch);
                    if (!extracted.isPresent()) {
                        // extract failed
                        success = false;
                        break;
                    } else {
                        extractForAll.addAll(ExpressionUtils.extractDisjunction(extracted.get()));
                    }
                }
                if (success) {
                    dependentPredicates.add(ExpressionUtils.or(extractForAll));
                }
            }
        }
        return dependentPredicates;
    }

    // extract some conjucts from expr, all slots of the extracted conjunct comes from the table referred by qualifier.
    // example: expr=(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY'), qualifier="n1."
    // output: n1.n_name = 'FRANCE'
    private Optional<Expression> extractMultipleTableExpression(Expression expr, Set<String> qualifiers) {
        // suppose the qualifier is table T, then the process steps are as follow:
        // 1. split the expression into conjunctions: c1 and c2 and c3 and ...
        // 2. for each conjunction ci, suppose its extract is Ei:
        //    a) if ci's all slots come from T, then the whole ci is extracted, then Ei = ci;
        //    b) if ci is an OR expression, then split ci into disjunctions:  ci => d1 or d2 or d3 or ...,
        //       for each disjunction, extract it recuirsely, suppose after extract dj, we get ej,
        //       if all the dj can extracted ej, then extract ci succ, which is Ei = e1 or e2 or e3 or ...,
        //       if any dj extract failed, then extract ci fail
        // 3. collect all the succ extracted Ei, and the result for table T is `E1 and E2 and E3 and ...`
        //
        // for example:
        // suppose expr = (t1.a = 1 or (t2.b = 2 and t1.c = 3)) and (t1.d = 4 or t2.e = 5), qualifier = t1, then
        // c1 = (t1.a = 1 or (t2.b = 2 and t1.c = 3)),
        // because the whole c1 contains slot t2.b not belong to t1, so cannot extract the whole c1,
        // but c1 is an OR expression, so split c1 into disjunctions:
        // d1 => t1.a = 1, d2 => (t2.b = 2 and t1.c = 3)
        // then after extract on d1, we get e1 = t1.a = 1, extract on d2, we get t1.c = 3,
        // so we can extract E1 for c1:   t1.a = 1 or t1.c = 3
        List<Expression> output = Lists.newArrayList();
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(expr);
        for (Expression conjunct : conjuncts) {
            if (isTableExpression(conjunct, qualifiers)) {
                output.add(conjunct);
            } else if (conjunct instanceof Or) {
                List<Expression> disjuncts = ExpressionUtils.extractDisjunction(conjunct);
                List<Expression> extracted = Lists.newArrayListWithExpectedSize(disjuncts.size());
                boolean success = true;
                for (Expression disjunct : disjuncts) {
                    Optional<Expression> extractedDisjunct = extractMultipleTableExpression(disjunct, qualifiers);
                    if (extractedDisjunct.isPresent()) {
                        extracted.addAll(ExpressionUtils.extractDisjunction(extractedDisjunct.get()));
                    } else {
                        // extract failed
                        success = false;
                        break;
                    }
                }
                if (success) {
                    output.add(ExpressionUtils.or(extracted));
                }
            }
        }
        if (output.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(ExpressionUtils.and(output));
        }
    }

    private boolean isTableExpression(Expression expr, Set<String> qualifiers) {
        //TODO: cache getSlotQualifierAsString() result.
        return expr.getInputSlots().stream().allMatch(slot -> qualifiers.contains(slot.getJoinQualifier()));
    }
}
