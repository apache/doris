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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.Set;

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
 * TODO: This rule just match filter, but it could be applied to inner/cross join condition.
 */
public class ExtractSingleTableExpressionFromDisjunction extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter().then(filter -> {
            List<Expression> dependentPredicates = extractDependentConjuncts(filter.getConjuncts());
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
        }).toRule(RuleType.EXTRACT_SINGLE_TABLE_EXPRESSION_FROM_DISJUNCTION);
    }

    private List<Expression> extractDependentConjuncts(Set<Expression> conjuncts) {
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
            for (Slot inputSlot : disjuncts.get(0).getInputSlots()) {
                String qualifier = String.join(".", inputSlot.getQualifier());
                List<Expression> extractForAll = Lists.newArrayList();
                boolean success = true;
                for (Expression expr : ExpressionUtils.extractDisjunction(conjunct)) {
                    Optional<Expression> extracted = extractSingleTableExpression(expr, qualifier);
                    if (!extracted.isPresent()) {
                        // extract failed
                        success = false;
                        break;
                    } else {
                        extractForAll.add(extracted.get());
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
    private Optional<Expression> extractSingleTableExpression(Expression expr, String qualifier) {
        List<Expression> output = Lists.newArrayList();
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(expr);
        for (Expression conjunct : conjuncts) {
            if (isSingleTableExpression(conjunct, qualifier)) {
                output.add(conjunct);
            }
        }
        if (output.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(ExpressionUtils.and(output));
        }
    }

    private boolean isSingleTableExpression(Expression expr, String qualifier) {
        //TODO: cache getSlotQualifierAsString() result.
        for (Slot slot : expr.getInputSlots()) {
            String slotQualifier = String.join(".", slot.getQualifier());
            if (!slotQualifier.equals(qualifier)) {
                return false;
            }
        }
        return true;
    }
}
