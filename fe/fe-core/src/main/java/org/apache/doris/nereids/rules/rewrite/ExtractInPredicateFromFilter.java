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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * ExtractSingleTableInPredicate
 */
public class ExtractInPredicateFromFilter implements RewriteRuleFactory {
    public static final int REWRITE_OR_TO_IN_PREDICATE_THRESHOLD = 2;

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(logicalFilter().then(
                this::tryToExtractSingleTablePredicate
        ).toRule(RuleType.EXTRACT_IN_PREDICATE_FROM_FILTER));
    }

    /**
     * tryToExtractSingleTablePredicate
     */
    public LogicalFilter<Plan> tryToExtractSingleTablePredicate(LogicalFilter<Plan> filter) {
        Set<Expression> conjuncts = filter.getConjuncts();
        Set<Expression> inOrEqual = Sets.newHashSet();
        // "a=1 or a=2" is converted to a in (1, 2), and no need to keep "a=1 or a=2",
        // "(a=1 and c=1) or (a=2 and d=2)" is converted to "a in (1, 2) and (a=1 and c=1) or (a=2 and d=2)"
        // (a=1 and c=1) or (a=2 and d=2) is put to the remainConjuncts
        List<Expression> remainConjuncts = Lists.newArrayList();
        boolean hasNewInOrEuqal = false;
        for (Expression conjunct : conjuncts) {
            if (conjunct instanceof EqualTo) {
                inOrEqual.add(conjunct);
            } else if (conjunct instanceof InPredicate) {
                inOrEqual.add(conjunct);
            } else if (conjunct instanceof Or) {
                ExtractResult extractResult = tryToRewriteOr((Or) conjunct);
                for (Expression extracted : extractResult.extracted) {
                    if (!inOrEqual.contains(extracted)) {
                        inOrEqual.add(extracted);
                        hasNewInOrEuqal = true;
                    }
                    extractResult.optOr.ifPresent(remainConjuncts::add);
                }
            } else {
                remainConjuncts.add(conjunct);
            }
        }
        if (hasNewInOrEuqal) {
            inOrEqual.addAll(remainConjuncts);
            return filter.withConjuncts(inOrEqual);
        }
        return null;
    }

    private static class ExtractResult {
        List<Expression> extracted; // extracted In-predicate or EqualTo
        Optional<Or> optOr;

        ExtractResult(List<Expression> extracted, Or or) {
            this.extracted = extracted;
            this.optOr = Optional.ofNullable(or);
        }
    }

    /*
   it is not necessary to rewrite "a like 'xyz' or a=1 or a=2" to "a like 'xyz' or a in (1, 2)",
   because we cannot push "a in (1, 2)" into storage layer
 */
    private boolean hasInOrEqualChildren(Expression disjunct) {
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(disjunct);
        for (Expression conjunct : conjuncts) {
            if (conjunct instanceof EqualTo || conjunct instanceof InPredicate) {
                return true;
            }
        }
        return false;
    }

    private ExtractResult tryToRewriteOr(Or or) {
        List<Expression> disjuncts = ExpressionUtils.extractDisjunction(or);
        for (Expression disjunct : disjuncts) {
            if (!hasInOrEqualChildren(disjunct)) {
                return new ExtractResult(ImmutableList.of(), or);
            }
        }

        Map<Expression, Set<Literal>> candidates = getCandidates(disjuncts.get(0));
        if (candidates.isEmpty()) {
            return new ExtractResult(ImmutableList.of(), or);
        }

        // verify each candidate
        for (int i = 1; i < disjuncts.size(); i++) {
            Map<Expression, Set<Literal>> otherCandidates = getCandidates(disjuncts.get(i));
            if (otherCandidates.isEmpty()) {
                return new ExtractResult(ImmutableList.of(), or);
            }
            candidates = mergeCandidates(candidates, otherCandidates);
            if (candidates.isEmpty()) {
                return new ExtractResult(ImmutableList.of(), or);
            }
        }
        List<Expression> extracted = candidatesToFinalResult(candidates);
        boolean keep = keepOriginalOrExpression(disjuncts);
        if (keep) {
            return new ExtractResult(extracted, or);
        } else {
            return new ExtractResult(extracted, null);
        }
    }

    private Map<Expression, Set<Literal>> mergeCandidates(
            Map<Expression, Set<Literal>> a,
            Map<Expression, Set<Literal>> b) {
        Map<Expression, Set<Literal>> result = new LinkedHashMap<>();
        for (Expression expr : a.keySet()) {
            Set<Literal> otherLiterals = b.get(expr);
            if (otherLiterals != null) {
                Set<Literal> literals = a.get(expr);
                literals.addAll(otherLiterals);
                if (!literals.isEmpty()) {
                    result.put(expr, literals);
                }
            }
        }
        return result;
    }

    private List<Expression> candidatesToFinalResult(Map<Expression, Set<Literal>> candidates) {
        List<Expression> conjuncts = new ArrayList<>();
        for (Expression key : candidates.keySet()) {
            Set<Literal> literals = candidates.get(key);
            if (literals.size() < REWRITE_OR_TO_IN_PREDICATE_THRESHOLD) {
                for (Literal literal : literals) {
                    conjuncts.add(new EqualTo(key, literal));
                }
            } else {
                conjuncts.add(new InPredicate(key, ImmutableList.copyOf(literals)));
            }
        }
        return conjuncts;
    }

    private Map<Expression, Set<Literal>> getCandidates(Expression disjunct) {
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(disjunct);
        Map<Expression, Set<Literal>> candidates = new LinkedHashMap<>();
        // collect candidates from the first disjunction
        for (int idx = 0; idx < conjuncts.size(); idx++) {
            // find pattern: A=1 / A in (1, 2, 3 ...)
            // candidates: A->[1] / A -> [1, 2, 3, ...]
            Expression conjunct = conjuncts.get(idx);
            Expression compareExpr = null;
            if (conjunct instanceof EqualTo) {
                EqualTo eq = (EqualTo) conjunct;
                Literal literal = null;
                if (!(eq.left() instanceof Literal) && eq.right() instanceof Literal) {
                    compareExpr = eq.left();
                    literal = (Literal) eq.right();
                } else if (!(eq.right() instanceof Literal) && eq.left() instanceof Literal) {
                    compareExpr = eq.right();
                    literal = (Literal) eq.left();
                }
                if (compareExpr != null) {
                    Set<Literal> literals = candidates.get(compareExpr);
                    if (literals == null) {
                        literals = Sets.newHashSet();
                        literals.add(literal);
                        candidates.put(compareExpr, literals);
                    } else {
                        // pattern like (A=1 and A=2) should be processed by SimplifyRange rule
                        // OrToIn rule does apply to this expression
                        candidates.clear();
                        break;

                    }
                }
            } else if (conjunct instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) conjunct;
                Set<Literal> literalOptions = new LinkedHashSet<>();
                boolean allLiteralOpts = true;
                for (Expression opt : inPredicate.getOptions()) {
                    if (opt instanceof Literal) {
                        literalOptions.add((Literal) opt);
                    } else {
                        allLiteralOpts = false;
                        break;
                    }
                }

                if (allLiteralOpts) {
                    Set<Literal> alreadyMappedLiterals = candidates.get(inPredicate.getCompareExpr());
                    if (alreadyMappedLiterals == null) {
                        candidates.put(inPredicate.getCompareExpr(), literalOptions);
                    } else {
                        // pattern like (A=1 and A in (1, 2)) should be processed by SimplifyRange rule
                        // OrToIn rule does apply to this expression
                        candidates.clear();
                        break;
                    }
                }
            }
        }
        return candidates;
    }

    private boolean keepOriginalOrExpression(List<Expression> disjuncts) {
        for (Expression disjunct : disjuncts) {
            List<Expression> conjuncts = ExpressionUtils.extractConjunction(disjunct);
            if (conjuncts.size() > 1) {
                return true;
            }
        }
        return false;
    }

}
