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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.expression.ExpressionBottomUpRewriter;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MutableState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * dependends on SimplifyRange rule
 *
 */
public class OrToIn implements ExpressionPatternRuleFactory {

    public static final OrToIn INSTANCE = new OrToIn();

    public static final int REWRITE_OR_TO_IN_PREDICATE_THRESHOLD = 2;

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(Or.class).then(OrToIn.INSTANCE::rewrite)
        );
    }

    public Expression rewriteTree(Expression expr, ExpressionRewriteContext context) {
        if (expr instanceof CompoundPredicate) {
            expr = SimplifyRange.rewrite((CompoundPredicate) expr, context);
        }
        ExpressionBottomUpRewriter bottomUpRewriter = ExpressionRewrite.bottomUp(this);
        return bottomUpRewriter.rewrite(expr, context);
    }

    private Expression rewrite(Or or) {
        if (or.getMutableState(MutableState.KEY_OR_TO_IN).isPresent()) {
            return or;
        }
        Pair<Expression, Expression> pair = extractCommonConjunct(or);
        Expression result = tryToRewriteIn(pair.second);
        if (pair.first != null) {
            result = new And(pair.first, result);
        }
        result.setMutableState(MutableState.KEY_OR_TO_IN, 1);
        return result;
    }

    private Expression tryToRewriteIn(Expression or) {
        or.setMutableState(MutableState.KEY_OR_TO_IN, 1);
        List<Expression> disjuncts = ExpressionUtils.extractDisjunction(or);
        for (Expression disjunct : disjuncts) {
            if (!hasInOrEqualChildren(disjunct)) {
                return or;
            }
        }

        Map<Expression, Set<Literal>> candidates = getCandidates(disjuncts.get(0));
        if (candidates.isEmpty()) {
            return or;
        }

        // verify each candidate
        for (int i = 1; i < disjuncts.size(); i++) {
            Map<Expression, Set<Literal>> otherCandidates = getCandidates(disjuncts.get(i));
            if (otherCandidates.isEmpty()) {
                return or;
            }
            candidates = mergeCandidates(candidates, otherCandidates);
            if (candidates.isEmpty()) {
                return or;
            }
        }
        if (!candidates.isEmpty()) {
            Expression conjunct = candidatesToFinalResult(candidates);
            boolean keep = keepOriginalOrExpression(disjuncts);
            if (keep) {
                return new And(conjunct, or);
            } else {
                return conjunct;
            }
        }
        return or;
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

    private boolean containsAny(Set a, Set b) {
        for (Object x : a) {
            if (b.contains(x)) {
                return true;
            }
        }
        return false;
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

    private Expression candidatesToFinalResult(Map<Expression, Set<Literal>> candidates) {
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
        return ExpressionUtils.and(conjuncts);
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

    // conjuncts.get(idx) has different input slots
    private boolean independentConjunct(int idx, List<Expression> conjuncts) {
        Expression conjunct = conjuncts.get(idx);
        Set<Slot> targetSlots = conjunct.getInputSlots();
        if (conjuncts.size() == 1) {
            return true;
        }
        for (int i = 0; i < conjuncts.size(); i++) {
            if (i != idx) {
                Set<Slot> otherInput = Sets.newHashSet();
                otherInput.addAll(conjuncts.get(i).getInputSlots());
                otherInput.retainAll(targetSlots);
                if (!otherInput.isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

    private Map<Expression, Set<Literal>> getCandidates(Expression disjunct) {
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(disjunct);
        Map<Expression, Set<Literal>> candidates = new LinkedHashMap<>();
        // collect candidates from the first disjunction
        for (int idx = 0; idx < conjuncts.size(); idx++) {
            if (!independentConjunct(idx, conjuncts)) {
                continue;
            }
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

    /**
     * (a and b and ...) or (a and c and ...)
     * =>
     * a and [(b and ...) or (c and ...)]
     * extract the common part: a
     * and remaining part (b and ...) or (c and ...)
     * @returns Pair (common, remaining)
     */
    private Pair<Expression, Expression> extractCommonConjunct(Or or) {
        List<Expression> disjuncts = ExpressionUtils.extractDisjunction(or);
        List<List<Expression>> conjunctsList = Lists.newArrayList();
        for (Expression disjunct : disjuncts) {
            conjunctsList.add(ExpressionUtils.extractConjunction(disjunct));
        }
        List<Expression> commons = Lists.newArrayList();
        for (Expression a : conjunctsList.get(0)) {
            boolean isCommon = true;
            for (int i = 1; i < disjuncts.size(); i++) {
                if (!conjunctsList.get(i).contains(a)) {
                    isCommon = false;
                    break;
                }
            }
            if (isCommon) {
                commons.add(a);
            }
        }
        if (!commons.isEmpty()) {
            List<Expression> remainPart = Lists.newArrayList();
            for (int i = 0; i < disjuncts.size(); i++) {
                conjunctsList.get(i).removeAll(commons);
                remainPart.add(ExpressionUtils.and(conjunctsList.get(i)));
            }
            Expression remainOr = ExpressionUtils.or(remainPart);
            return Pair.of(ExpressionUtils.and(commons), remainOr);
        } else {
            return Pair.of(null, or);
        }
    }
}
