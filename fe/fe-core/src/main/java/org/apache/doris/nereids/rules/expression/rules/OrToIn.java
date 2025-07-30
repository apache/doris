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
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteRule;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Do NOT use this rule in ExpressionOptimization
 * apply this rule on filter expressions in extract mode,
 * on other expressions in replace mode
 *
 */
public class OrToIn {
    /**
     * case 1: from (a=1 and b=1) or (a=2), "a in (1, 2)" is inferred,
     * inferred expr is not equivalent to the original expr
     * - replaceMode: output origin expr
     * - extractMode: output a in (1, 2) and  (a=1 and b=1) or (a=2)
     *
     * case 2: from (a=1) or (a=2), "a in (1,2)" is inferred, the inferred expr is equivalent to the original expr
     * - replaceMode/extractMode: output a in (1, 2)
     *
     * extractMode only used for filter, the inferred In-predicate could be pushed down.
     */
    public enum Mode {
        replaceMode,
        extractMode
    }

    public static final OrToIn EXTRACT_MODE_INSTANCE = new OrToIn(Mode.extractMode);
    public static final OrToIn REPLACE_MODE_INSTANCE = new OrToIn(Mode.replaceMode);

    private final Mode mode;

    public OrToIn(Mode mode) {
        this.mode = mode;
    }

    /**
     * simplify and then rewrite
     */
    public Expression rewriteTree(Expression expr, ExpressionRewriteContext context) {
        if (!expr.containsType(Or.class, InPredicate.class)) {
            return expr;
        }
        ExpressionRewriteRule<ExpressionRewriteContext> simplify = ExpressionRewrite.bottomUp(SimplifyRange.INSTANCE);
        expr = simplify.rewrite(expr, context);
        return rewriteTree(expr);
    }

    /**
     * rewrite tree
     */
    public Expression rewriteTree(Expression expr) {
        List<Expression> children = expr.children();
        if (children.isEmpty()) {
            return expr;
        }

        Builder<Expression> newChildrenBuilder = ImmutableList.builderWithExpectedSize(children.size());
        for (Expression child : children) {
            newChildrenBuilder.add(rewriteTree(child));
        }
        List<Expression> newChildren = newChildrenBuilder.build();
        if (expr instanceof And) {
            // filter out duplicated conjunct
            // example: OrToInTest.testDeDup()
            Set<Expression> dedupSet = new LinkedHashSet<>();
            for (Expression newChild : newChildren) {
                dedupSet.addAll(ExpressionUtils.extractConjunction(newChild));
            }
            newChildren = Lists.newArrayList(dedupSet);
        }
        if (expr instanceof CompoundPredicate && newChildren.size() == 1) {
            // (a=1) and (a=1)
            // after rewrite, newChildren=[(a=1)]
            expr = newChildren.get(0);
        } else {
            expr = expr.withChildren(newChildren);
        }
        if (expr instanceof Or) {
            expr = rewrite((Or) expr);
        }
        return expr;
    }

    private Expression rewrite(Or or) {
        Pair<Expression, Expression> pair = extractCommonConjunct(or);
        Expression result = tryToRewriteIn(pair.second);
        if (pair.first != null) {
            result = new And(pair.first, result);
        }
        return result;
    }

    private Expression tryToRewriteIn(Expression or) {
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
                if (mode == Mode.extractMode) {
                    return new And(conjunct, or);
                } else {
                    return or;
                }
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
        Builder<Expression> result = ImmutableList.builderWithExpectedSize(candidates.size());
        for (Entry<Expression, Set<Literal>> entry : candidates.entrySet()) {
            result.add(ExpressionUtils.toInPredicateOrEqualTo(entry.getKey(), entry.getValue()));
        }
        return ExpressionUtils.and(result.build());
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
