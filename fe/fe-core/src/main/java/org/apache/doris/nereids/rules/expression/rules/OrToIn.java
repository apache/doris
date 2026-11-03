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
import org.apache.doris.nereids.analyzer.Unbound;
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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashSet;
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
     * case 3 (extractMode only): the candidate slot is also referenced by sibling conjuncts of the same disjunct,
     * e.g. "(a=1 and b=1) or (a in (2, 3) and ((a=2 and c=1) or (a=3 and c=2)))".
     * - replaceMode: output origin expr
     * - extractMode: output a in (1, 2, 3) and (origin expr)
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

    /**
     * Whether conjuncts.get(idx) may contribute an "X = literal" / "X IN (literals)" candidate.
     *
     * Replace mode may substitute the inferred In-predicate for the original OR, so it keeps the
     * historical requirement that the candidate does not share input slots with sibling conjuncts.
     *
     * Extract mode never drops the original OR, it only appends "X IN (union of literals)". When X is a
     * plain slot (deterministic, evaluated once per row), every disjunct implies its own top-level
     * "X = c" / "X IN (...)", so the whole OR implies "X IN (union)" no matter whether X is also
     * referenced by sibling conjuncts. Under three-valued logic "X IN (union) AND or" is equivalent to
     * "or": or=TRUE implies X IN (union)=TRUE, and X IN (union)=FALSE implies every disjunct is FALSE.
     * A typical shape where the independence check blocks a useful pushdown:
     *   (spm='a' AND ...) OR (... AND spm IN ('c','d') AND ((spm='c' AND ...) OR (spm='d' AND ...)))
     * Here "spm IN ('c','d')" was inferred from the inner OR, which is kept and still references spm.
     */
    private boolean isCandidateConjunct(int idx, List<Expression> conjuncts) {
        if (mode == Mode.extractMode && isSlotCompareWithLiterals(conjuncts.get(idx))) {
            return true;
        }
        return independentConjunct(idx, conjuncts);
    }

    private static boolean isSlotCompareWithLiterals(Expression conjunct) {
        if (conjunct instanceof EqualTo) {
            EqualTo eq = (EqualTo) conjunct;
            return (eq.left() instanceof Slot && eq.right() instanceof Literal
                            && isInferenceSafeScalarType(eq.left())
                            && isInferenceSafeScalarType(eq.right()))
                    || (eq.right() instanceof Slot && eq.left() instanceof Literal
                            && isInferenceSafeScalarType(eq.right())
                            && isInferenceSafeScalarType(eq.left()));
        }
        if (conjunct instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) conjunct;
            if (!(inPredicate.getCompareExpr() instanceof Slot)
                    || !isInferenceSafeScalarType(inPredicate.getCompareExpr())) {
                return false;
            }
            for (Expression option : inPredicate.getOptions()) {
                if (!(option instanceof Literal) || !isInferenceSafeScalarType(option)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Whether the type of expr admits the relaxation.
     *
     * Both sides are inspected when possible: the slot decides how "=" is evaluated and the literal
     * decides what IN puts into its hash set, so a type is only safe when neither side vetoes it.
     */
    private static boolean isInferenceSafeScalarType(Expression expr) {
        if (expr instanceof Unbound) {
            // UnboundSlot throws on getDataType(). Planning always binds slots before this rule
            // runs, so an unknown type only shows up in unit tests; do not let it veto.
            return true;
        }
        return isInferenceSafeScalarType(expr.getDataType());
    }

    /**
     * Whether "X = c" and "X IN (c1, c2, ...)" agree for this type, which the three-valued
     * argument above depends on.
     *
     * Only plain scalar types are accepted. Complex types (ARRAY/STRUCT/MAP) are excluded because
     * their IN is executed by FunctionCollectionIn, which drops the null map of the left column
     * instead of propagating it. "a IN ([1], [2])" then returns a non-null FALSE for a NULL row
     * where "a = [1]" returns NULL, so "a IN (...) AND or" is no longer equivalent to "or" and a
     * NULL-sensitive parent (IS NULL / NOT / CASE / ...) changes the query result. Equality on a
     * nullable ARRAY is also legal in Doris, so the shape is reachable in practice.
     *
     * Floating point is excluded as a precaution: "X = c" and the hash-based IN may disagree on
     * -0.0 and NaN, which would break the "or=TRUE implies X IN (union)=TRUE" direction.
     *
     * Rejecting a type is never a loss: the conjunct simply falls back to independentConjunct()
     * and gets exactly the pre-change behavior.
     */
    private static boolean isInferenceSafeScalarType(DataType type) {
        return type.isIntegralType() // TINYINT / SMALLINT / INT / BIGINT / LARGEINT
                || type.isDecimalLikeType() // DECIMALV2 / DECIMALV3
                || type.isDateLikeType() // DATE / DATETIME / DATEV2 / DATETIMEV2 / TIMESTAMP_NS / TIMESTAMPTZ
                || type.isTimeType() // TIMEV2
                || type.isStringLikeType() // CHAR / VARCHAR / STRING
                || type.isBooleanType();
    }

    /**
     * Register "compareExpr -> literals" as a candidate of the current disjunct.
     *
     * A second candidate for the same compareExpr (e.g. "A=1 and A in (1, 2)") is a pattern for the
     * SimplifyRange rule, so OrToIn gives up on that expression. Historically that was done by clearing
     * every candidate of the disjunct and stopping, which was harmless because the independence check
     * never let two conjuncts on the same slot get here. The shared-slot relaxation makes it reachable,
     * and clearing everything would also throw away unrelated slots (e.g. "b = 10" in
     * "b = 10 and a = 1 and a in (1, 2)"), losing an inference that existed before the relaxation.
     * So only the conflicting compareExpr is dropped, and it is remembered in {@code conflicted} so that
     * a third conjunct on it cannot bring it back. Dropping a candidate only weakens the appended
     * In-predicate, never its correctness.
     */
    private static void addCandidate(Map<Expression, Set<Literal>> candidates, Set<Expression> conflicted,
            Expression compareExpr, Set<Literal> literals) {
        if (conflicted.contains(compareExpr)) {
            return;
        }
        if (!candidates.containsKey(compareExpr)) {
            candidates.put(compareExpr, literals);
            return;
        }
        candidates.remove(compareExpr);
        conflicted.add(compareExpr);
    }

    private Map<Expression, Set<Literal>> getCandidates(Expression disjunct) {
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(disjunct);
        Map<Expression, Set<Literal>> candidates = new LinkedHashMap<>();
        Set<Expression> conflicted = new HashSet<>();
        // collect candidates from the first disjunction
        for (int idx = 0; idx < conjuncts.size(); idx++) {
            if (!isCandidateConjunct(idx, conjuncts)) {
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
                    Set<Literal> literals = Sets.newHashSet();
                    literals.add(literal);
                    addCandidate(candidates, conflicted, compareExpr, literals);
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
                    addCandidate(candidates, conflicted, inPredicate.getCompareExpr(), literalOptions);
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
