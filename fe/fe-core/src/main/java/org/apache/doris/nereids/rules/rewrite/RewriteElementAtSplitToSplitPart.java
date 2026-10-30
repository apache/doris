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
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SplitByString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SplitPart;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;

import com.google.common.collect.ImmutableList;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Rewrite `element_at(split_by_string(str, sep), n) OP rhs` to
 * `split_part(str, sep, n) OP rhs`, when the rewrite is semantics-preserving.
 *
 * <p>split_by_string builds the full array (a memcpy + PODArray allocation per part,
 * per row) and element_at then picks one entry; split_part short-circuits as soon as
 * it finds the n-th delimiter, so the rewrite eliminates the intermediate array and
 * the trailing-parts work. This is a pure expression rewrite — no per-column, per-SQL,
 * or per-function specialization.
 *
 * <p><b>Where the two forms actually differ.</b> With a non-empty separator and a
 * positive index the two agree on every input except one: when the string yields no
 * n-th part, element_at returns NULL while split_part can return the empty string ''.
 * Concretely {@code split_by_string('', ',')} is an empty array so
 * {@code element_at(..., 1)} is NULL, whereas {@code split_part('', ',', 1)} returns ''
 * (an unmatched delimiter makes the whole — here empty — string the first part).
 * A negative index is a second source of divergence and is NOT rewritten (see the
 * index guard below).
 *
 * <p><b>Why the rewrite is still safe under the guards below.</b> We only touch the
 * root expression of a top-level filter conjunct. A conjunct must evaluate to TRUE for
 * a row to survive, so at that position NULL and false are indistinguishable — both
 * drop the row. Replacing element_at's NULL with split_part's '' is therefore
 * invisible as long as the comparison against '' yields false rather than true, i.e.
 * the RHS is a non-empty string literal (for '=') or an IN list of non-empty string
 * literals. We deliberately do NOT recurse into the conjunct: inside NOT / OR / CASE
 * (any non-monotone or NULL-vs-false-sensitive context) NULL and '' are no longer
 * interchangeable — e.g. {@code NOT(element_at(...)= 'a')} is NULL (drops the row)
 * while {@code NOT(split_part(...)= 'a')} would be true (keeps it).
 *
 * <p>Additional guards: the separator must be a non-empty string literal (an empty
 * separator makes split_by_string split into characters while split_part returns '',
 * and a column separator could be empty at runtime); the index must be a non-negative
 * int-like literal — for n >= 1 split_part counts parts forward exactly like
 * split_by_string, and n = 0 is unconditionally NULL in both, while a negative
 * (back-counting) index uses rfind and can land on a different boundary for a
 * self-overlapping multi-char separator (e.g. 'aaa' with 'aa': element_at(-1) = 'a'
 * but split_part(-1) = '').
 */
public class RewriteElementAtSplitToSplitPart implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.REWRITE_ELEMENT_AT_SPLIT_TO_SPLIT_PART.build(
                        logicalFilter().then(filter -> {
                            Set<Expression> newConjuncts =
                                    new LinkedHashSet<>(filter.getConjuncts().size());
                            boolean changed = false;
                            for (Expression conjunct : filter.getConjuncts()) {
                                Expression rewritten = rewriteConjunct(conjunct);
                                if (rewritten != conjunct) {
                                    changed = true;
                                }
                                newConjuncts.add(rewritten);
                            }
                            return changed ? filter.withConjuncts(newConjuncts) : filter;
                        })
                )
        );
    }

    private static Expression rewriteConjunct(Expression conjunct) {
        if (conjunct instanceof EqualTo) {
            return rewriteEqualTo((EqualTo) conjunct);
        }
        if (conjunct instanceof InPredicate) {
            return rewriteInPredicate((InPredicate) conjunct);
        }
        return conjunct;
    }

    private static Expression rewriteEqualTo(EqualTo equalTo) {
        // NormalizeBinaryPredicatesRule puts the literal on the right, but check both
        // sides to stay correct regardless of ordering.
        Expression rewritten = tryRewriteEq(equalTo.left(), equalTo.right());
        if (rewritten != null) {
            return rewritten;
        }
        rewritten = tryRewriteEq(equalTo.right(), equalTo.left());
        if (rewritten != null) {
            return rewritten;
        }
        return equalTo;
    }

    private static Expression tryRewriteEq(Expression value, Expression rhs) {
        if (!(rhs instanceof StringLikeLiteral) || ((StringLikeLiteral) rhs).getValue().isEmpty()) {
            return null;
        }
        SplitCall split = matchElementAtSplit(value);
        if (split == null) {
            return null;
        }
        return new EqualTo(new SplitPart(split.str, split.sep, split.index), rhs);
    }

    private static Expression rewriteInPredicate(InPredicate in) {
        SplitCall split = matchElementAtSplit(in.getCompareExpr());
        if (split == null || !allNonEmptyStringLiterals(in.getOptions())) {
            return in;
        }
        return new InPredicate(new SplitPart(split.str, split.sep, split.index), in.getOptions());
    }

    private static SplitCall matchElementAtSplit(Expression expr) {
        if (!(expr instanceof ElementAt)) {
            return null;
        }
        ElementAt at = (ElementAt) expr;
        if (!(at.child(0) instanceof SplitByString)) {
            return null;
        }
        // Index must be a non-negative int-like literal so split_part's INT arg binds
        // without an extra runtime cast; parser emits TinyIntLiteral for small ints, so
        // match the whole IntegerLikeLiteral hierarchy and normalize to IntegerLiteral.
        // n >= 1: split_part counts parts forward exactly like split_by_string, so the
        // n-th part matches the array element. n = 0: element_at(arr, 0) and
        // split_part(s, sep, 0) are both unconditionally NULL, so the rewrite is exact
        // and additionally skips building the array. Negative n is NOT rewritten --
        // split_part's back-counting uses rfind, which for a self-overlapping multi-char
        // separator lands on a different boundary than split_by_string's non-overlapping
        // forward split (e.g. 'aaa' with 'aa': split_by_string -> ['', 'a'] so
        // element_at(-1) = 'a', but split_part(-1) = '').
        if (!(at.child(1) instanceof IntegerLikeLiteral)) {
            return null;
        }
        long value = ((IntegerLikeLiteral) at.child(1)).getLongValue();
        if (value < 0 || value != (int) value) {
            return null;
        }
        SplitByString sbs = (SplitByString) at.child(0);
        Expression sep = sbs.child(1);
        if (!(sep instanceof StringLikeLiteral) || ((StringLikeLiteral) sep).getValue().isEmpty()) {
            return null;
        }
        return new SplitCall(sbs.child(0), sep, new IntegerLiteral((int) value));
    }

    private static boolean allNonEmptyStringLiterals(List<Expression> options) {
        if (options.isEmpty()) {
            return false;
        }
        for (Expression option : options) {
            if (!(option instanceof StringLikeLiteral)
                    || ((StringLikeLiteral) option).getValue().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static class SplitCall {
        final Expression str;
        final Expression sep;
        final Expression index;

        SplitCall(Expression str, Expression sep, Expression index) {
            this.str = str;
            this.sep = sep;
            this.index = index;
        }
    }
}
