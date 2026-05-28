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

import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

/**
 * HMS-side prefilter rewriter. Replaces qualifying large {@code IN(slot, lit, lit, ...)} predicates
 * with a {@code (slot >= min) AND (slot <= max)} range so that a giant OR tree is never pushed to
 * HMS. Activation conditions:
 * <ul>
 *   <li>{@link Config#hive_metastore_partition_pruning_in_predicate_threshold} {@code > 0}</li>
 *   <li>IN predicate has more options than the threshold</li>
 *   <li>compare expression is a {@link SlotReference}</li>
 *   <li>every option is a {@link Literal} (NULLs are skipped when computing min/max)</li>
 *   <li>at least one non-null literal exists</li>
 * </ul>
 * If any condition fails the IN predicate is left untouched.
 *
 * <p>The {@link Pair#second} flag in the result signals whether at least one such rewrite happened
 * anywhere in the tree. When {@code true}, callers <strong>must</strong> apply the original
 * (un-rewritten) predicate again on the HMS candidate set to drop false positives.
 */
public class LargeInPredicateToRangeRewriter extends DefaultExpressionRewriter<CascadesContext> {

    private boolean rangePrefilterGenerated;

    private LargeInPredicateToRangeRewriter() {
    }

    /**
     * Rewrite {@code expression} into an HMS-friendly variant. Returns the rewritten expression
     * paired with a flag indicating whether any range prefilter was generated.
     */
    public static Pair<Expression, Boolean> rewrite(Expression expression, CascadesContext cascadesContext) {
        LargeInPredicateToRangeRewriter rewriter = new LargeInPredicateToRangeRewriter();
        Expression rewritten = expression.accept(rewriter, cascadesContext);
        return Pair.of(rewritten, rewriter.rangePrefilterGenerated);
    }

    @Override
    public Expression visitInPredicate(InPredicate in, CascadesContext context) {
        Expression rangePrefilter = tryRewriteLargeInPredicateToRange(in);
        if (rangePrefilter != null) {
            rangePrefilterGenerated = true;
            return rangePrefilter;
        }
        return super.visitInPredicate(in, context);
    }

    private Expression tryRewriteLargeInPredicateToRange(InPredicate inPredicate) {
        if (!(inPredicate.getCompareExpr() instanceof SlotReference)) {
            return null;
        }

        int threshold = Config.hive_metastore_partition_pruning_in_predicate_threshold;
        if (threshold <= 0 || inPredicate.getOptions().size() <= threshold) {
            return null;
        }

        Literal minLiteral = null;
        Literal maxLiteral = null;
        for (Expression option : inPredicate.getOptions()) {
            if (!(option instanceof Literal)) {
                return null;
            }
            if (option instanceof NullLiteral) {
                continue;
            }
            Literal literal = (Literal) option;
            if (minLiteral == null
                    || literal.toLegacyLiteral().compareLiteral(minLiteral.toLegacyLiteral()) < 0) {
                minLiteral = literal;
            }
            if (maxLiteral == null
                    || literal.toLegacyLiteral().compareLiteral(maxLiteral.toLegacyLiteral()) > 0) {
                maxLiteral = literal;
            }
        }

        if (minLiteral == null || maxLiteral == null) {
            return null;
        }

        Expression compareExpr = inPredicate.getCompareExpr();
        return new And(new GreaterThanEqual(compareExpr, minLiteral), new LessThanEqual(compareExpr, maxLiteral));
    }
}
