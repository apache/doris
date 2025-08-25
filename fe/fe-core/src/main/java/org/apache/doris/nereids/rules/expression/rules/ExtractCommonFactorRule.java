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
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.rules.rewrite.SkipSimpleExprs;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Extract common expr for `CompoundPredicate`.
 * for example:
 * transform (a or b) and (a or c) to a or (b and c)
 * transform (a and b) or (a and c) to a and (b or c)
 */
@Developing
public class ExtractCommonFactorRule implements ExpressionPatternRuleFactory {
    public static final ExtractCommonFactorRule INSTANCE = new ExtractCommonFactorRule();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                 matchesTopType(CompoundPredicate.class)
                         .then(ExtractCommonFactorRule::extractCommonFactor)
                         .toRule(ExpressionRuleType.EXTRACT_COMMON_FACTOR)
        );
    }

    /** extractCommonFactor */
    public static Expression extractCommonFactor(CompoundPredicate originExpr) {
        // fast return
        boolean canExtract = false;
        Set<Expression> childrenSet = new LinkedHashSet<>();
        for (Expression child : originExpr.children()) {
            if ((child instanceof CompoundPredicate || child instanceof BooleanLiteral)) {
                canExtract = true;
            }
            childrenSet.add(child);
        }
        if (!canExtract) {
            if (childrenSet.size() != originExpr.children().size()) {
                if (childrenSet.size() == 1) {
                    return childrenSet.iterator().next();
                } else {
                    return originExpr.withChildren(Utils.fastToImmutableList(childrenSet));
                }
            }
            return originExpr;
        } else if (SkipSimpleExprs.isSimpleExpr(originExpr)) {
            return originExpr;
        }
        // flatten same type to a list
        // e.g. ((a and (b or c)) and c) -> [a, (b or c), c]
        List<Expression> flatten = ExpressionUtils.extract(originExpr);

        // combine and delete some boolean literal predicate
        // e.g. (a and true) -> a
        Expression simplified = ExpressionUtils.compound(originExpr instanceof And, flatten);
        if (!(simplified instanceof CompoundPredicate)) {
            return simplified;
        }

        // separate two levels CompoundPredicate to partitions
        // e.g. ((a and (b or c)) and c) -> [[a], [b, c], c]
        ImmutableSet.Builder<List<Expression>> partitionsBuilder
                = ImmutableSet.builderWithExpectedSize(simplified.children().size());
        for (Expression onPartition : simplified.children()) {
            if (onPartition instanceof CompoundPredicate) {
                partitionsBuilder.add(ExpressionUtils.extract((CompoundPredicate) onPartition));
            } else {
                partitionsBuilder.add(ImmutableList.of(onPartition));
            }
        }
        Set<List<Expression>> partitions = partitionsBuilder.build();

        return extractCommonFactors(simplified instanceof And, Utils.fastToImmutableList(partitions));
    }

    private static Expression extractCommonFactors(boolean isAnd, List<List<Expression>> initPartitions) {
        // extract factor and fill into commonFactorToPartIds
        // e.g.
        //      originPredicate:         (a and (b and c)) and (b or c)
        //      initPartitions: [[a], [b], [c], [b, c]]
        //
        //   -> commonFactorToPartIds = {a: [0], b: [1, 3], c: [2, 3]}.
        //      so we can know `b` and `c` is a common factors
        SetMultimap<Expression, Integer> commonFactorToPartIds = Multimaps.newSetMultimap(
                Maps.newLinkedHashMap(), LinkedHashSet::new
        );
        for (int i = 0; i < initPartitions.size(); i++) {
            List<Expression> partition = initPartitions.get(i);
            for (Expression expression : partition) {
                commonFactorToPartIds.put(expression, i);
            }
        }

        //     commonFactorToPartIds = {a: [0], b: [1, 3], c: [2, 3]}
        //
        //  -> reverse key value of commonFactorToPartIds and remove intersecting partition
        //
        //  -> 1. reverse: {[0]: [a], [1, 3]: [b], [2, 3]: [c]}
        //  -> 2. sort by key size desc: {[1, 3]: [b], [2, 3]: [c], [0]: [a]}
        //  -> 3. remove intersection partition: {[1, 3]: [b], [2]: [c], [0]: [a]},
        //        because first part and second part intersect by partition 3
        LinkedHashMap<Set<Integer>, Set<Expression>> commonFactorPartitions
                = partitionByMostCommonFactors(commonFactorToPartIds);

        // now we can do extract common factors for each part:
        //    originPredicate:         (a and (b and c)) and (b or c)
        //    initPartitions:          [[a], [b], [c], [b, c]]
        //    commonFactorPartitions:  {[1, 3]: [b], [0]: [a]}
        //
        // -> extractedExprs: [
        //                       b or (false and c) = b,
        //                       a,
        //                       c
        //                    ]
        //
        // -> result: (b or c) and a and c
        ImmutableList.Builder<Expression> extractedExprs
                = ImmutableList.builderWithExpectedSize(commonFactorPartitions.size());
        for (Entry<Set<Integer>, Set<Expression>> kv : commonFactorPartitions.entrySet()) {
            Expression extracted = doExtractCommonFactors(isAnd, initPartitions, kv.getKey(), kv.getValue());
            extractedExprs.add(extracted);
        }

        // combine and eliminate some boolean literal predicate
        return ExpressionUtils.compound(isAnd, extractedExprs.build());
    }

    private static Expression doExtractCommonFactors(boolean isAnd,
            List<List<Expression>> partitions, Set<Integer> partitionIds, Set<Expression> commonFactors) {
        ImmutableList.Builder<Expression> uncorrelatedExprPartitionsBuilder
                = ImmutableList.builderWithExpectedSize(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            List<Expression> partition = partitions.get(partitionId);
            ImmutableSet.Builder<Expression> uncorrelatedBuilder
                    = ImmutableSet.builderWithExpectedSize(partition.size());
            for (Expression exprOfPart : partition) {
                if (!commonFactors.contains(exprOfPart)) {
                    uncorrelatedBuilder.add(exprOfPart);
                }
            }

            Set<Expression> uncorrelated = uncorrelatedBuilder.build();
            Expression partitionWithoutCommonFactor = ExpressionUtils.compound(!isAnd, uncorrelated);
            if (partitionWithoutCommonFactor instanceof CompoundPredicate) {
                partitionWithoutCommonFactor = extractCommonFactor((CompoundPredicate) partitionWithoutCommonFactor);
            }
            uncorrelatedExprPartitionsBuilder.add(partitionWithoutCommonFactor);
        }

        // common factors should be flip of isAnd
        Expression combineCommonFactor = ExpressionUtils.compound(!isAnd, commonFactors);
        if (combineCommonFactor instanceof CompoundPredicate) {
            combineCommonFactor = extractCommonFactor((CompoundPredicate) combineCommonFactor);
        }
        List<Expression> rewriteCommonFactors = isAnd ? ExpressionUtils.extractDisjunction(combineCommonFactor)
                : ExpressionUtils.extractConjunction(combineCommonFactor);

        ImmutableList<Expression> uncorrelatedExprPartitions = uncorrelatedExprPartitionsBuilder.build();
        Expression combineUncorrelatedExpr = ExpressionUtils.compound(isAnd, uncorrelatedExprPartitions);

        ImmutableList.Builder<Expression> allExprs = ImmutableList.builderWithExpectedSize(
                rewriteCommonFactors.size() + 1);
        allExprs.addAll(rewriteCommonFactors);
        allExprs.add(combineUncorrelatedExpr);

        return ExpressionUtils.compound(!isAnd, allExprs.build());
    }

    private static LinkedHashMap<Set<Integer>, Set<Expression>> partitionByMostCommonFactors(
            SetMultimap<Expression, Integer> commonFactorToPartIds) {
        SetMultimap<Set<Integer>, Expression> partWithCommonFactors = Multimaps.newSetMultimap(
                Maps.newLinkedHashMap(), LinkedHashSet::new
        );

        for (Entry<Expression, Collection<Integer>> factorToId : commonFactorToPartIds.asMap().entrySet()) {
            partWithCommonFactors.put((Set<Integer>) factorToId.getValue(), factorToId.getKey());
        }

        List<Set<Integer>> sortedPartitionIdHasCommonFactor = Lists.newArrayList(partWithCommonFactors.keySet());
        // place the most common factor at the head of this list
        sortedPartitionIdHasCommonFactor.sort((p1, p2) -> p2.size() - p1.size());

        LinkedHashMap<Set<Integer>, Set<Expression>> shouldExtractFactors = Maps.newLinkedHashMap();

        Set<Integer> allocatedPartitions = Sets.newLinkedHashSet();
        for (Set<Integer> originMostCommonFactorPartitions : sortedPartitionIdHasCommonFactor) {
            ImmutableSet.Builder<Integer> notAllocatePartitions = ImmutableSet.builderWithExpectedSize(
                    originMostCommonFactorPartitions.size());
            for (Integer partId : originMostCommonFactorPartitions) {
                if (allocatedPartitions.add(partId)) {
                    notAllocatePartitions.add(partId);
                }
            }

            Set<Integer> mostCommonFactorPartitions = notAllocatePartitions.build();
            if (!mostCommonFactorPartitions.isEmpty()) {
                Set<Expression> commonFactors = partWithCommonFactors.get(originMostCommonFactorPartitions);
                shouldExtractFactors.put(mostCommonFactorPartitions, commonFactors);
            }
        }

        return shouldExtractFactors;
    }
}
