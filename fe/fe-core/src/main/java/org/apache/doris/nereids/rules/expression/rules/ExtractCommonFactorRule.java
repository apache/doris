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
                 matchesTopType(CompoundPredicate.class).then(ExtractCommonFactorRule::extractCommonFactor)
        );
    }

    private static Expression extractCommonFactor(CompoundPredicate originExpr) {
        // fast return
        if (!(originExpr.left() instanceof CompoundPredicate || originExpr.left() instanceof BooleanLiteral)
                && !(originExpr.right() instanceof CompoundPredicate || originExpr.right() instanceof BooleanLiteral)) {
            return originExpr;
        }

        // flatten same type to a list
        // e.g. ((a and (b or c)) and c) -> [a, (b or c), c]
        List<Expression> flatten = ExpressionUtils.extract(originExpr);

        // combine and delete some boolean literal predicate
        // e.g. (a and true) -> true
        Expression simplified = ExpressionUtils.combineAsLeftDeepTree(originExpr.getClass(), flatten);
        if (!(simplified instanceof CompoundPredicate)) {
            return simplified;
        }

        // separate two levels CompoundPredicate to partitions
        // e.g. ((a and (b or c)) and c) -> [[a], [b, c], c]
        CompoundPredicate leftDeapTree = (CompoundPredicate) simplified;
        ImmutableSet.Builder<List<Expression>> partitionsBuilder
                = ImmutableSet.builderWithExpectedSize(flatten.size());
        for (Expression onPartition : ExpressionUtils.extract(leftDeapTree)) {
            if (onPartition instanceof CompoundPredicate) {
                partitionsBuilder.add(ExpressionUtils.extract((CompoundPredicate) onPartition));
            } else {
                partitionsBuilder.add(ImmutableList.of(onPartition));
            }
        }
        Set<List<Expression>> partitions = partitionsBuilder.build();

        Expression result = extractCommonFactors(originExpr, leftDeapTree, Utils.fastToImmutableList(partitions));
        return result;
    }

    private static Expression extractCommonFactors(CompoundPredicate originPredicate,
            CompoundPredicate leftDeapTreePredicate, List<List<Expression>> initPartitions) {
        // extract factor and fill into commonFactorToPartIds
        // e.g.
        //      originPredicate:         (a and (b and c)) and (b or c)
        //      leftDeapTreePredicate:   ((a and b) and c) and (b or c)
        //      initPartitions: [[a], [b], [c], [b, c]]
        //
        //   -> commonFactorToPartIds = {a: [0], b: [1, 3], c: [2, 3]}.
        //      so we can know `b` and `c` is a common factors
        SetMultimap<Expression, Integer> commonFactorToPartIds = Multimaps.newSetMultimap(
                Maps.newLinkedHashMap(), LinkedHashSet::new
        );
        int originExpressionNum = 0;
        int partId = 0;
        for (List<Expression> partition : initPartitions) {
            for (Expression expression : partition) {
                commonFactorToPartIds.put(expression, partId);
                originExpressionNum++;
            }
            partId++;
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

        int extractedExpressionNum = 0;
        for (Set<Expression> exprs : commonFactorPartitions.values()) {
            extractedExpressionNum += exprs.size();
        }

        // no any common factor
        if (commonFactorPartitions.entrySet().iterator().next().getKey().size() <= 1
                && !(originPredicate.getWidth() > leftDeapTreePredicate.getWidth())
                && originExpressionNum <= extractedExpressionNum) {
            // this condition is important because it can avoid deap loop:
            // origin originExpr:               A = 1 and (B > 0 and B < 10)
            // after ExtractCommonFactorRule:   (A = 1 and B > 0) and (B < 10)     (left deap tree)
            // after SimplifyRange:             A = 1 and (B > 0 and B < 10)       (right deap tree)
            return originPredicate;
        }

        // now we can do extract common factors for each part:
        //    originPredicate:         (a and (b and c)) and (b or c)
        //    leftDeapTreePredicate:   ((a and b) and c) and (b or c)
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
            Expression extracted = doExtractCommonFactors(
                    leftDeapTreePredicate, initPartitions, kv.getKey(), kv.getValue()
            );
            extractedExprs.add(extracted);
        }

        // combine and eliminate some boolean literal predicate
        return ExpressionUtils.combineAsLeftDeepTree(leftDeapTreePredicate.getClass(), extractedExprs.build());
    }

    private static Expression doExtractCommonFactors(
            CompoundPredicate originPredicate,
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
            Expression partitionWithoutCommonFactor
                    = ExpressionUtils.combineAsLeftDeepTree(originPredicate.flipType(), uncorrelated);
            if (partitionWithoutCommonFactor instanceof CompoundPredicate) {
                partitionWithoutCommonFactor = extractCommonFactor((CompoundPredicate) partitionWithoutCommonFactor);
            }
            uncorrelatedExprPartitionsBuilder.add(partitionWithoutCommonFactor);
        }

        ImmutableList<Expression> uncorrelatedExprPartitions = uncorrelatedExprPartitionsBuilder.build();
        ImmutableList.Builder<Expression> allExprs = ImmutableList.builderWithExpectedSize(commonFactors.size() + 1);
        allExprs.addAll(commonFactors);

        Expression combineUncorrelatedExpr = ExpressionUtils.combineAsLeftDeepTree(
                originPredicate.getClass(), uncorrelatedExprPartitions);
        allExprs.add(combineUncorrelatedExpr);

        Expression result = ExpressionUtils.combineAsLeftDeepTree(originPredicate.flipType(), allExprs.build());
        return result;
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
