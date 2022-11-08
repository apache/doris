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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.ColumnBound;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.planner.PartitionPruner;
import org.apache.doris.planner.RangePartitionPrunerV2;
import org.apache.doris.planner.ScanNode.ColumnRanges;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Used to prune partition of olap scan, should execute after SwapProjectAndFilter, MergeConsecutiveFilters,
 * MergeConsecutiveProjects and all predicate push down related rules.
 */
public class PruneOlapScanPartition extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalOlapScan()).when(p -> !p.child().isPartitionPruned()).thenApply(ctx -> {
            LogicalFilter<LogicalOlapScan> filter = ctx.root;
            LogicalOlapScan scan = filter.child();
            Expression predicate = filter.getPredicates();
            OlapTable table = scan.getTable();
            Set<String> partitionColumnNameSet = Utils.execWithReturnVal(table::getPartitionColumnNames);
            PartitionInfo partitionInfo = table.getPartitionInfo();
            // TODO: 1. support grammar: SELECT * FROM tbl PARTITION(p1,p2)
            //       2. support list partition
            if (partitionColumnNameSet.isEmpty() || !partitionInfo.getType().equals(PartitionType.RANGE)) {
                return ctx.root;
            }
            List<Expression> expressionList = ExpressionUtils.extractConjunction(predicate);
            // TODO: Process all partition column for now, better to process required column only.
            Map<String, ColumnRange> columnNameToRange = Maps.newHashMap();
            for (String colName : partitionColumnNameSet) {
                ColumnRange columnRange = createColumnRange(colName, expressionList);
                columnNameToRange.put(colName, columnRange);
            }

            Map<Long, PartitionItem> keyItemMap = partitionInfo.getIdToItem(false);
            PartitionPruner partitionPruner = new RangePartitionPrunerV2(keyItemMap,
                    partitionInfo.getPartitionColumns(), columnNameToRange);
            Collection<Long> selectedPartitionId = Utils.execWithReturnVal(partitionPruner::prune);
            LogicalOlapScan rewrittenScan =
                    scan.withSelectedPartitionId(new ArrayList<>(selectedPartitionId));
            return new LogicalFilter<>(filter.getPredicates(), rewrittenScan);
        }).toRule(RuleType.OLAP_SCAN_PARTITION_PRUNE);
    }

    private ColumnRange createColumnRange(String colName, List<Expression> expressionList) {
        ColumnRange result = ColumnRange.create();
        for (Expression expression : expressionList) {
            Set<SlotReference> slotReferences = expression.collect(SlotReference.class::isInstance);
            if (slotReferences.size() != 1 || !slotReferences.iterator().next().getName().equals(colName)) {
                continue;
            }
            if (expression instanceof Or) {
                List<Expression> disjunctiveList = ExpressionUtils.extractDisjunction(expression);
                if (disjunctiveList.isEmpty()) {
                    continue;
                }
                List<Range<ColumnBound>> disjunctiveRanges = Lists.newArrayList();
                Set<Boolean> hasIsNull = Sets.newHashSet();
                boolean allMatch = disjunctiveList.stream().allMatch(e -> {
                    ColumnRanges ranges = exprToRanges(e, colName);
                    switch (ranges.type) {
                        case IS_NULL:
                            hasIsNull.add(true);
                            return true;
                        case CONVERT_SUCCESS:
                            disjunctiveRanges.addAll(ranges.ranges);
                            return true;
                        case CONVERT_FAILURE:
                        default:
                            return false;
                    }
                });
                if (allMatch && !(disjunctiveRanges.isEmpty() && hasIsNull.isEmpty())) {
                    result.intersect(disjunctiveRanges);
                    result.setHasDisjunctiveIsNull(!hasIsNull.isEmpty());
                }
            } else {
                ColumnRanges ranges = exprToRanges(expression, colName);
                switch (ranges.type) {
                    case IS_NULL:
                        result.setHasConjunctiveIsNull(true);
                        break;
                    case CONVERT_SUCCESS:
                        result.intersect(ranges.ranges);
                        break;
                    case CONVERT_FAILURE:
                    default:
                        break;
                }
            }
        }
        return result;
    }

    private ColumnRanges exprToRanges(Expression expression, String colName) {
        // TODO: process in/is null expression
        if (!(expression instanceof ComparisonPredicate)) {
            return ColumnRanges.createFailure();
        }
        List<Range<ColumnBound>> result = Lists.newArrayList();
        ComparisonPredicate comparisonPredicate = (ComparisonPredicate) expression;
        Expression rightChild = comparisonPredicate.child(1);
        if (rightChild == null || !rightChild.isConstant() || !(rightChild instanceof Literal)) {
            return ColumnRanges.createFailure();
        }
        LiteralExpr value = ((Literal) rightChild).toLegacyLiteral();
        if (expression instanceof EqualTo) {
            ColumnBound bound = ColumnBound.of(value);
            result.add(Range.closed(bound, bound));
        } else if (expression instanceof GreaterThanEqual) {
            result.add(Range.atLeast(ColumnBound.of(value)));
        } else if (expression instanceof GreaterThan) {
            result.add(Range.greaterThan(ColumnBound.of(value)));
        } else if (expression instanceof LessThan) {
            result.add(Range.lessThan(ColumnBound.of(value)));
        } else if (expression instanceof LessThanEqual) {
            result.add(Range.atMost(ColumnBound.of(value)));
        }
        if (result.isEmpty()) {
            return ColumnRanges.createFailure();
        } else {
            return ColumnRanges.create(result);
        }
    }
}
