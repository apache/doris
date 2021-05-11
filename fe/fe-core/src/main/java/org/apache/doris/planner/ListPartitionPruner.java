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

package org.apache.doris.planner;

import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * list partition pruner
 */
public class ListPartitionPruner implements PartitionPruner {

    private Map<Long, PartitionItem> partitionListMap;
    private List<Column>                       partitionColumns;
    private Map<String, PartitionColumnFilter> partitionColumnFilters;

    public ListPartitionPruner(Map<Long, PartitionItem> listMap,
                               List<Column> columns,
                               Map<String, PartitionColumnFilter> filters) {
        partitionListMap = listMap;
        partitionColumns = columns;
        partitionColumnFilters = filters;
    }

    private Collection<Long> pruneListMap(Map<Long, PartitionItem> listMap,
                                          Range<PartitionKey> range,
                                          int columnId) {
        Set<Long> resultSet = Sets.newHashSet();
        for (Map.Entry<Long, PartitionItem> entry : listMap.entrySet()) {
            List<PartitionKey> partitionKeys = entry.getValue().getItems();
            for (PartitionKey partitionKey : partitionKeys) {
                LiteralExpr expr = partitionKey.getKeys().get(columnId);
                if (contain(range, expr, columnId)) {
                    resultSet.add(entry.getKey());
                }
            }
        }
        return resultSet;
    }

    /**
     * check literal expr exist in partition range
     * @param range the partition key range
     * @param literalExpr expr to be checked
     * @param columnId expr column index in partition key
     * @return
     */
    private boolean contain(Range<PartitionKey> range, LiteralExpr literalExpr, int columnId) {
        LiteralExpr lowerExpr = range.lowerEndpoint().getKeys().get(columnId);
        LiteralExpr upperExpr = range.upperEndpoint().getKeys().get(columnId);
        BoundType lType = range.lowerBoundType();
        BoundType uType = range.upperBoundType();
        int ret1 = PartitionKey.compareLiteralExpr(literalExpr, lowerExpr);
        int ret2 = PartitionKey.compareLiteralExpr(literalExpr, upperExpr);

        if (lType == BoundType.CLOSED && uType == BoundType.CLOSED) {
            if (ret1 >= 0 && ret2 <= 0) {
                return true;
            }
        } else if (lType == BoundType.CLOSED && uType == BoundType.OPEN) {
            if (ret1 >= 0 && ret2 < 0) {
                return true;
            }
        } else if (lType == BoundType.OPEN && uType == BoundType.CLOSED) {
            if (ret1 > 0 && ret2 <= 0) {
                return true;
            }
        } else if (lType == BoundType.OPEN && uType == BoundType.OPEN) {
            if (ret1 > 0 && ret2 < 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * get min literal expr from partition key list map by partition key column id.
     * @param columnId
     * @return
     */
    private LiteralExpr getMinLiteral(int columnId) {
        LiteralExpr minLiteral = null;
        for (Map.Entry<Long, PartitionItem> entry : partitionListMap.entrySet()) {
            List<PartitionKey> partitionKeys = entry.getValue().getItems();
            for (PartitionKey partitionKey : partitionKeys) {
                minLiteral = getMinExpr(partitionKey.getKeys().get(columnId), minLiteral);
            }
        }
        return minLiteral;
    }

    private LiteralExpr getMinExpr(LiteralExpr expr, LiteralExpr minLiteral) {
        if (minLiteral == null) {
            minLiteral = expr;
            return minLiteral;
        }
        if (expr.compareLiteral(minLiteral) < 0) {
            minLiteral = expr;
        }
        return minLiteral;
    }

    private Collection<Long> prune(
            Map<Long, PartitionItem> listMap,
            int columnId,
            PartitionKey minKey,
            PartitionKey maxKey,
            int complex)
            throws AnalysisException {
        // if partition item map is empty, no need to prune.
        if (listMap.size() == 0) {
            return Lists.newArrayList();
        }

        if (columnId == partitionColumns.size()) {
            try {
                return pruneListMap(listMap, Range.closed(minKey, maxKey), columnId - 1);
            } catch (IllegalArgumentException e) {
                return Lists.newArrayList();
            }
        }
        Column keyColumn = partitionColumns.get(columnId);
        PartitionColumnFilter filter = partitionColumnFilters.get(keyColumn.getName());
        // no filter in this column
        if (null == filter) {
            minKey.pushColumn(getMinLiteral(columnId), keyColumn.getDataType());
            maxKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getDataType()), true),
                                keyColumn.getDataType());
            Collection<Long> result = null;
            try {
                // prune next partition column
                result = prune(listMap, columnId + 1, minKey, maxKey, complex);
            } catch (IllegalArgumentException e) {
                result = Lists.newArrayList();
            }
            minKey.popColumn();
            maxKey.popColumn();
            return result;
        }
        InPredicate inPredicate = filter.getInPredicate();
        if (null == inPredicate || inPredicate.getChildren().size() * complex > 100) {
            // case: where k1 = 1;
            if (filter.lowerBoundInclusive && filter.upperBoundInclusive 
                    && filter.lowerBound != null && filter.upperBound != null 
                    && 0 == filter.lowerBound.compareLiteral(filter.upperBound)) {
                minKey.pushColumn(filter.lowerBound, keyColumn.getDataType());
                maxKey.pushColumn(filter.upperBound, keyColumn.getDataType());
                // handle like in predicate
                Collection<Long> result = pruneListMap(listMap, Range.closed(minKey, maxKey), columnId);
                // prune next partition column
                if (partitionColumns.size() > 1) {
                    result.retainAll(prune(listMap, columnId + 1, minKey, maxKey, complex));
                }
                minKey.popColumn();
                maxKey.popColumn();
                return result;
            }
            // no in predicate
            BoundType lowerType = filter.lowerBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
            BoundType upperType = filter.upperBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
            boolean isPushMin = false;
            boolean isPushMax = false;
            int lastColumnId = partitionColumns.size() - 1;
            if (filter.lowerBound != null) {
                minKey.pushColumn(filter.lowerBound, keyColumn.getDataType());
            } else {
                minKey.pushColumn(getMinLiteral(columnId),
                        keyColumn.getDataType());
                isPushMin = true;
            }
            if (filter.upperBound != null) {
                maxKey.pushColumn(filter.upperBound, keyColumn.getDataType());
            } else {
                maxKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getDataType()), true),
                        keyColumn.getDataType());
                isPushMax = true;
            }

            Collection<Long> result = null;
            try {
                result = pruneListMap(listMap, Range.range(minKey, lowerType, maxKey, upperType), columnId);
                // prune next partition column
                if (partitionColumns.size() > 1) {
                    result.retainAll(prune(listMap, columnId + 1, minKey, maxKey, complex));
                }
                return result;
            } catch (IllegalArgumentException e) {
                result = Lists.newArrayList();
            }
            if (isPushMin) {
                minKey.popColumn();
            }
            if (isPushMax) {
                maxKey.popColumn();
            }
            return result;
        }
        Set<Long> resultSet = Sets.newHashSet();
        int childrenNum = inPredicate.getChildren().size();
        complex = inPredicate.getChildren().size() * complex;
        for (int i = 1; i < childrenNum; ++i) {
            LiteralExpr expr = (LiteralExpr) inPredicate.getChild(i);
            minKey.pushColumn(expr, keyColumn.getDataType());
            maxKey.pushColumn(expr, keyColumn.getDataType());
            Collection<Long> result = pruneListMap(listMap, Range.closed(minKey, maxKey), columnId);
            // prune next partition column
            if (partitionColumns.size() > 1) {
                // Take the intersection
                result.retainAll(prune(listMap, columnId + 1, minKey, maxKey, complex));
            }
            resultSet.addAll(result);
            minKey.popColumn();
            maxKey.popColumn();
        }
        return resultSet;
    }

    public Collection<Long> prune() throws AnalysisException {
        PartitionKey minKey = new PartitionKey();
        PartitionKey maxKey = new PartitionKey();
        return prune(partitionListMap, 0, minKey, maxKey, 1);
    }
}
