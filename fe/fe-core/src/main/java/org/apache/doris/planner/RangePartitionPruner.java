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
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangePartitionPruner implements PartitionPruner {
    private static final Logger LOG = LogManager.getLogger(RangePartitionPruner.class);

    private Map<Long, PartitionItem> partitionRangeMap;
    private List<Column> partitionColumns;
    private Map<String, PartitionColumnFilter> partitionColumnFilters;

    public RangePartitionPruner(Map<Long, PartitionItem> rangeMap,
                                List<Column> columns,
                                Map<String, PartitionColumnFilter> filters) {
        partitionRangeMap = rangeMap;
        partitionColumns = columns;
        partitionColumnFilters = filters;
    }

    private Collection<Long> prune(RangeMap<PartitionKey, Long> rangeMap,
                                   int columnIdx,
                                   PartitionKey minKey,
                                   PartitionKey maxKey,
                                   int complex)
            throws AnalysisException {
        LOG.debug("column idx {}, column filters {}", columnIdx, partitionColumnFilters);
        // the last column in partition Key
        if (columnIdx == partitionColumns.size()) {
            try {
                return Lists.newArrayList(rangeMap.subRangeMap(Range.closed(minKey, maxKey)).asMapOfRanges().values());
            } catch (IllegalArgumentException e) {
                return Lists.newArrayList();
            }
        }
        // no filter in this column
        Column keyColumn = partitionColumns.get(columnIdx);
        PartitionColumnFilter filter = partitionColumnFilters.get(keyColumn.getName());
        if (null == filter) {
            // If there is no condition in the first column, there is no need to add the infinity range for all columns.
            int pushMinCount;
            int pushMaxCount;
            if (columnIdx == 0) {
                minKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getDataType()), false),
                    keyColumn.getDataType());
                maxKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getDataType()), true),
                    keyColumn.getDataType());
                pushMinCount = 1;
                pushMaxCount = 1;
            } else {
                pushMinCount = minKey.fillWithInfinity(partitionColumns, false);
                pushMaxCount = maxKey.fillWithInfinity(partitionColumns, true);
            }
            Collection<Long> result = null;
            try {
                result = Lists.newArrayList(
                        rangeMap.subRangeMap(Range.closed(minKey, maxKey)).asMapOfRanges().values());
            } catch (IllegalArgumentException e) {
                result = Lists.newArrayList();
            }
            for (; pushMinCount > 0; pushMinCount--) {
                minKey.popColumn();
            }
            for (; pushMaxCount > 0; pushMaxCount--) {
                maxKey.popColumn();
            }
            return result;
        }
        InPredicate inPredicate = filter.getInPredicate();
        if (null == inPredicate || inPredicate.getChildren().size() * complex > 100) {
            if (filter.lowerBoundInclusive && filter.upperBoundInclusive
                    && filter.lowerBound != null && filter.upperBound != null
                    && 0 == filter.lowerBound.compareLiteral(filter.upperBound)) {

                // eg: [10, 10], [null, null]
                if (filter.lowerBound instanceof NullLiteral && filter.upperBound instanceof NullLiteral) {
                    // replace Null with min value
                    LiteralExpr minKeyValue = LiteralExpr.createInfinity(
                            Type.fromPrimitiveType(keyColumn.getDataType()), false);
                    minKey.pushColumn(minKeyValue, keyColumn.getDataType());
                    maxKey.pushColumn(minKeyValue, keyColumn.getDataType());
                } else {
                    minKey.pushColumn(filter.lowerBound, keyColumn.getDataType());
                    maxKey.pushColumn(filter.upperBound, keyColumn.getDataType());
                }
                Collection<Long> result = prune(rangeMap, columnIdx + 1, minKey, maxKey, complex);
                minKey.popColumn();
                maxKey.popColumn();
                return result;
            }

            // no in predicate
            BoundType lowerType = filter.lowerBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
            BoundType upperType = filter.upperBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
            int pushMinCount;
            int pushMaxCount;
            if (filter.lowerBound != null) {
                minKey.pushColumn(filter.lowerBound, keyColumn.getDataType());
                pushMinCount = 1 + minKey.fillWithInfinity(partitionColumns, !filter.lowerBoundInclusive);
            } else {
                pushMinCount = minKey.fillWithInfinity(partitionColumns, false);
            }
            if (filter.upperBound != null) {
                maxKey.pushColumn(filter.upperBound, keyColumn.getDataType());
                pushMaxCount = 1 + maxKey.fillWithInfinity(partitionColumns, filter.upperBoundInclusive);
            } else {
                pushMaxCount = maxKey.fillWithInfinity(partitionColumns, true);
            }

            Collection<Long> result = null;
            try {
                result = Lists.newArrayList(rangeMap.subRangeMap(
                            Range.range(minKey, lowerType, maxKey, upperType)).asMapOfRanges().values());
            } catch (IllegalArgumentException e) {
                result = Lists.newArrayList();
            }

            for (; pushMinCount > 0; pushMinCount--) {
                minKey.popColumn();
            }
            for (; pushMaxCount > 0; pushMaxCount--) {
                maxKey.popColumn();
            }
            return result;
        }
        Set<Long> resultSet = Sets.newHashSet();
        int childrenNum = inPredicate.getChildren().size();
        int newComplex = inPredicate.getChildren().size() * complex;
        for (int i = 1; i < childrenNum; ++i) {
            LiteralExpr expr = (LiteralExpr) inPredicate.getChild(i);
            minKey.pushColumn(expr, keyColumn.getDataType());
            maxKey.pushColumn(expr, keyColumn.getDataType());
            Collection<Long> subList = prune(rangeMap, columnIdx + 1, minKey, maxKey, newComplex);
            for (long partId : subList) {
                resultSet.add(partId);
            }
            minKey.popColumn();
            maxKey.popColumn();
        }

        return resultSet;
    }

    public Collection<Long> prune() throws AnalysisException {
        PartitionKey minKey = new PartitionKey();
        PartitionKey maxKey = new PartitionKey();
        // Map to RangeMapTree
        RangeMap<PartitionKey, Long> rangeMap = TreeRangeMap.create();
        for (Map.Entry<Long, PartitionItem> entry : partitionRangeMap.entrySet()) {
            rangeMap.put(entry.getValue().getItems(), entry.getKey());
        }
        return prune(rangeMap, 0, minKey, maxKey, 1);
    }
}
