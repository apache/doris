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
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * (hujie, cmy)
 * ATTN: do not delete it before considering useless for certain
 */
@Deprecated
public class ListPartitionPruner implements PartitionPruner {
    private static final Logger LOG = LoggerFactory.getLogger(ListPartitionPruner.class);

    private Map<Long, List<PartitionKey>> partitionListMap;
    private List<Column>                       partitionColumns;
    private Map<String, PartitionColumnFilter> partitionColumnFilters;

    public ListPartitionPruner(Map<Long, List<PartitionKey>> listMap,
                               List<Column> columns,
                               Map<String, PartitionColumnFilter> filters) {
        partitionListMap = listMap;
        partitionColumns = columns;
        partitionColumnFilters = filters;
    }

    private Collection<Long> pruneListMap(
            Map<Long, List<PartitionKey>> listMap,
            Range<PartitionKey> range) {
        Set<Long> resultSet = Sets.newHashSet();
        for (Map.Entry<Long, List<PartitionKey>> entry : listMap.entrySet()) {
            for (PartitionKey key : entry.getValue()) {
                if (range.contains(key)) {
                    resultSet.add(entry.getKey());
                    break;
                }
            }
        }
        return resultSet;
    }

    private Collection<Long> prune(
            Map<Long, List<PartitionKey>> listMap,
            int columnId,
            PartitionKey minKey,
            PartitionKey maxKey,
            int complex)
            throws AnalysisException {
        if (columnId == partitionColumns.size()) {
            try {
                return pruneListMap(listMap, Range.closed(minKey, maxKey));
            } catch (IllegalArgumentException e) {
                return Lists.newArrayList();
            }
        }
        Column keyColumn = partitionColumns.get(columnId);
        PartitionColumnFilter filter = partitionColumnFilters.get(keyColumn.getName());
        // no filter in this column
        if (null == filter) {
            minKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getDataType()), false),
                    keyColumn.getDataType());
            maxKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getDataType()), true),
                    keyColumn.getDataType());
            Collection<Long> result = null;
            try {
                return pruneListMap(listMap, Range.closed(minKey, maxKey));
            } catch (IllegalArgumentException e) {
                result = Lists.newArrayList();
            }
            minKey.popColumn();
            maxKey.popColumn();
            return result;
        }
        InPredicate inPredicate = filter.getInPredicate();
        if (null == inPredicate || inPredicate.getChildren().size() * complex > 100) {
            if (filter.lowerBoundInclusive && filter.upperBoundInclusive 
                    && filter.lowerBound != null && filter.upperBound != null 
                    && 0 == filter.lowerBound.compareLiteral(filter.upperBound)) {
                minKey.pushColumn(filter.lowerBound, keyColumn.getDataType());
                maxKey.pushColumn(filter.upperBound, keyColumn.getDataType());
                Collection<Long> result =
                        prune(listMap, columnId + 1, minKey, maxKey, complex);
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
                if (filter.lowerBoundInclusive && columnId != lastColumnId) {
                    Column column = partitionColumns.get(columnId + 1);
                    minKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(column.getDataType()), false),
                            column.getDataType());
                    isPushMin = true;
                }
            } else {
                minKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getDataType()), false),
                        keyColumn.getDataType());
                isPushMin = true;
            }
            if (filter.upperBound != null) {
                maxKey.pushColumn(filter.upperBound, keyColumn.getDataType());
                if (filter.upperBoundInclusive && columnId != lastColumnId) {
                    Column column = partitionColumns.get(columnId + 1);
                    maxKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(column.getDataType()), true),
                            column.getDataType());
                    isPushMax = true;
                }
            } else {
                maxKey.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(keyColumn.getDataType()), true),
                        keyColumn.getDataType());
                isPushMax = true;
            }

            Collection<Long> result = null;
            try {
                return pruneListMap(listMap, Range.range(minKey, lowerType, maxKey, upperType));
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
            resultSet.addAll(prune(listMap, columnId + 1, minKey, maxKey, complex));
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

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
