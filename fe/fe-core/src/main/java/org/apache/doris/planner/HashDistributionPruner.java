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
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * Prune the distribution by distribution columns' predicate, recursively.
 * It only supports binary equal predicate and in predicate with AND combination.
 * For example:
 *      where a = 1 and b in (2,3,4) and c in (5,6,7)
 *      a/b/c are distribution columns
 *
 * the config 'max_distribution_pruner_recursion_depth' will limit the max recursion depth of pruning.
 * the recursion depth is calculated by the product of element number of all predicates.
 * The above example's depth is 9(= 1 * 3 * 3)
 *
 * If depth is larger than 'max_distribution_pruner_recursion_depth', all buckets will be return without pruning.
 */
public class HashDistributionPruner implements DistributionPruner {
    private static final Logger LOG = LogManager.getLogger(HashDistributionPruner.class);

    // Tablet snapshot in hash bucket order.
    private final List<Tablet> tablets;
    private final int bucketNum;
    // partition columns
    private final List<Column> distributionColumns;
    // partition column filters
    private final Map<String, PartitionColumnFilter> distributionColumnFilters;
    private final int hashMod;

    public HashDistributionPruner(List<Column> schema, MaterializedIndex materializedIndex, List<Column> columns,
            Map<String, PartitionColumnFilter> filters, int hashMod, boolean isBaseIndexSelected) {
        this.tablets = materializedIndex.getTablets();
        this.bucketNum = tablets.size();
        this.distributionColumns = columns;
        this.hashMod = hashMod;
        if (isBaseIndexSelected) {
            this.distributionColumnFilters = filters;
        } else {
            this.distributionColumnFilters = new CaseInsensitiveMap();
            Map<String, String> mvToBaseColumnName = new HashMap<>();
            for (Column col : schema) {
                mvToBaseColumnName.put(col.getName(), col.tryGetBaseColumnName());
            }
            for (Map.Entry<String, PartitionColumnFilter> filter : filters.entrySet()) {
                String baseColName = mvToBaseColumnName.get(filter.getKey());
                if (baseColName != null) {
                    this.distributionColumnFilters.put(baseColName, filter.getValue());
                }
            }
        }
    }

    // columnId: which column to compute
    // hashKey: the key which to compute hash value
    public Collection<Long> prune(int columnId, PartitionKey hashKey, int complex) {
        if (columnId == distributionColumns.size()) {
            // compute Hash Key
            long hashValue = hashKey.getHashValue();
            return Lists.newArrayList(getTabletId((int) ((hashValue & 0xffffffff) % hashMod)));
        }
        Column keyColumn = distributionColumns.get(columnId);
        PartitionColumnFilter filter = distributionColumnFilters.get(keyColumn.getName());
        if (null == filter) {
            // no filter in this column, no partition Key
            // return all subPartition
            return getAllTabletIds();
        }
        InPredicate inPredicate = filter.getInPredicate();
        if (null == inPredicate
                || inPredicate.getInElementNum() * complex > Config.max_distribution_pruner_recursion_depth) {
            // equal one value
            if (filter.lowerBoundInclusive && filter.upperBoundInclusive
                    && filter.lowerBound != null && filter.upperBound != null
                    && 0 == filter.lowerBound.compareLiteral(filter.upperBound)) {
                hashKey.pushColumn(filter.lowerBound, keyColumn.getDataType());
                Collection<Long> result = prune(columnId + 1, hashKey, complex);
                hashKey.popColumn();
                return result;
            }
            // return all SubPartition
            return getAllTabletIds();
        }

        if (!(inPredicate.getChild(0) instanceof SlotRef)) {
            // return all SubPartition
            return getAllTabletIds();
        }
        Set<Long> resultSet = Sets.newHashSet();
        int inElementNum = inPredicate.getInElementNum();
        int newComplex = inElementNum * complex;
        int childrenNum = inPredicate.getChildren().size();
        for (int i = 1; i < childrenNum; ++i) {
            LiteralExpr expr = (LiteralExpr) inPredicate.getChild(i);
            hashKey.pushColumn(expr, keyColumn.getDataType());
            Collection<Long> subList = prune(columnId + 1, hashKey, newComplex);
            resultSet.addAll(subList);
            hashKey.popColumn();
            if (resultSet.size() >= bucketNum) {
                break;
            }
        }
        return resultSet;
    }

    private long getTabletId(int bucket) {
        return tablets.get(bucket).getId();
    }

    private List<Long> getAllTabletIds() {
        List<Long> tabletIds = new ArrayList<>(bucketNum);
        for (Tablet tablet : tablets) {
            tabletIds.add(tablet.getId());
        }
        return tabletIds;
    }

    public Collection<Long> prune() {
        PartitionKey hashKey = new PartitionKey();
        return prune(0, hashKey, 1);
    }
}
