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
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.Config;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
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

    // partition list, sort by the hash code
    private List<Long> bucketsList;
    // partition columns
    private List<Column>                       distributionColumns;
    // partition column filters
    private Map<String, PartitionColumnFilter> distributionColumnFilters;
    private int                                hashMod;

    HashDistributionPruner(List<Long> bucketsList, List<Column> columns,
                           Map<String, PartitionColumnFilter> filters, int hashMod) {
        this.bucketsList = bucketsList;
        this.distributionColumns = columns;
        this.distributionColumnFilters = filters;
        this.hashMod = hashMod;
    }

    // columnId: which column to compute
    // hashKey: the key which to compute hash value
    public Collection<Long> prune(int columnId, PartitionKey hashKey, int complex) {
        if (columnId == distributionColumns.size()) {
            // compute Hash Key
            long hashValue = hashKey.getHashValue();
            return Lists.newArrayList(bucketsList.get((int) ((hashValue & 0xffffffff) % hashMod)));
        }
        Column keyColumn = distributionColumns.get(columnId);
        PartitionColumnFilter filter = distributionColumnFilters.get(keyColumn.getName());
        if (null == filter) {
            // no filter in this column, no partition Key
            // return all subPartition
            return Lists.newArrayList(bucketsList);
        }
        InPredicate inPredicate = filter.getInPredicate();
        if (null == inPredicate || inPredicate.getInElementNum() * complex > Config.max_distribution_pruner_recursion_depth) {
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
            return Lists.newArrayList(bucketsList);
        }

        if (!(inPredicate.getChild(0) instanceof SlotRef)) {
            // return all SubPartition
            return Lists.newArrayList(bucketsList);
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
            if (resultSet.size() >= bucketsList.size()) {
                break;
            }
        }
        return resultSet;
    }

    public Collection<Long> prune() {
        PartitionKey hashKey = new PartitionKey();
        return prune(0, hashKey, 1);
    }
}
