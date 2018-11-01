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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashDistributionPruner implements DistributionPruner {
    private static final Logger LOG = LogManager.getLogger(HashDistributionPruner.class);

    // partition list, sort by the hash code
    private List<Long>                   partitionList;
    // partition columns
    private List<Column>                       partitionColumns;
    // partition column filters
    private Map<String, PartitionColumnFilter> partitionColumnFilters;
    private int                                hashMod;

    HashDistributionPruner(List<Long> partitions, List<Column> columns,
                           Map<String, PartitionColumnFilter> filters, int hashMod) {
        this.partitionList = partitions;
        this.partitionColumns = columns;
        this.partitionColumnFilters = filters;
        this.hashMod = hashMod;
    }

    // columnId: which column to compute
    // hashKey: the key which to compute hash value
    public Collection<Long> prune(int columnId, PartitionKey hashKey, int complex) {
        if (columnId == partitionColumns.size()) {
            // compute Hash Key
            long hashValue = hashKey.getHashValue();
            return Lists.newArrayList(
                    partitionList.get((int) ((hashValue & 0xffffffff) % hashMod)));
        }
        Column keyColumn = partitionColumns.get(columnId);
        PartitionColumnFilter filter = partitionColumnFilters.get(keyColumn.getName());
        if (null == filter) {
            // no filter in this column, no partition Key
            // return all subPartition
            return Lists.newArrayList(partitionList);
        }
        InPredicate inPredicate = filter.getInPredicate();
        if (null == inPredicate || inPredicate.getChildren().size() * complex > 100) {
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
            return Lists.newArrayList(partitionList);
        }
        
        if (null != inPredicate) {
            if (! (inPredicate.getChild(0) instanceof SlotRef)) {
                // return all SubPartition
                return Lists.newArrayList(partitionList);
            }
        }
        Set<Long> resultSet = Sets.newHashSet();
        int childrenNum = inPredicate.getChildren().size();
        int newComplex = inPredicate.getChildren().size() * complex;
        for (int i = 1; i < childrenNum; ++i) {
            LiteralExpr expr = (LiteralExpr) inPredicate.getChild(i);
            hashKey.pushColumn(expr, keyColumn.getDataType());
            Collection<Long> subList = prune(columnId + 1, hashKey, newComplex);
            for (Long subPartitionId : subList) {
                resultSet.add(subPartitionId);
            }
            hashKey.popColumn();
            if (resultSet.size() >= partitionList.size()) {
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
