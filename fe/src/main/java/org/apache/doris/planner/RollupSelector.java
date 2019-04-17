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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public final class RollupSelector {
    private static final Logger LOG = LogManager.getLogger(RollupSelector.class);

    // Rollup's table info.
    private final TupleDescriptor tupleDesc;
    private final OlapTable table;

    public RollupSelector(TupleDescriptor tupleDesc, OlapTable table) {
        this.tupleDesc = tupleDesc;
        this.table = table;
    }

    public long selectBestRollup(
            Collection<Long> partitionIds, List<Expr> conjuncts, boolean isPreAggregation)
            throws UserException {
        Preconditions.checkArgument(partitionIds != null && !partitionIds.isEmpty(),
                "Paritition can't be null or empty.");
        // Get first partition to select best prefix index rollups, because MaterializedIndex ids in one rollup's partitions are all same.
        final List<Long> bestPrefixIndexRollups =
                selectBestPrefixIndexRollup(
                        table.getPartition(partitionIds.iterator().next()),
                        conjuncts,
                        isPreAggregation);
        return selectBestRowCountRollup(bestPrefixIndexRollups, partitionIds);
    }

    private long selectBestRowCountRollup(List<Long> bestPrefixIndexRollups, Collection<Long> partitionIds) {
        long minRowCount = Long.MAX_VALUE;
        long selectedIndexId = 0;
        for (Long indexId : bestPrefixIndexRollups) {
            long rowCount = 0;
            for (Long partitionId : partitionIds) {
                rowCount += table.getPartition(partitionId).getIndex(indexId).getRowCount();
            }
            LOG.debug("rowCount={} for table={}", rowCount, indexId);
            if (rowCount < minRowCount) {
                minRowCount = rowCount;
                selectedIndexId = indexId;
            } else if (rowCount == minRowCount) {
                // check column number, select one minimum column number
                int selectedColumnSize = table.getIndexIdToSchema().get(selectedIndexId).size();
                int currColumnSize = table.getIndexIdToSchema().get(indexId).size();
                if (currColumnSize < selectedColumnSize) {
                    selectedIndexId = indexId;
                }
            }
        }
        return selectedIndexId;
    }

    private List<Long> selectBestPrefixIndexRollup(
            Partition partition, List<Expr> conjuncts, boolean isPreAggregation) throws UserException {

        final Set<String> predicateColumns = Sets.newHashSet();
        final List<String> outputColumns = Lists.newArrayList();
        collectColumns(conjuncts, outputColumns, predicateColumns);

        final List<MaterializedIndex> rollups = Lists.newArrayList();
        rollups.add(partition.getBaseIndex());
        rollups.addAll(partition.getRollupIndices());
        LOG.debug("num of rollup(base included): {}, pre aggr: {}", rollups.size(), isPreAggregation);

        // 1. find all rollup indexes which contains all tuple columns
        final List<MaterializedIndex> rollupsContainsOutput = Lists.newArrayList();
        final List<Column> baseTableColumns = table.getKeyColumnsByIndexId(partition.getBaseIndex().getId());
        for (MaterializedIndex rollup : rollups) {
            final Set<String> rollupColumns = Sets.newHashSet();
            table.getSchemaByIndexId(rollup.getId())
                    .stream().forEach(column -> rollupColumns.add(column.getName()));

            if (rollupColumns.containsAll(outputColumns)) {
                // If preAggregation is off, so that we only can use base table
                // or those rollup tables which key columns is the same with base table
                // (often in different order)
                if (isPreAggregation) {
                    LOG.debug("preAggregation is on. add index {} which contains all output columns",
                            rollup.getId());
                    rollupsContainsOutput.add(rollup);
                } else if (table.getKeyColumnsByIndexId(rollup.getId()).size() == baseTableColumns.size()) {
                    LOG.debug("preAggregation is off, but index {} have same key columns with base index.",
                            rollup.getId());
                    rollupsContainsOutput.add(rollup);
                }
            } else {
                LOG.debug("exclude index {} because it does not contain all output columns", rollup.getId());
            }
        }

        Preconditions.checkArgument(rollupsContainsOutput.size() > 0,
                "Can't find candicate rollup contains all output columns.");

        // 2. find all rollups which match the prefix most based on predicates column from containTupleIndices.
        int maxPrefixMatchCount = 0;
        int prefixMatchCount = 0;
        List<Long> rollupsMatchingBestPrefixIndex = Lists.newArrayList();
        for (MaterializedIndex index : rollupsContainsOutput) {
            prefixMatchCount = 0;
            for (Column col : table.getSchemaByIndexId(index.getId())) {
                if (predicateColumns.contains(col.getName())) {
                    prefixMatchCount++;
                } else {
                    break;
                }
            }

            if (prefixMatchCount == maxPrefixMatchCount) {
                LOG.debug("s3: find a equal prefix match index {}. match count: {}", index.getId(), prefixMatchCount);
                rollupsMatchingBestPrefixIndex.add(index.getId());
            } else if (prefixMatchCount > maxPrefixMatchCount) {
                LOG.debug("s3: find a better prefix match index {}. match count: {}", index.getId(), prefixMatchCount);
                maxPrefixMatchCount = prefixMatchCount;
                rollupsMatchingBestPrefixIndex.clear();
                rollupsMatchingBestPrefixIndex.add(index.getId());
            }
        }

        if (rollupsMatchingBestPrefixIndex.isEmpty()) {
            rollupsMatchingBestPrefixIndex =
                    rollupsContainsOutput.stream().map(n -> n.getId()).collect(Collectors.toList());
        }

        // 3. sorted the final candidate indexes by index id
        // this is to make sure that candidate indexes find in all partitions will be returned in same order
        Collections.sort(rollupsMatchingBestPrefixIndex, new Comparator<Long>() {
            @Override
            public int compare(Long id1, Long id2) {
                return (int) (id1 - id2);
            }
        });
        return rollupsMatchingBestPrefixIndex;
    }

    private void collectColumns(List<Expr> conjuncts, List<String> outputColumns, Set<String> predicateColumns) throws UserException {
        // 1. Get columns which has predicate on it.
        for (SlotDescriptor slot : tupleDesc.getSlots()) {
            for (Expr expr : conjuncts) {
                if (!isUsedForPrefixIndex(expr)) {
                    continue;
                }
                if (expr.isBound(slot.getId())) {
                    predicateColumns.add(slot.getColumn().getName());
                }
            }
        }

        // 2. Get materialized slot's columns by parent.
        for (SlotDescriptor slot : tupleDesc.getMaterializedSlots()) {
            Column col = slot.getColumn();
            outputColumns.add(col.getName());
        }
    }

    private boolean isUsedForPrefixIndex(Expr expr) {
        if (expr.isConstant()) {
            return false;
        } else if (expr instanceof InPredicate) {
            final InPredicate predicate = (InPredicate)expr;
            if (predicate.isNotIn()) {
                return false;
            }
        } else if (expr instanceof BinaryPredicate) {
            final BinaryPredicate predicate = (BinaryPredicate)expr;
            if (predicate.getOp().isUnequivalence()) {
                return false;
            }
        }
        return true;
    }
}
