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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


public final class RollupSelector {
    private static final Logger LOG = LogManager.getLogger(RollupSelector.class);

    // Rollup's table info.
    private final TupleDescriptor tupleDesc;
    private final OlapTable table;
    private final Analyzer analyzer;

    public RollupSelector(Analyzer analyzer, TupleDescriptor tupleDesc, OlapTable table) {
        this.analyzer = analyzer;
        this.tupleDesc = tupleDesc;
        this.table = table;
    }

    public SelectedRolupInfo selectBestRollup(
            Collection<Long> partitionIds, List<Expr> conjuncts, boolean isPreAggregation)
            throws UserException {
        Preconditions.checkArgument(partitionIds != null && !partitionIds.isEmpty(),
                "Paritition can't be null or empty.");
        final SelectedRolupInfo selectedRolupInfo =
                selectBestPrefixIndexRollup(
                        table.getPartition(partitionIds.iterator().next()),
                        conjuncts,
                        isPreAggregation);
        selectBestRowCountRollup(selectedRolupInfo, partitionIds);
        selectedRolupInfo.caculateBestRollup();
        return selectedRolupInfo;
    }

    private void selectBestRowCountRollup(SelectedRolupInfo selectedBestPrefixRollup, Collection<Long> partitionIds) {
        for (Long indexId : selectedBestPrefixRollup.getRollupMatchedPrefixIds()) {
            long rowCount = 0;
            for (Long partitionId : partitionIds) {
                rowCount += table.getPartition(partitionId).getIndex(indexId).getRowCount();
            }
            LOG.debug("rowCount={} for table={}", rowCount, indexId);
            selectedBestPrefixRollup.addRollupRowCount(indexId, rowCount);
        }
    }

    private SelectedRolupInfo selectBestPrefixIndexRollup(
            Partition partition, List<Expr> conjuncts, boolean isPreAggregation) throws UserException {
        final SelectedRolupInfo rollupWithBestPrefix = new SelectedRolupInfo();
        final List<String> outputColumns = Lists.newArrayList();
        for (SlotDescriptor slot : tupleDesc.getMaterializedSlots()) {
            Column col = slot.getColumn();
            outputColumns.add(col.getName());
        }

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
        final Set<String> equivalenceColumns = Sets.newHashSet();
        final Set<String> unequivalenceColumns = Sets.newHashSet();
        collectColumns(conjuncts, equivalenceColumns, unequivalenceColumns);
        matchPrefixIndex(rollupsContainsOutput, equivalenceColumns,
                unequivalenceColumns, rollupWithBestPrefix);

        return rollupWithBestPrefix;
    }

    private void matchPrefixIndex(List<MaterializedIndex> candidateRollups,
                                  Set<String> equivalenceColumns,
                                  Set<String> unequivalenceColumns,
                                  SelectedRolupInfo rollupWithBestPrefix) {
        if (equivalenceColumns.size() == 0 && unequivalenceColumns.size() == 0) {
            final List<Column> EMPTY = Lists.newArrayList();
            candidateRollups.stream().forEach(index -> rollupWithBestPrefix.addRollupJoinColumns(index.getId(), EMPTY));
            return;
        }

        for (MaterializedIndex index : candidateRollups) {
            final List<Column> joinColumns = Lists.newArrayList();
            for (Column col : table.getSchemaByIndexId(index.getId())) {
                if (equivalenceColumns.contains(col.getName())) {
                    joinColumns.add(col);
                } else if (unequivalenceColumns.contains(col.getName())) {
                    // Unequivalence predicate's columns can match only first column in rollup.
                    joinColumns.add(col);
                    break;
                } else {
                    break;
                }
            }

            rollupWithBestPrefix.addRollupJoinColumns(index.getId(), joinColumns);
        }
    }

    private void collectColumns(
            List<Expr> conjuncts, Set<String> equivalenceColumns, Set<String> unequivalenceColumns) {

        // 1. Get columns which has predicate on it.
        for (Expr expr : conjuncts) {
            if (!isPredicateUsedForPrefixIndex(expr, false)) {
                continue;
            }
            for (SlotDescriptor slot : tupleDesc.getMaterializedSlots()) {
                if (expr.isBound(slot.getId())) {
                    if (!isEquivalenceExpr(expr)) {
                        unequivalenceColumns.add(slot.getColumn().getName());
                    } else {
                        equivalenceColumns.add(slot.getColumn().getName());
                    }
                    break;
                }
            }
        }

        // 2. Equal join predicates when pushing inner child.
        List<Expr> eqJoinPredicate = analyzer.getEqJoinConjuncts(tupleDesc.getId());
        for (Expr expr : eqJoinPredicate) {
            if (!isPredicateUsedForPrefixIndex(expr, true)) {
                continue;
            }
            for (SlotDescriptor slot : tupleDesc.getMaterializedSlots()) {
                for (int i = 0; i < 2; i++) {
                    if (expr.getChild(i).isBound(slot.getId())) {
                        equivalenceColumns.add(slot.getColumn().getName());
                        break;
                    }
                }
            }
        }
    }

    private boolean isEquivalenceExpr(Expr expr) {
        if (expr instanceof InPredicate) {
            return true;
        }
        if (expr instanceof BinaryPredicate) {
            final BinaryPredicate predicate = (BinaryPredicate) expr;
            if (predicate.getOp().isEquivalence()) {
                return true;
            }
        }
        return false;
    }

    private boolean isPredicateUsedForPrefixIndex(Expr expr, boolean isJoinConjunct) {
        if (!(expr instanceof InPredicate)
                && !(expr instanceof BinaryPredicate)) {
            return false;
        }
        if (expr instanceof InPredicate) {
            return isInPredicateUsedForPrefixIndex((InPredicate) expr);
        } else if (expr instanceof BinaryPredicate) {
            if (isJoinConjunct) {
                return isEqualJoinConjunctUsedForPrefixIndex((BinaryPredicate) expr);
            } else {
                return isBinaryPredicateUsedForPrefixIndex((BinaryPredicate) expr);
            }
        }
        return true;
    }

    private boolean isEqualJoinConjunctUsedForPrefixIndex(BinaryPredicate expr) {
        Preconditions.checkArgument(expr.getOp().isEquivalence());
        if (expr.isAuxExpr()) {
            return false;
        }
        for (Expr child : expr.getChildren()) {
            for (SlotDescriptor slot : tupleDesc.getMaterializedSlots()) {
                if (child.isBound(slot.getId()) && isSlotRefNested(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isBinaryPredicateUsedForPrefixIndex(BinaryPredicate expr) {
        if (expr.isAuxExpr() || expr.getOp().isUnequivalence()) {
            return false;
        }
        return (isSlotRefNested(expr.getChild(0)) && expr.getChild(1).isConstant())
                || (isSlotRefNested(expr.getChild(1)) && expr.getChild(0).isConstant());
    }

    private boolean isInPredicateUsedForPrefixIndex(InPredicate expr) {
        if (expr.isNotIn()) {
            return false;
        }
        return isSlotRefNested(expr.getChild(0)) && expr.isLiteralChildren();
    }

    private boolean isSlotRefNested(Expr expr) {
        while (expr instanceof CastExpr) {
            expr = expr.getChild(0);
        }
        return expr instanceof SlotRef;
    }

    public static class SelectedRolupInfo {
        private Map<Long, List<Column>> idToJoinColumns;
        private Map<Long, Long> idToRowCount;
        private long selectedRollupId;
        private List<Column> matchedPrefixIndex;

        private SelectedRolupInfo() {
            this.idToJoinColumns = Maps.newHashMap();
            this.idToRowCount = Maps.newHashMap();
            this.selectedRollupId = -1;
            this.matchedPrefixIndex = Lists.newArrayList();
        }

        public long getSelectedRollupId() {
            return this.selectedRollupId;
        }

        public void setSelectedRollupId(long id) {
            this.selectedRollupId = id;
        }

        public List<Column> getMatchedPrefixIndex() {
            return matchedPrefixIndex;
        }

        private void addRollupJoinColumns(long id, List<Column> hitPrefixColumns) {
            this.idToJoinColumns.put(id, hitPrefixColumns);
        }

        private Collection<Long> getRollupMatchedPrefixIds() {
            return idToJoinColumns.keySet();
        }

        private void addRollupRowCount(long id, long rows) {
            this.idToRowCount.put(id, rows);
        }

        /**
         * Actually it's not a good way to select best rollup by this, because
         * the rollup which has smaller data may has lowerer cost than these
         * which have longer matched prefix-index.
         */
        private void caculateBestRollup() {
            // 1. Get rollups matching best prefix index.
            final Map<Long, Integer> rollupScores = Maps.newHashMap();
            for (long id : idToJoinColumns.keySet()) {
                List<Column> columns = idToJoinColumns.get(id);
                int prefixIndexSize = 0;
                for (Column column : columns) {
                    if (!column.getDataType().isStringType()) {
                        prefixIndexSize += column.getDataType().getSlotSize();
                    } else if (column.getDataType().isChar()) {
                        prefixIndexSize += column.getStrLen();
                    } else if (column.getDataType().isVarchar()) {
                        prefixIndexSize += 20;
                        if (prefixIndexSize > 32) {
                            prefixIndexSize = 32;
                        }
                        break;
                    }

                    if (prefixIndexSize >= 32) {
                        prefixIndexSize = 32;
                        break;
                    }
                }
                rollupScores.put(id, prefixIndexSize);
            }

            final List<Long> bestRollupIds = Lists.newArrayList();
            int matchBestColumnsSize = -1;
            for (long id : rollupScores.keySet()) {
                int matchedIndexSize = rollupScores.get(id);
                if (matchBestColumnsSize == -1 || matchBestColumnsSize == matchedIndexSize) {
                    matchBestColumnsSize = matchedIndexSize;
                    bestRollupIds.add(id);
                } else if (matchBestColumnsSize < matchedIndexSize) {
                    bestRollupIds.clear();
                    bestRollupIds.add(id);
                }
            }

            // 2. Get rollup which has smallest data.
            long matchBestRowCount = -1;
            for (Long id : bestRollupIds) {
                long rowCount = idToRowCount.get(id);
                if (matchBestRowCount == -1 || matchBestRowCount > rowCount) {
                    matchBestRowCount = rowCount;
                    selectedRollupId = id;
                    matchedPrefixIndex = idToJoinColumns.get(id);
                }
            }
        }
    }
}
