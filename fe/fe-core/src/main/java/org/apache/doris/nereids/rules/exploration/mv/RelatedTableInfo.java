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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The related partition table info which mv relate
 */
public class RelatedTableInfo {
    private final boolean pctPossible;
    private final Set<String> failReasons = new HashSet<>();
    private final List<RelatedTableColumnInfo> tableColumnInfos;

    public RelatedTableInfo(boolean pctPossible, List<RelatedTableColumnInfo> tableColumnInfos,
            Set<String> failReasons) {
        this.pctPossible = pctPossible;
        this.failReasons.addAll(failReasons);
        this.tableColumnInfos = tableColumnInfos;
    }

    public static RelatedTableInfo successWith(List<RelatedTableColumnInfo> tableColumnInfos) {
        return new RelatedTableInfo(true, tableColumnInfos, ImmutableSet.of());
    }

    public static RelatedTableInfo failWith(String failReason) {
        return new RelatedTableInfo(false, ImmutableList.of(), ImmutableSet.of(failReason));
    }

    public static RelatedTableInfo failWith(Set<String> failReasons) {
        return new RelatedTableInfo(false, ImmutableList.of(), failReasons);
    }

    public boolean isPctPossible() {
        return pctPossible;
    }

    public String getFailReason() {
        return String.join(",", failReasons);
    }

    public Set<String> getFailReasons() {
        return failReasons;
    }

    /**
     * Get valid table column infos
     */
    public List<RelatedTableColumnInfo> getTableColumnInfos() {
        List<RelatedTableColumnInfo> validTableColumnInfos = new ArrayList<>();
        for (RelatedTableColumnInfo relatedTableColumnInfo : tableColumnInfos) {
            if (relatedTableColumnInfo.getColumn() != null) {
                validTableColumnInfos.add(relatedTableColumnInfo);
            }
        }
        return validTableColumnInfos;
    }

    /**
     * The related table and partition column info which mv relates
     */
    public static final class RelatedTableColumnInfo {
        // This records the partition named expression
        private final NamedExpression partitionNamedExpression;
        // This records the partition rollup expression if exist
        private final Optional<Expression> partitionExpression;
        // This record the partition column is original or derived from equal set
        private final boolean isOriginalPartition;
        // This partition column to check is in the table's partition columns, if not this is false
        // if in the table's partition columns, this is true, this is used to derive union all partition column
        private boolean isFromTablePartitionColumn;
        private boolean isReachRelationCheck;

        private RelatedTableColumnInfo(NamedExpression partitionNamedExpression,
                Expression partitionExpression,
                boolean isOriginalPartition,
                boolean isFromTablePartitionColumn) {
            this.partitionNamedExpression = partitionNamedExpression;
            this.partitionExpression = Optional.ofNullable(partitionExpression);
            this.isOriginalPartition = isOriginalPartition;
            this.isFromTablePartitionColumn = isFromTablePartitionColumn;
        }

        /**
         * get partition table info
         */
        public BaseTableInfo getTableInfo() {
            if (!(partitionNamedExpression instanceof SlotReference)
                    || !partitionNamedExpression.isColumnFromTable()
                    || !((SlotReference) partitionNamedExpression).getOriginalTable().isPresent()) {
                return null;
            }
            return new BaseTableInfo(((SlotReference) partitionNamedExpression).getOriginalTable().get());
        }

        /**
         * get column str
         */
        public String getColumnStr() {
            Column column = getColumn();
            return column == null ? null : column.getName();
        }

        /**
         * get column
         */
        public Column getColumn() {
            if (!(partitionNamedExpression instanceof SlotReference)
                    || !partitionNamedExpression.isColumnFromTable()
                    || !((SlotReference) partitionNamedExpression).getOriginalTable().isPresent()) {
                return null;
            }
            return extractColumn(this.partitionNamedExpression);
        }

        public NamedExpression getPartitionNamedExpression() {
            return partitionNamedExpression;
        }

        public Optional<Expression> getPartitionExpression() {
            return partitionExpression;
        }

        public boolean isOriginalPartition() {
            return isOriginalPartition;
        }

        public boolean isFromTablePartitionColumn() {
            return isFromTablePartitionColumn;
        }

        public void setFromTablePartitionColumn(boolean fromTablePartitionColumn) {
            isFromTablePartitionColumn = fromTablePartitionColumn;
        }

        public boolean isReachRelationCheck() {
            return isReachRelationCheck;
        }

        public void setReachRelationCheck(boolean reachRelationCheck) {
            isReachRelationCheck = reachRelationCheck;
        }

        public static RelatedTableColumnInfo of(NamedExpression partitionNamedExpression,
                Expression partitionExpression, boolean isOriginalPartition, boolean isFromTablePartitionColumn) {
            return new RelatedTableColumnInfo(partitionNamedExpression, partitionExpression,
                    isOriginalPartition, isFromTablePartitionColumn);
        }

        /**
         * Extract column from slot reference
         */
        private static Column extractColumn(NamedExpression slotReference) {
            if (!(slotReference instanceof SlotReference)) {
                return null;
            }
            Optional<Column> slotReferenceColumn = ((SlotReference) slotReference).getOriginalColumn();
            if (!slotReferenceColumn.isPresent()) {
                return null;
            }
            if (!slotReference.isColumnFromTable()) {
                // Column is not from table
                return null;
            }
            Expr definExpr = slotReferenceColumn.get().getDefineExpr();
            if (definExpr instanceof SlotRef) {
                // If slotReference is from sync mv when rbo, should get actual column
                Column referenceRollupColumn = ((SlotRef) definExpr).getColumn();
                if (referenceRollupColumn != null) {
                    return referenceRollupColumn;
                }
            }
            return slotReferenceColumn.get();
        }

        @Override
        public String toString() {
            return "RelatedTableColumnInfo{"
                    + "partitionNamedExpression=" + partitionNamedExpression
                    + ", partitionExpression=" + partitionExpression
                    + ", isOriginalPartition=" + isOriginalPartition
                    + ", isFromTablePartitionColumn=" + isFromTablePartitionColumn
                    + ", isReachRelationCheck=" + isReachRelationCheck + '}';
        }
    }
}
