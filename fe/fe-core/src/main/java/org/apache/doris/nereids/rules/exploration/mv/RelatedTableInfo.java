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

import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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
    private final List<TableColumnInfo> tableColumnInfos;

    public RelatedTableInfo(boolean pctPossible, List<TableColumnInfo> tableColumnInfos,
            Set<String> failReasons) {
        this.pctPossible = pctPossible;
        this.failReasons.addAll(failReasons);
        this.tableColumnInfos = tableColumnInfos;
    }

    public static RelatedTableInfo successWith(List<TableColumnInfo> tableColumnInfos) {
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

    public List<TableColumnInfo> getTableColumnInfos() {
        return tableColumnInfos;
    }

    /**
     * The related table and partition column info which mv relates
     */
    public static final class TableColumnInfo {
        // The partition table which mv relates
        private final BaseTableInfo tableInfo;
        // The column which mv relates
        private final String column;
        // This records the partition rollup expression if exist
        private Optional<Expression> partitionExpression;
        // This record the partition column is original or derived from equal set
        private final boolean isOriginalPartition;

        public TableColumnInfo(BaseTableInfo tableInfo, String column,
                Expression partitionExpression, boolean isOriginalPartition) {
            this.tableInfo = tableInfo;
            this.column = column;
            this.partitionExpression = Optional.ofNullable(partitionExpression);
            this.isOriginalPartition = isOriginalPartition;
        }

        public BaseTableInfo getTableInfo() {
            return tableInfo;
        }

        public String getColumn() {
            return column;
        }

        public Optional<Expression> getPartitionExpression() {
            return partitionExpression;
        }

        public boolean isOriginalPartition() {
            return isOriginalPartition;
        }

        @Override
        public String toString() {
            return "TableColumnInfo{" + "tableInfo=" + tableInfo + ", column='" + column + '\''
                    + ", partitionExpression=" + partitionExpression + ", isOriginalPartition=" + isOriginalPartition
                    + '}';
        }
    }
}
