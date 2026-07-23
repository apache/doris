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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;

import java.util.List;

/**
 * Resolves a selected-index column to its base-table partition column.
 */
public final class RuntimeFilterPartitionColumnResolver {
    private RuntimeFilterPartitionColumnResolver() {
    }

    /**
     * Returns the matching partition-column ordinal, or -1 when the target is
     * not a value-preserving representation of a base partition column.
     */
    public static int findPartitionColumnIndex(OlapScanNode scanNode, Column targetColumn) {
        if (targetColumn == null) {
            return -1;
        }
        OlapTable table = scanNode.getOlapTable();
        List<Column> partitionColumns = table.getPartitionInfo().getPartitionColumns();
        if (targetColumn.isMaterializedViewColumn()) {
            if (targetColumn.isAggregated()) {
                return -1;
            }
            Expr defineExpr = targetColumn.getDefineExpr();
            if (!(defineExpr instanceof SlotRef)) {
                return -1;
            }
            Column baseColumn = ((SlotRef) defineExpr).getColumn();
            return baseColumn == null ? -1 : findSameIndexColumn(baseColumn, partitionColumns);
        }
        if (scanNode.getSelectedIndexId() == table.getBaseIndexId()) {
            return findSameIndexColumn(targetColumn, partitionColumns);
        }
        return findColumnByName(targetColumn.getName(), partitionColumns);
    }

    private static int findSameIndexColumn(Column targetColumn, List<Column> columns) {
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (targetColumn == column) {
                return i;
            }
            int targetUniqueId = targetColumn.getUniqueId();
            int columnUniqueId = column.getUniqueId();
            if (targetUniqueId != Column.COLUMN_UNIQUE_ID_INIT_VALUE
                    && columnUniqueId != Column.COLUMN_UNIQUE_ID_INIT_VALUE) {
                if (targetUniqueId == columnUniqueId) {
                    return i;
                }
            } else if (targetColumn.equals(column)) {
                return i;
            }
        }
        return -1;
    }

    private static int findColumnByName(String targetColumnName, List<Column> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (targetColumnName.equalsIgnoreCase(columns.get(i).getName())) {
                return i;
            }
        }
        return -1;
    }
}
