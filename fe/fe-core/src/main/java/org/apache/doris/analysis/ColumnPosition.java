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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;

import java.util.List;

// Column position used when add column
public class ColumnPosition {
    public static final ColumnPosition FIRST = new ColumnPosition();
    public static final ColumnPosition ROW_BINLOG_START = new ColumnPosition(Column.BINLOG_TIMESTAMP_COL);

    private String lastCol;

    public String getLastCol() {
        return lastCol;
    }

    // used to create FIRST position.
    private ColumnPosition() {
    }

    public ColumnPosition(String col) {
        this.lastCol = col;
    }


    public static ColumnPosition convertToRowBinlog(List<Column> rowBinlogSchema, ColumnPosition columnPosition,
                                                    boolean isKey, boolean before) {
        String lastKeyCol = "";
        String lastValueCol = "";
        String lastBeforeValueCol = "";
        for (Column column : rowBinlogSchema) {
            String columnName = column.getName();
            if (column.isKey()) {
                lastKeyCol = columnName;
            } else {
                if (columnName.contains(Column.BINLOG_BEFORE_PREFIX)) {
                    lastBeforeValueCol = columnName;
                } else if (columnName.equals(Column.BINLOG_LSN_COL)
                        || columnName.equals(Column.BINLOG_OPERATION_COL)
                        || columnName.equals(Column.BINLOG_TIMESTAMP_COL)) {
                    continue;
                } else {
                    lastValueCol = columnName;
                }
            }
        }
        if (Strings.isNullOrEmpty(lastValueCol)) {
            lastValueCol = lastKeyCol;
        }
        if (Strings.isNullOrEmpty(lastBeforeValueCol)) {
            lastBeforeValueCol = lastValueCol;
        }
        if (columnPosition == null) {
            // add to last
            if (isKey) {
                return new ColumnPosition(lastKeyCol);
            } else if (!before) {
                return new ColumnPosition(lastValueCol);
            } else {
                return new ColumnPosition(lastBeforeValueCol);
            }
        }
        if (columnPosition == FIRST) {
return FIRST;
        } else {
            String lastCol = columnPosition.getLastCol();
            if (lastCol.equals(lastKeyCol)) {
                return new ColumnPosition(before ? lastValueCol : lastKeyCol);
            }
            return new ColumnPosition(before ? Column.generateBeforeColName(lastCol) : lastCol);
        }
    }

    public void analyze() throws AnalysisException {
        if (this == FIRST) {
            return;
        }
        if (Strings.isNullOrEmpty(lastCol)) {
            throw new AnalysisException("Column is empty.");
        }
    }

    public boolean isFirst() {
        return this == FIRST;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (this == FIRST) {
            sb.append("FIRST");
        } else {
            sb.append("AFTER `").append(lastCol).append("`");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
