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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.AddColumnClause;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AddColumnOp
 */
public class AddColumnOp extends AlterTableOp {
    private ColumnDefinition columnDef;
    // Column position
    private ColumnPosition colPos;
    // if rollupName is null, add to column to base index.
    private String rollupName;

    private Map<String, String> properties;
    // set in analyze
    private Column column;

    public AddColumnOp(ColumnDefinition columnDef, ColumnPosition colPos, String rollupName,
            Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.columnDef = columnDef;
        this.colPos = colPos;
        this.rollupName = rollupName;
        this.properties = properties;
    }

    public Column getColumn() {
        return column;
    }

    public ColumnPosition getColPos() {
        return colPos;
    }

    public String getRollupName() {
        return rollupName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (colPos != null) {
            colPos.analyze();
        }
        validateColumnDef(tableName, columnDef, colPos, rollupName);
        column = columnDef.translateToCatalogStyleForSchemaChange();
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new AddColumnClause(toSql(), column, colPos, rollupName, properties);
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD COLUMN ").append(columnDef.toSql());
        if (colPos != null) {
            sb.append(" ").append(colPos.toSql());
        }
        if (rollupName != null) {
            sb.append(" IN `").append(rollupName).append("`");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    /**
     * validateColumnDef
     */
    public static void validateColumnDef(TableNameInfo tableName, ColumnDefinition columnDef, ColumnPosition colPos,
            String rollupName)
            throws UserException {
        if (columnDef == null) {
            throw new AnalysisException("No column definition in add column clause.");
        }
        boolean isOlap = false;
        OlapTable olapTable = null;
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        boolean isEnableMergeOnWrite = false;
        KeysType keysType = KeysType.DUP_KEYS;
        Set<String> clusterKeySet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        TableIf table = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrDdlException(tableName.getCtl())
                .getDbOrDdlException(tableName.getDb())
                .getTableOrDdlException(tableName.getTbl());
        if (table instanceof OlapTable) {
            isOlap = true;
            olapTable = (OlapTable) table;
            keysType = olapTable.getKeysType();
            AggregateType aggregateType = columnDef.getAggType();
            Long indexId = olapTable.getIndexIdByName(rollupName);
            if (indexId != null) {
                MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
                if (indexMeta.getDefineStmt() != null) {
                    throw new AnalysisException("Cannot add column in rollup " + rollupName);
                }
            }
            if (keysType == KeysType.AGG_KEYS) {
                if (aggregateType == null) {
                    columnDef.setIsKey(true);
                } else {
                    if (aggregateType == AggregateType.NONE) {
                        throw new AnalysisException(
                                String.format("can't set NONE as aggregation type on column %s",
                                        columnDef.getName()));
                    }
                }
            } else if (keysType == KeysType.UNIQUE_KEYS) {
                if (aggregateType != null && !aggregateType.isReplaceFamily() && columnDef.isVisible()) {
                    throw new AnalysisException(
                            String.format("Can not assign aggregation method on column in Unique data model table: %s",
                                    columnDef.getName()));
                }
            }
            isEnableMergeOnWrite = olapTable.getEnableUniqueKeyMergeOnWrite();
            clusterKeySet
                    .addAll(olapTable.getBaseSchema().stream().filter(Column::isClusterKey).map(Column::getName)
                            .collect(Collectors.toList()));
        }
        columnDef.validate(isOlap, keysSet, clusterKeySet, isEnableMergeOnWrite, keysType);
        if (!columnDef.isNullable() && !columnDef.hasDefaultValue()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DEFAULT_FOR_FIELD, columnDef.getName());
        }
        if (olapTable != null && colPos != null) {
            if (colPos.isFirst()) {
                if (!columnDef.isKey()) {
                    throw new AnalysisException(
                            String.format("Can't add value column %s as First column", columnDef.getName()));
                }
            } else {
                Column afterColumn = null;
                Column beforeColumn = null;
                for (Column col : olapTable.getFullSchema()) {
                    if (beforeColumn == null && afterColumn != null) {
                        beforeColumn = col;
                    }
                    if (col.getName().equalsIgnoreCase(colPos.getLastCol())) {
                        afterColumn = col;
                    }
                    if (col.getName().equalsIgnoreCase(columnDef.getName())) {
                        throw new AnalysisException(String.format("column %s already exists in table %s",
                                columnDef.getName(), tableName.getTbl()));
                    }
                }
                if (afterColumn != null) {
                    if (afterColumn.isKey()) {
                        if (!columnDef.isKey() && beforeColumn != null && beforeColumn.isKey()) {
                            throw new AnalysisException(String.format("can't add value column %s before key column %s",
                                    columnDef.getName(), beforeColumn.getName()));
                        }
                    } else {
                        if (columnDef.isKey()) {
                            throw new AnalysisException(String.format("can't add key column %s after value column %s",
                                    columnDef.getName(), afterColumn.getName()));
                        }
                    }
                } else {
                    // do nothing for now, because previous command may add a new column, but it can only be seen
                    // after previous command finished, we should not report error only by check the currect schema
                }
            }
        }
    }
}
