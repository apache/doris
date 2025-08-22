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
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.ModifyColumnClause;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ModifyColumnOp
 */
public class ModifyColumnOp extends AlterTableOp {
    private ColumnDefinition columnDef;
    private ColumnPosition colPos;
    // which rollup is to be modify, if rollup is null, modify base table.
    private String rollupName;

    private Map<String, String> properties;

    // set in analyze
    private Column column;

    public ModifyColumnOp(ColumnDefinition columnDef, ColumnPosition colPos, String rollup,
            Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.columnDef = columnDef;
        this.colPos = colPos;
        this.rollupName = rollup;
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
        if (columnDef == null) {
            throw new AnalysisException("No column definition in add column clause.");
        }
        String colName = columnDef.getName();
        boolean isOlap = false;
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        boolean isEnableMergeOnWrite = false;
        KeysType keysType = KeysType.DUP_KEYS;
        Set<String> clusterKeySet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        Column originalColumn = null;
        TableIf table = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrDdlException(tableName.getCtl())
                .getDbOrDdlException(tableName.getDb())
                .getTableOrDdlException(tableName.getTbl());
        OlapTable olapTable = null;
        List<Column> schemaColumns = null;
        if (table instanceof OlapTable) {
            isOlap = true;
            olapTable = (OlapTable) table;
            keysType = olapTable.getKeysType();
            isEnableMergeOnWrite = olapTable.getEnableUniqueKeyMergeOnWrite();
            if (!Strings.isNullOrEmpty(rollupName)) {
                throw new AnalysisException("Cannot modify column in rollup " + rollupName);
            } else {
                originalColumn = olapTable.getColumn(colName);
                if (originalColumn != null) {
                    columnDef.setIsKey(originalColumn.isKey());
                }
                schemaColumns = olapTable.getFullSchema();
                if (olapTable.getPartitionColumnNames().contains(colName.toLowerCase())
                        || olapTable.getDistributionColumnNames().contains(colName.toLowerCase())) {
                    throw new AnalysisException("Can not modify partition or distribution column : " + colName);
                }
                long baseIndexId = olapTable.getBaseIndexId();
                for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getVisibleIndexIdToMeta().entrySet()) {
                    long indexId = entry.getKey();
                    if (indexId != baseIndexId) {
                        MaterializedIndexMeta meta = entry.getValue();
                        Set<String> mvUsedColumns = RelationUtil.getMvUsedColumnNames(meta);
                        if (mvUsedColumns.contains(colName)) {
                            throw new AnalysisException(
                                    "Can not modify column contained by mv, mv=" + olapTable.getIndexNameById(indexId));
                        }
                    }
                }
            }
        }
        columnDef.validate(isOlap, keysSet, clusterKeySet, isEnableMergeOnWrite, keysType);
        if (colPos != null) {
            colPos.analyze();
            if (olapTable != null) {
                if (colPos.isFirst()) {
                    if (!columnDef.isKey()) {
                        throw new AnalysisException(
                                String.format("Can't add value column %s as First column", columnDef.getName()));
                    }
                } else {
                    if (columnDef.getName().equalsIgnoreCase(colPos.getLastCol())) {
                        throw new AnalysisException("Can not modify column position after itself");
                    }
                    List<Column> reorderedColumns = new ArrayList<>(schemaColumns.size());
                    for (Column col : schemaColumns) {
                        if (!col.getName().equalsIgnoreCase(columnDef.getName())) {
                            reorderedColumns.add(col);
                            if (originalColumn != null && col.getName().equalsIgnoreCase(colPos.getLastCol())) {
                                reorderedColumns.add(originalColumn);
                            }
                        }
                    }
                    if (originalColumn != null && reorderedColumns.size() != olapTable.getFullSchema().size()) {
                        throw new AnalysisException(String.format("Column[%s] does not exist", colPos.getLastCol()));
                    }

                    boolean seeValueColumn = false;
                    for (Column col : reorderedColumns) {
                        if (seeValueColumn && col.isKey()) {
                            throw new AnalysisException(
                                    String.format("Can not modify key column %s after value column", col.getName()));
                        }
                        seeValueColumn = seeValueColumn || !col.isKey();
                    }
                }
            }
        }
        column = columnDef.translateToCatalogStyleForSchemaChange();
        if (originalColumn != null) {
            originalColumn.checkSchemaChangeAllowed(column);
        }
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new ModifyColumnClause(toSql(), column, colPos, rollupName, properties);
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
        return true;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY COLUMN ").append(columnDef.toSql());
        if (colPos != null) {
            sb.append(" ").append(colPos);
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
}
