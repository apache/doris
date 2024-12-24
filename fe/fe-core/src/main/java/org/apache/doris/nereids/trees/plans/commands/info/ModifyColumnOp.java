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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        boolean isOlap = false;
        Table table = null;
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (tableName != null) {
            table = Env.getCurrentInternalCatalog().getDbOrDdlException(tableName.getDb())
                    .getTableOrDdlException(tableName.getTbl());
            if (table instanceof OlapTable && ((OlapTable) table).getKeysType() == KeysType.AGG_KEYS
                    && columnDef.getAggType() == null) {
                columnDef.setIsKey(true);
                keysSet.add(columnDef.getName());
            }
            if (table instanceof OlapTable) {
                columnDef.setKeysType(((OlapTable) table).getKeysType());
                isOlap = true;
            }
        }

        boolean isEnableMergeOnWrite = false;
        KeysType keysType = KeysType.DUP_KEYS;
        Set<String> clusterKeySet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (isOlap) {
            OlapTable olapTable = (OlapTable) table;
            isEnableMergeOnWrite = olapTable.getEnableUniqueKeyMergeOnWrite();
            keysType = olapTable.getKeysType();
            keysSet.addAll(
                    olapTable.getBaseSchemaKeyColumns().stream().map(Column::getName).collect(Collectors.toList()));
            clusterKeySet.addAll(olapTable.getBaseSchema().stream().filter(Column::isClusterKey).map(Column::getName)
                    .collect(Collectors.toList()));
        }
        columnDef.validate(isOlap, keysSet, clusterKeySet, isEnableMergeOnWrite, keysType);
        if (colPos != null) {
            colPos.analyze();
        }
        if (Strings.isNullOrEmpty(rollupName)) {
            rollupName = null;
        }
        column = columnDef.translateToCatalogStyle();
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
