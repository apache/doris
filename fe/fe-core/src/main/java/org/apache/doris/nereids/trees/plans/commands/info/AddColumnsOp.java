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
import org.apache.doris.analysis.AddColumnsClause;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * AddColumnsOp
 */
public class AddColumnsOp extends AlterTableOp {
    private List<ColumnDefinition> columnDefs;
    private String rollupName;

    private Map<String, String> properties;
    // set in analyze
    private List<Column> columns;

    public AddColumnsOp(List<ColumnDefinition> columnDefs, String rollupName, Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.columnDefs = columnDefs;
        this.rollupName = rollupName;
        this.properties = properties;
    }

    public AddColumnsOp(String rollupName, Map<String, String> properties, List<Column> columns) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.columnDefs = Collections.emptyList();
        this.rollupName = rollupName;
        this.properties = properties;
        this.columns = columns;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getRollupName() {
        return rollupName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (columnDefs == null || columnDefs.isEmpty()) {
            throw new AnalysisException("Columns is empty in add columns clause.");
        }
        for (ColumnDefinition colDef : columnDefs) {
            AddColumnOp.validateColumnDef(tableName, colDef, null, rollupName);
        }

        TableIf table = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrDdlException(tableName.getCtl())
                .getDbOrDdlException(tableName.getDb())
                .getTableOrDdlException(tableName.getTbl());
        if (table instanceof OlapTable) {
            boolean seeValueColumn = false;
            for (ColumnDefinition colDef : columnDefs) {
                if (seeValueColumn && colDef.isKey()) {
                    throw new AnalysisException(
                            String.format("Cannot add key column %s after value column", colDef.getName()));
                }
                seeValueColumn = seeValueColumn || !colDef.isKey();
            }
        }

        columns = Lists.newArrayList();
        for (ColumnDefinition columnDef : columnDefs) {
            Column col = columnDef.translateToCatalogStyle();
            columns.add(col);
        }
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new AddColumnsClause(toSql(), columns, rollupName, properties);
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
        sb.append("ADD COLUMN (");
        int idx = 0;
        for (ColumnDefinition columnDef : columnDefs) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append(columnDef.toSql());
            idx++;
        }
        sb.append(")");
        if (rollupName != null) {
            sb.append(" IN `").append(rollupName).append("`");
        }
        return sb.toString();
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
