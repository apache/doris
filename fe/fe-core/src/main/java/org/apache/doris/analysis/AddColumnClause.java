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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// clause which is used to add one column to
public class AddColumnClause extends AlterTableClause {
    private static final Logger LOG = LogManager.getLogger(AddColumnClause.class);
    private ColumnDef columnDef;
    // Column position
    private ColumnPosition colPos;
    // if rollupName is null, add to column to base index.
    private String rollupName;

    private Map<String, String> properties;
    // set in analyze
    private Column column;

    public Column getColumn() {
        return column;
    }

    public ColumnPosition getColPos() {
        return colPos;
    }

    public String getRollupName() {
        return rollupName;
    }

    public AddColumnClause(ColumnDef columnDef, ColumnPosition colPos, String rollupName,
                           Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.columnDef = columnDef;
        this.colPos = colPos;
        this.rollupName = rollupName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, DdlException {
        if (columnDef == null) {
            throw new AnalysisException("No column definition in add column clause.");
        }
        if (tableName != null) {
            Table table = Env.getCurrentInternalCatalog().getDbOrDdlException(tableName.getDb())
                    .getTableOrDdlException(tableName.getTbl());
            if (table instanceof OlapTable && ((OlapTable) table).getKeysType() == KeysType.AGG_KEYS
                    && columnDef.getAggregateType() == null) {
                columnDef.setIsKey(true);
            }
            if (table instanceof OlapTable) {
                columnDef.setKeysType(((OlapTable) table).getKeysType());
            }
        }

        columnDef.analyze(true);
        if (colPos != null) {
            colPos.analyze();
        }

        if (!columnDef.isAllowNull() && columnDef.getDefaultValue() == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DEFAULT_FOR_FIELD, columnDef.getName());
        }

        if (columnDef.getAggregateType() != null && colPos != null && colPos.isFirst()) {
            throw new AnalysisException("Cannot add value column[" + columnDef.getName() + "] at first");
        }

        if (Strings.isNullOrEmpty(rollupName)) {
            rollupName = null;
        }

        column = columnDef.toColumn();
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
}
