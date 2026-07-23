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
import org.apache.doris.analysis.ColumnPath;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Map;

/**
 * RenameColumnOp
 */
public class RenameColumnOp extends AlterTableOp {
    private String colName;
    private ColumnPath columnPath;
    private String newColName;

    public RenameColumnOp(String colName, String newColName) {
        this(ColumnPath.of(colName), newColName);
    }

    public RenameColumnOp(ColumnPath columnPath, String newColName) {
        super(AlterOpType.RENAME);
        this.colName = columnPath.getLeafName();
        this.columnPath = columnPath;
        this.newColName = newColName;
    }

    public String getColName() {
        return colName;
    }

    public ColumnPath getColumnPath() {
        return columnPath;
    }

    public String getNewColName() {
        return newColName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(colName)) {
            throw new AnalysisException("Column name is not set");
        }

        if (Strings.isNullOrEmpty(newColName)) {
            throw new AnalysisException("New column name is not set");
        }

        if (!columnPath.isNested() && colName.startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
            throw new AnalysisException("Do not support rename hidden column");
        }

        if (!columnPath.isNested()) {
            TableIf table = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrDdlException(tableName.getCtl())
                    .getDbOrDdlException(tableName.getDb())
                    .getTableOrDdlException(tableName.getTbl());
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                if (olapTable.hasRowTtl()
                        && colName.equalsIgnoreCase(olapTable.getRowTtlCol())) {
                    throw new AnalysisException(
                            "Can not rename a row ttl source column");
                }
            }
        }

        FeNameFormat.checkColumnName(newColName);
      
        if (columnPath.isNested()) {
            FeNameFormat.checkColumnNameBypassSystemColumnPrefix(newColName);
        } else {
            FeNameFormat.checkColumnName(newColName);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
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
        return "RENAME COLUMN " + columnPath.toSql() + " " + SqlUtils.getIdentSql(newColName);
    }

    @Override
    public String toString() {
        return toSql();
    }
}
