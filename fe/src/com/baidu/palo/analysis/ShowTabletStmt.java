// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.proc.TabletsProcDir;
import com.baidu.palo.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

public class ShowTabletStmt extends ShowStmt {

    private String dbName;
    private String tableName;
    private long tabletId;

    private boolean isShowSingleTablet;

    public ShowTabletStmt(TableName dbTableName, long tabletId) {
        if (dbTableName == null) {
            this.dbName = null;
            this.tableName = null;
            this.isShowSingleTablet = true;
        } else {
            this.dbName = dbTableName.getDb();
            this.tableName = dbTableName.getTbl();
            this.isShowSingleTablet = false;
        }
        this.tabletId = tabletId;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTabletId() {
        return tabletId;
    }

    public boolean isShowSingleTablet() {
        return isShowSingleTablet;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        if (!isShowSingleTablet && Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getDbFullName(getClusterName(), dbName);
        }

        // check access
        if (!analyzer.getCatalog().getUserMgr().isSuperuser(analyzer.getUser())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW TABLET");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW TABLET ");
        if (isShowSingleTablet) {
            sb.append(tabletId);
        } else {
            sb.append("`").append(dbName).append("`.`").append(tableName).append("`");
        }
        return sb.toString();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (isShowSingleTablet) {
            builder.addColumn(new Column("DbName", ColumnType.createVarchar(30)));
            builder.addColumn(new Column("TableName", ColumnType.createVarchar(30)));
            builder.addColumn(new Column("PartitionName", ColumnType.createVarchar(30)));
            builder.addColumn(new Column("IndexName", ColumnType.createVarchar(30)));
            builder.addColumn(new Column("DbId", ColumnType.createVarchar(30)));
            builder.addColumn(new Column("TableId", ColumnType.createVarchar(30)));
            builder.addColumn(new Column("PartitionId", ColumnType.createVarchar(30)));
            builder.addColumn(new Column("IndexId", ColumnType.createVarchar(30)));
            builder.addColumn(new Column("IsSync", ColumnType.createVarchar(30)));
        } else {
            for (String title : TabletsProcDir.TITLE_NAMES) {
                builder.addColumn(new Column(title, ColumnType.createVarchar(30)));
            }
        }
        return builder.build();
    }
}
