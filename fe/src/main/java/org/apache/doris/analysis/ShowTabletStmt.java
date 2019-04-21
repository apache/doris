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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.TabletsProcDir;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

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
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (!isShowSingleTablet && Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }

        // check access
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
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
            builder.addColumn(new Column("DbName", ScalarType.createVarchar(30)));
            builder.addColumn(new Column("TableName", ScalarType.createVarchar(30)));
            builder.addColumn(new Column("PartitionName", ScalarType.createVarchar(30)));
            builder.addColumn(new Column("IndexName", ScalarType.createVarchar(30)));
            builder.addColumn(new Column("DbId", ScalarType.createVarchar(30)));
            builder.addColumn(new Column("TableId", ScalarType.createVarchar(30)));
            builder.addColumn(new Column("PartitionId", ScalarType.createVarchar(30)));
            builder.addColumn(new Column("IndexId", ScalarType.createVarchar(30)));
            builder.addColumn(new Column("IsSync", ScalarType.createVarchar(30)));
            builder.addColumn(new Column("DetailCmd", ScalarType.createVarchar(30)));
        } else {
            for (String title : TabletsProcDir.TITLE_NAMES) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
