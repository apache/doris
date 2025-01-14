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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.DbName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/**
 * Represents the command for SHOW CREATE DATABASE.
 */
public class ShowCreateDatabaseCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Database", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create Database", ScalarType.createVarchar(30)))
                    .build();

    private final String databaseName;
    private final String catalogName;

    public ShowCreateDatabaseCommand(DbName dbName) {
        super(PlanType.SHOW_CREATE_DATABASE_COMMAND);
        this.databaseName = Objects.requireNonNull(dbName.getDb(), "Database name cannot be null");
        this.catalogName = dbName.getCtl();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        String ctlgName = catalogName;
        if (Strings.isNullOrEmpty(catalogName)) {
            ctlgName = Env.getCurrentEnv().getCurrentCatalog().getName();
        }
        if (Strings.isNullOrEmpty(databaseName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, databaseName);
        }

        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), ctlgName, databaseName,
                PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR,
                    PrivPredicate.SHOW.getPrivs().toString(), databaseName);
        }

        List<List<String>> rows = Lists.newArrayList();

        StringBuilder sb = new StringBuilder();
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(ctlgName);
        if (catalog instanceof HMSExternalCatalog) {
            String simpleDBName = ClusterNamespace.getNameFromFullName(databaseName);
            org.apache.hadoop.hive.metastore.api.Database db = ((HMSExternalCatalog) catalog).getClient()
                    .getDatabase(simpleDBName);
            sb.append("CREATE DATABASE `").append(simpleDBName).append("`")
                    .append(" LOCATION '")
                    .append(db.getLocationUri())
                    .append("'");
        } else if (catalog instanceof IcebergExternalCatalog) {
            IcebergExternalDatabase db = (IcebergExternalDatabase) catalog.getDbOrAnalysisException(databaseName);
            sb.append("CREATE DATABASE `").append(databaseName).append("`")
                .append(" LOCATION '")
                .append(db.getLocation())
                .append("'");
        } else {
            DatabaseIf db = catalog.getDbOrAnalysisException(databaseName);
            sb.append("CREATE DATABASE `").append(ClusterNamespace.getNameFromFullName(databaseName)).append("`");
            if (db.getDbProperties().getProperties().size() > 0) {
                sb.append("\nPROPERTIES (\n");
                sb.append(new PrintableMap<>(db.getDbProperties().getProperties(), "=", true, true, false));
                sb.append("\n)");
            }
        }

        rows.add(Lists.newArrayList(ClusterNamespace.getNameFromFullName(databaseName), sb.toString()));
        return new ShowResultSet(this.getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateDatabaseCommand(this, context);
    }

    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
