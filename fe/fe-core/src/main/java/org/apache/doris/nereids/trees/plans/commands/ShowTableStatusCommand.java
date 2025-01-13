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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.util.TimeUtils;
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

/**
 * ShowVariablesCommand
 */
public class ShowTableStatusCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Name", ScalarType.createVarchar(64)))
            .addColumn(new Column("Engine", ScalarType.createVarchar(10)))
            .addColumn(new Column("Version", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Row_format", ScalarType.createVarchar(64)))
            .addColumn(new Column("Rows", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Avg_row_length", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Data_length", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Max_data_length", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Index_length", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Data_free", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Auto_increment", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Create_time", ScalarType.createType(PrimitiveType.DATETIME)))
            .addColumn(new Column("Update_time", ScalarType.createType(PrimitiveType.DATETIME)))
            .addColumn(new Column("Check_time", ScalarType.createType(PrimitiveType.DATETIME)))
            .addColumn(new Column("Collation", ScalarType.createVarchar(64)))
            .addColumn(new Column("Checksum", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("Create_options", ScalarType.createVarchar(64)))
            .addColumn(new Column("Comment", ScalarType.createVarchar(64)))
            .build();

    private String catalog;
    private String db;
    private String wild;

    public ShowTableStatusCommand(String catalog, String db, String wild) {
        super(PlanType.SHOW_TABLE_STATUS_COMMAND);
        this.catalog = catalog;
        this.db = db;
        this.wild = wild;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDb() {
        return db;
    }

    public String getPattern() {
        return wild;
    }

    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (Strings.isNullOrEmpty(db)) {
            db = ctx.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = ctx.getDefaultCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FOR_CATALOG);
            }
        }

        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ctx,
                catalog, db, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString(), db);
        }

        List<List<String>> rows = Lists.newArrayList();
        DatabaseIf<TableIf> dbName = ctx.getEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(getCatalog())
                .getDbOrAnalysisException(getDb());
        if (dbName != null) {
            PatternMatcher matcher = null;
            if (getPattern() != null) {
                matcher = PatternMatcherWrapper.createMysqlPattern(getPattern(),
                    CaseSensibility.TABLE.getCaseSensibility());
            }
            for (TableIf table : dbName.getTables()) {
                if (matcher != null && !matcher.match(table.getName())) {
                    continue;
                }

                // check tbl privs
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ctx, getCatalog(),
                            dbName.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                    continue;
                }
                List<String> row = Lists.newArrayList();
                // Name
                row.add(table.getName());
                // Engine
                row.add(table.getEngine());
                // version
                row.add(null);
                // Row_format
                row.add(null);
                // Rows
                row.add(String.valueOf(table.getCachedRowCount()));
                // Avg_row_length
                row.add(String.valueOf(table.getAvgRowLength()));
                // Data_length
                row.add(String.valueOf(table.getDataLength()));
                // Max_data_length
                row.add(null);
                // Index_length
                row.add(null);
                // Data_free
                row.add(null);
                // Auto_increment
                row.add(null);
                // Create_time
                row.add(TimeUtils.longToTimeString(table.getCreateTime() * 1000));
                // Update_time
                if (table.getUpdateTime() > 0) {
                    row.add(TimeUtils.longToTimeString(table.getUpdateTime()));
                } else {
                    row.add(null);
                }
                // Check_time
                if (table.getLastCheckTime() > 0) {
                    row.add(TimeUtils.longToTimeString(table.getLastCheckTime()));
                } else {
                    row.add(null);
                }
                // Collation
                row.add("utf-8");
                // Checksum
                row.add(null);
                // Create_options
                row.add(null);

                row.add(table.getComment());
                rows.add(row);
            }
        }
        // sort by table name
        rows.sort((x, y) -> {
            return x.get(0).compareTo(y.get(0));
        });
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableStatusCommand(this, context);
    }

}
