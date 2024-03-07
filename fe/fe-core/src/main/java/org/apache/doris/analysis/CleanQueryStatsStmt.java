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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.lang3.StringUtils;

/**
 * CLEAN ALL QUERY STATS;
 * CLEAN DATABASE QUERY STATS FROM db;
 * CLEAN TABLE QUERY STATS FROM db.table;
 */
public class CleanQueryStatsStmt extends DdlStmt {
    private String dbName;
    private TableName tableName;
    private Scope scope;

    /**
     * CLEAN DATABASE QUERY STATS FROM db;
     */
    public CleanQueryStatsStmt(String dbName, Scope scope) {
        this.dbName = dbName;
        this.tableName = null;
        this.scope = scope;
    }

    /**
     * CLEAN TABLE QUERY STATS FROM db.table;
     */
    public CleanQueryStatsStmt(TableName tableName, Scope scope) {
        this.dbName = null;
        this.tableName = tableName;
        this.scope = scope;
    }

    public CleanQueryStatsStmt() {
        this.scope = Scope.ALL;
        this.tableName = null;
        this.dbName = null;
    }

    public String getDbName() {
        return dbName;
    }

    public TableName getTableName() {
        return tableName;
    }

    public Scope getScope() {
        return scope;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        switch (scope) {
            case ALL:
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "CLEAN ALL QUERY STATS");
                }
                break;
            case DB:
                if (StringUtils.isEmpty(dbName)) {
                    dbName = analyzer.getDefaultDb();
                }
                if (StringUtils.isEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }

                Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(dbName);
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.ALTER)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "CLEAN DATABASE QUERY STATS FOR " + ClusterNamespace.getNameFromFullName(dbName));
                }
                break;
            case TABLE:
                tableName.analyze(analyzer);
                dbName = tableName.getDb();
                if (StringUtils.isEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }
                DatabaseIf db = Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(dbName);
                db.getTableOrAnalysisException(tableName.getTbl());
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), dbName, tableName.getTbl(), PrivPredicate.ALTER)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "CLEAN TABLE QUERY STATS FROM " + tableName);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + scope);
        }
    }

    @Override
    public String toSql() {
        switch (scope) {
            case ALL:
                return "CLEAN ALL QUERY STATS";
            case DB:
                return "CLEAN DATABASE QUERY STATS FOR " + dbName;
            case TABLE:
                return "CLEAN TABLE QUERY STATS FROM " + tableName;
            default:
                throw new IllegalStateException("Unexpected value: " + scope);
        }
    }

    /**
     * Scope of clean query stats
     */
    public enum Scope {
        ALL, DB, TABLE
    }
}
