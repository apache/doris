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

import com.google.common.collect.Lists;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * @author wangcong
 * @version 1.0
 * @date 2020/10/7 10:32 上午
 */
public class CreateTableLikeStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateTableLikeStmt.class);

    private CreateTableStmt parsedCreateTableStmt;
    private final boolean isExternal;
    private final boolean ifNotExists;
    private final TableName tableName;
    private final TableName existedTableName;

    public CreateTableLikeStmt(boolean ifNotExists, boolean isExternal, TableName tableName, TableName existedTableName) {
        this.ifNotExists = ifNotExists;
        this.isExternal = isExternal;
        this.tableName = tableName;
        this.existedTableName = existedTableName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public String getExistedDbName() {
        return existedTableName.getDb();
    }

    public String getExistedTableName() {
        return existedTableName.getTbl();
    }

    public CreateTableStmt getParsedCreateTableStmt() {
        return parsedCreateTableStmt;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        existedTableName.analyze(analyzer);
        ConnectContext ctx = ConnectContext.get();
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ctx, existedTableName.getDb(),
                existedTableName.getTbl(), PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SELECT");
        }

        tableName.analyze(analyzer);
        FeNameFormat.checkTableName(getTableName());
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ctx, tableName.getDb(),
                tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }

        Database db = Catalog.getCurrentCatalog().getDb(getExistedDbName());
        List<String> createTableStmt = Lists.newArrayList();
        db.readLock();
        try {
            Table table = db.getTable(getExistedTableName());
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, getExistedTableName());
            }
            Catalog.getDdlStmt(tableName.getDb(), table, createTableStmt, null, null, false, false);
            if (createTableStmt.isEmpty()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERROR_CREATE_TABLE_LIKE_EMPTY, "CREATE");
            }
        } finally {
            db.readUnlock();
        }
        parsedCreateTableStmt = (CreateTableStmt) SqlParserUtils.parseAndAnalyzeStmt(createTableStmt.get(0), ctx);
        parsedCreateTableStmt.setTableName(getTableName());
    }

    @Override
    public String toSql() {
        return String.format("CREATE TABLE %s LIKE %s", tableName.toSql(), existedTableName.toSql());
    }

    @Override
    public String toString() {
        return toSql();
    }
}
