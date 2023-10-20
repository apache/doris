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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

/**
 * @author wangcong
 * @version 1.0
 * @date 2020/10/7 10:32 上午
 */
public class CreateTableLikeStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateTableLikeStmt.class);

    private final boolean ifNotExists;
    private final TableName tableName;
    private final TableName existedTableName;
    private final ArrayList<String> rollupNames;
    private final boolean withAllRollup;

    public CreateTableLikeStmt(boolean ifNotExists, TableName tableName, TableName existedTableName,
            ArrayList<String> rollupNames, boolean withAllRollup) throws DdlException {
        this.ifNotExists = ifNotExists;
        this.tableName = tableName;
        this.existedTableName = existedTableName;
        if (!CollectionUtils.isEmpty(rollupNames) && withAllRollup) {
            throw new DdlException("Either all or part of the rollup can be copied, not both");
        }
        this.rollupNames = rollupNames;
        this.withAllRollup = withAllRollup;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
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

    public ArrayList<String> getRollupNames() {
        return rollupNames;
    }

    public boolean isWithAllRollup() {
        return withAllRollup;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        existedTableName.analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(existedTableName.getCtl(), this.getClass().getSimpleName());
        ConnectContext ctx = ConnectContext.get();
        if (!skipAuth && !Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, existedTableName.getDb(),
                existedTableName.getTbl(), PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SELECT");
        }

        tableName.analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());
        FeNameFormat.checkTableName(getTableName());
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, tableName.getDb(),
                tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName.toSql()).append(" LIKE ").append(existedTableName.toSql());
        if (withAllRollup && CollectionUtils.isEmpty(rollupNames)) {
            sb.append(" WITH ROLLUP");
        }
        if (!withAllRollup && !CollectionUtils.isEmpty(rollupNames)) {
            sb.append(" WITH ROLLUP (").append(Joiner.on(",").join(rollupNames)).append(")");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
