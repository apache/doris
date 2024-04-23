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

import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;

/** CreateTableLikeInfo */
public class CreateTableLikeInfo {
    private final boolean ifNotExists;
    private final TableNameInfo tableName;
    private final TableNameInfo existedTableName;
    private final ArrayList<String> rollupNames;
    private final boolean withAllRollup;

    public CreateTableLikeInfo(boolean ifNotExists, TableNameInfo tableName, TableNameInfo existedTableName,
            ArrayList<String> rollupNames, boolean withAllRollup) {
        this.ifNotExists = ifNotExists;
        this.tableName = tableName;
        this.existedTableName = existedTableName;
        this.rollupNames = rollupNames;
        this.withAllRollup = withAllRollup;
    }

    public CreateTableLikeStmt translateToLegacyStmt() throws DdlException {
        return new CreateTableLikeStmt(ifNotExists, tableName.transferToTableName(),
                existedTableName.transferToTableName(), rollupNames, withAllRollup);
    }

    /** validate */
    public void validate(ConnectContext ctx) throws AnalysisException {
        existedTableName.analyze(ctx);
        // disallow external catalog
        Util.prohibitExternalCatalog(existedTableName.getCtl(), "CreateTableLikeStmt");
        //check privilege
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ctx, existedTableName.getCtl(), existedTableName.getDb(),
                        existedTableName.getTbl(), PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SELECT");
        }

        tableName.analyze(ctx);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), "CreateTableLikeStmt");
        FeNameFormat.checkTableName(tableName.getTbl());
        //check privilege
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, tableName.getCtl(), tableName.getDb(),
                tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }
    }
}
