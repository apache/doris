// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

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

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.proc.AuthProcDir;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.qe.ShowResultSetMetaData;

/*
 *  SHOW ALL GRANTS;
 *      show all grants.
 *      
 *  SHOW GRANTS:
 *      show grants of current user
 *      
 *  SHOW GRANTS FOR user@'xxx';
 *      show grants for specified user identity
 */
//
// SHOW GRANTS;
// SHOW GRANTS FOR user@'xxx'
public class ShowGrantsStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA;
    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String col : AuthProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(col, ColumnType.createVarchar(100)));
        }
        META_DATA = builder.build();
    }

    private boolean isAll;
    private UserIdentity userIdent;

    public ShowGrantsStmt(UserIdentity userIdent, boolean isAll) {
        this.userIdent = userIdent;
        this.isAll = isAll;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public boolean isAll() {
        return isAll;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        if (userIdent != null) {
            if (isAll) {
                throw new AnalysisException("Can not specified keyword ALL when specified user");
            }
            userIdent.analyze(analyzer.getClusterName());
        } else {
            if (!isAll) {
                // self
                userIdent = new UserIdentity(ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP());
                userIdent.setIsAnalyzed();
            }
        }

        UserIdentity self = new UserIdentity(ConnectContext.get().getQualifiedUser(),
                ConnectContext.get().getRemoteIP());

        if (isAll || !self.equals(userIdent)) {
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

}
