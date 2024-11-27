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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.proc.AuthProcDir;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;

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
public class ShowGrantsStmt extends ShowStmt implements NotFallbackInParser {

    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String col : AuthProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(100)));
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

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (userIdent != null) {
            if (isAll) {
                throw new AnalysisException("Can not specified keyword ALL when specified user");
            }
            userIdent.analyze();
        } else {
            if (!isAll) {
                // self
                userIdent = ConnectContext.get().getCurrentUserIdentity();
            }
        }
        Preconditions.checkState(isAll || userIdent != null);
        UserIdentity self = ConnectContext.get().getCurrentUserIdentity();

        // if show all grants, or show other user's grants, need global GRANT priv.
        if (isAll || !self.equals(userIdent)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }
        if (userIdent != null && !Env.getCurrentEnv().getAccessManager().getAuth().doesUserExist(userIdent)) {
            throw new AnalysisException(String.format("User: %s does not exist", userIdent));
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

}
