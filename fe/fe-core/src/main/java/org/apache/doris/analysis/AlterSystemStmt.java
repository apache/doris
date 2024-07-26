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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import lombok.Getter;

@Getter
public class AlterSystemStmt extends DdlStmt {

    private final AlterClause alterClause;

    public AlterSystemStmt(AlterClause alterClause) {
        this.alterClause = alterClause;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                                "NODE");
        }

        Preconditions.checkState((alterClause instanceof AddBackendClause)
                || (alterClause instanceof DropBackendClause)
                || (alterClause instanceof DecommissionBackendClause)
                || (alterClause instanceof AddObserverClause)
                || (alterClause instanceof DropObserverClause)
                || (alterClause instanceof AddFollowerClause)
                || (alterClause instanceof DropFollowerClause)
                || (alterClause instanceof ModifyBrokerClause)
                || (alterClause instanceof AlterLoadErrorUrlClause)
                || (alterClause instanceof ModifyBackendClause)
                || (alterClause instanceof ModifyBackendHostNameClause)
                || (alterClause instanceof ModifyFrontendHostNameClause)
        );

        alterClause.analyze(analyzer);
    }

    @Override
    public String toSql() {
        return "ALTER SYSTEM " + alterClause.toSql();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}
