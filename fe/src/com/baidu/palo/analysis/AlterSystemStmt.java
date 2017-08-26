// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;

import com.google.common.base.Preconditions;

public class AlterSystemStmt extends DdlStmt {

    private AlterClause alterClause;

    public AlterSystemStmt(AlterClause alterClause) {
        this.alterClause = alterClause;
    }

    public AlterClause getAlterClause() {
        return alterClause;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        if (!analyzer.getCatalog().getUserMgr().isAdmin(analyzer.getUser())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER SYSTEM");
        }

        Preconditions.checkState((alterClause instanceof AddBackendClause)
                || (alterClause instanceof DropBackendClause)
                || (alterClause instanceof DecommissionBackendClause)
                || (alterClause instanceof AddObserverClause)
                || (alterClause instanceof DropObserverClause)
                || (alterClause instanceof AddFollowerClause)
                || (alterClause instanceof DropFollowerClause)
                || (alterClause instanceof ModifyBrokerClause)
                || (alterClause instanceof AlterLoadErrorUrlClause));

        alterClause.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER SYSTEM ").append(alterClause.toSql());
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}

