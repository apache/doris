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

import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.google.common.base.Strings;

// DROP USER statement
public class DropUserStmt extends DdlStmt {
    private String user;

    public DropUserStmt(String user) {
        this.user = user;
    }

    public String getUser() {
        return user;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(user)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CANNOT_USER, "DROP USER", user);
        }
        user = ClusterNamespace.getFullName(getClusterName(), user);
        // check access
        if (analyzer.getCatalog().getUserMgr().isSuperuser(user)) {
            if (!analyzer.getCatalog().getUserMgr().isAdmin(analyzer.getUser())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP USER");
            }
        } else {
            if (!analyzer.getCatalog().getUserMgr().isSuperuser(analyzer.getUser())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP USER");
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP USER '").append(user).append("'");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
