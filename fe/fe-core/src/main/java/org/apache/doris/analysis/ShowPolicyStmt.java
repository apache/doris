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
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.RowPolicy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import lombok.Getter;

/**
 * Show policy statement
 * syntax:
 * SHOW ROW POLICY [FOR user]
 **/
public class ShowPolicyStmt extends ShowStmt {

    @Getter
    private final PolicyTypeEnum type;

    @Getter
    private final UserIdentity user;

    public ShowPolicyStmt(PolicyTypeEnum type, UserIdentity user) {
        this.type = type;
        this.user = user;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (user != null) {
            user.analyze(analyzer.getClusterName());
        }
        // check auth
        if (!Env.getCurrentEnv().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ").append(type).append(" POLICY");
        switch (type) {
            case STORAGE:
                break;
            case ROW:
            default:
                if (user != null) {
                    sb.append(" FOR ").append(user);
                }
        }
        return sb.toString();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        switch (type) {
            case STORAGE:
                return StoragePolicy.STORAGE_META_DATA;
            case ROW:
            default:
                return RowPolicy.ROW_META_DATA;
        }
    }
}
