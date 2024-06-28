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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import lombok.Getter;

/**
 * Show objects where storage policy is used
 * syntax:
 * SHOW STORAGE POLICY USING [for policy_name]
 **/
public class ShowStoragePolicyUsingStmt extends ShowStmt {

    public static final ShowResultSetMetaData RESULT_META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("PolicyName", ScalarType.createVarchar(100)))
                .addColumn(new Column("Database", ScalarType.createVarchar(20)))
                .addColumn(new Column("Table", ScalarType.createVarchar(20)))
                .addColumn(new Column("Partitions", ScalarType.createVarchar(60)))
                .build();
    @Getter
    private final String policyName;

    public ShowStoragePolicyUsingStmt(String policyName) {
        this.policyName = policyName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW STORAGE POLICY USING");
        if (policyName != null) {
            sb.append(" FOR ").append(policyName);
        }

        return sb.toString();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return RESULT_META_DATA;
    }
}
