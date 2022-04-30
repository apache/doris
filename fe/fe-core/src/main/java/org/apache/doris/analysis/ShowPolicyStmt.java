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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import lombok.Getter;

/*
 Show policy statement
 syntax:
      SHOW ROW POLICY [FOR user]
*/
public class ShowPolicyStmt extends ShowStmt {
    
    @Getter
    private String type;
    
    @Getter
    private UserIdentity user;
    
    public ShowPolicyStmt(String type, UserIdentity user) {
        this.type = type;
        this.user = user;
    }
    
    public ShowPolicyStmt() {
    }
    
    private static final ShowResultSetMetaData ROW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("PolicyName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("DbName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("TableName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("FilterType", ScalarType.createVarchar(20)))
                    .addColumn(new Column("WherePredicate", ScalarType.createVarchar(65535)))
                    .addColumn(new Column("User", ScalarType.createVarchar(20)))
                    .addColumn(new Column("OriginStmt", ScalarType.createVarchar(65535)))
                    .build();
    
    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (user != null) {
            user.analyze(analyzer.getClusterName());
        }
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }
    
    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ROW POLICY");
        if (user != null) {
            sb.append(" FOR ").append(user);
        }
        return sb.toString();
    }
    
    @Override
    public ShowResultSetMetaData getMetaData() {
        return ROW_META_DATA;
    }
}
