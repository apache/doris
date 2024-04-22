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
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

// Show plugins statement.
// TODO(zhaochun): only for support MySQL
public class ShowPluginsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Description", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Version", ScalarType.createVarchar(20)))
                    .addColumn(new Column("JavaVersion", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ClassName", ScalarType.createVarchar(64)))
                    .addColumn(new Column("SoName", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Sources", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(250)))
                    .build();

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }
    }

    @Override
    public String toSql() {
        return "SHOW PLUGINS";
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
