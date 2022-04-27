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

public class AdminShowTabletStorageFormatStmt extends ShowStmt {
    private boolean verbose;

    @Override
    public boolean isVerbose() {
        return verbose;
    }

    public AdminShowTabletStorageFormatStmt(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // check access first
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_ACCESS_DENIED_ERROR,
                    toSql(),
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(), "ADMIN Privilege needed.");
        }

        super.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("ADMIN SHOW TABLET STORAGE TYPE");
        if (verbose) {
            sb.append(" VERBOSE");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (verbose) {
            builder.addColumn(new Column("BackendId", ScalarType.createVarchar(30)))
                    .addColumn(new Column("TabletId", ScalarType.createVarchar(30)))
                    .addColumn(new Column("StorageFormat", ScalarType.createVarchar(30)));
        } else {
            builder.addColumn(new Column("BackendId", ScalarType.createVarchar(30)))
                    .addColumn(new Column("V1Count", ScalarType.createVarchar(30)))
                    .addColumn(new Column("V2Count", ScalarType.createVarchar(30)));
        }
        return builder.build();
    }
}
