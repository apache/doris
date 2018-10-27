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
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.qe.ShowResultSetMetaData;

public class ShowRolesStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;
    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        builder.addColumn(new Column("Name", ColumnType.createVarchar(100)));
        builder.addColumn(new Column("Users", ColumnType.createVarchar(100)));
        builder.addColumn(new Column("GlobalPrivs", ColumnType.createVarchar(300)));
        builder.addColumn(new Column("DatabasePrivs", ColumnType.createVarchar(300)));
        builder.addColumn(new Column("TablePrivs", ColumnType.createVarchar(300)));

        META_DATA = builder.build();
    }

    public ShowRolesStmt() {

    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

}
