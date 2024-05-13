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
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;

// show data skew from tbl [partition(p1, p2, ...)]
public class ShowDataSkewStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionName").add("BucketIdx").add("AvgRowCount").add("AvgDataSize")
            .add("Graph").add("Percent")
            .build();

    private TableRef tblRef;

    public ShowDataSkewStmt(TableRef tblRef) {
        this.tblRef = tblRef;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        tblRef.getName().analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(tblRef.getName().getCtl(), this.getClass().getSimpleName());
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tblRef.getName().getCtl(), tblRef.getName().getDb(),
                        tblRef.getName().getTbl(),
                        PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW DATA SKEW",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tblRef.getName().getDb() + "." + tblRef.getName().getTbl());
        }
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    public PartitionNames getPartitionNames() {
        return tblRef.getPartitionNames();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
