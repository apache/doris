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

import com.baidu.palo.analysis.CompoundPredicate.Operator;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.mysql.privilege.PaloPrivilege;
import com.baidu.palo.mysql.privilege.PrivBitSet;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;

public class ShowClusterStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("cluster").build();

    public ShowClusterStmt() {
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ImmutableList<String> titleNames = null;
        titleNames = TITLE_NAMES;

        for (String title : titleNames) {
            builder.addColumn(new Column(title, ColumnType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public SelectStmt toSelectStmt(Analyzer analyzer) throws AnalysisException {
        return null;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(),
                                                                   PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ADMIN_PRIV,
                                                                                                  PaloPrivilege.NODE_PRIV),
                                                                                    Operator.OR))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        // TODO Auto-generated method stub
        return super.toSql();
    }

}
