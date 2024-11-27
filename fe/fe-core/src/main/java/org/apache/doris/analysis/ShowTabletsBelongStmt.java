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

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ShowTabletsBelongStmt is used to show information of tables which tablets are belonged to
 * syntax:
 * SHOW TABLETS BELONG tablet_ids
 */
public class ShowTabletsBelongStmt extends ShowStmt {
    private List<Long> tabletIds;

    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbName")
            .add("TableName")
            .add("TableSize")
            .add("PartitionNum")
            .add("BucketNum")
            .add("ReplicaCount")
            .add("TabletIds")
            .build();

    public ShowTabletsBelongStmt(List<Long> tabletIds) {
        this.tabletIds = tabletIds;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }
        if (tabletIds == null || tabletIds.isEmpty()) {
            throw new UserException("Please supply at least one tablet id");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW TABLETS BELONG ");

        for (long tabletId : tabletIds) {
            sb.append(tabletId);
            sb.append(", ");
        }

        String tmp = sb.toString();
        return tmp.substring(tmp.length() - 1);
    }

    @Override
    public String toString() {
        return toSql();
    }
}
