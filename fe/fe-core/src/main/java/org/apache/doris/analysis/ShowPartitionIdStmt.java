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

// SHOW PARTITION ID
public class ShowPartitionIdStmt extends ShowStmt implements NotFallbackInParser {
    private long partitionId;

    public ShowPartitionIdStmt(long partitionId) {
        this.partitionId = partitionId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        // check access first
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW PARTITION");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW");
        sb.append(" PARTITION ");
        sb.append(this.partitionId);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("DbName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("TableName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("PartitionName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("DbId", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("TableId", ScalarType.createVarchar(30)));
        return builder.build();
    }
}
