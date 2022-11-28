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

public class ShowResourceQueueStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder().addColumn(new Column("QueueId", ScalarType.BIGINT))
                    .addColumn(new Column("QueueName", ScalarType.createVarchar(64)))
                    .addColumn(new Column("PendingJobs", ScalarType.INT))
                    .addColumn(new Column("RunningJobs", ScalarType.INT))
                    .addColumn(new Column("QueueConfig", ScalarType.createStringType()))
                    .addColumn(new Column("MatchingPolicy", ScalarType.createStringType()))
                    .build();

    private final String queueName;

    public ShowResourceQueueStmt(String queueName) {
        this.queueName = queueName;
    }

    public ShowResourceQueueStmt() {
        this.queueName = null;
    }

    public String getQueueName() {
        return queueName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (!Env.getCurrentEnv().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW");
        if (queueName != null) {
            sb.append(" RESOURCE QUEUE");
            sb.append(queueName);
        } else {
            sb.append(" RESOURCE QUEUES");
        }

        return sb.toString();
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
