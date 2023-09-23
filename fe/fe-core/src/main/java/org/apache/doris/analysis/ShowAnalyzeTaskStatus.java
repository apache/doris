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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

/**
 * SHOW ANALYZE TASK STATUS [JOB_ID]
 */
public class ShowAnalyzeTaskStatus extends ShowStmt {

    private static final ShowResultSetMetaData ROW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("task_id", ScalarType.createVarchar(100)))
                    .addColumn(new Column("col_name", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("message", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("last_state_change_time", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("time_cost_in_ms", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("state", ScalarType.createVarchar(1000))).build();

    private final long jobId;

    public ShowAnalyzeTaskStatus(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!Config.enable_stats) {
            throw new UserException("Analyze function is forbidden, you should add `enable_stats=true`"
                    + "in your FE conf file");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return ROW_META_DATA;
    }

    public long getJobId() {
        return jobId;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
