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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.util.StatisticsUtil;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the command for SHOW ANALYZE TASK STATUS.
 */
public class ShowAnalyzeTaskCommand extends ShowCommand {
    private static final ShowResultSetMetaData ROW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("task_id", ScalarType.createVarchar(100)))
                    .addColumn(new Column("col_name", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("index_name", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("message", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("last_state_change_time", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("time_cost_in_ms", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("state", ScalarType.createVarchar(1000))).build();

    private final long jobId;

    public ShowAnalyzeTaskCommand(long jobId) {
        super(PlanType.SHOW_ANALYZE_TASK_COMMAND);
        this.jobId = jobId;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!ConnectContext.get().getSessionVariable().enableStats) {
            throw new UserException("Analyze function is forbidden, you should add `enable_stats=true`"
                    + "in your FE conf file");
        }
        AnalysisInfo jobInfo = Env.getCurrentEnv().getAnalysisManager().findJobInfo(jobId);
        TableIf table = StatisticsUtil.findTable(jobInfo.catalogId, jobInfo.dbId, jobInfo.tblId);
        List<AnalysisInfo> analysisInfos = Env.getCurrentEnv().getAnalysisManager().findTasks(jobId);
        List<List<String>> rows = new ArrayList<>();
        for (AnalysisInfo analysisInfo : analysisInfos) {
            List<String> row = new ArrayList<>();
            row.add(String.valueOf(analysisInfo.taskId));
            row.add(analysisInfo.colName);
            if (table instanceof OlapTable && analysisInfo.indexId != -1) {
                row.add(((OlapTable) table).getIndexNameById(analysisInfo.indexId));
            } else {
                row.add(table.getName());
            }
            row.add(analysisInfo.message);
            row.add(TimeUtils.getDatetimeFormatWithTimeZone().format(
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.lastExecTimeInMs),
                            ZoneId.systemDefault())));
            row.add(String.valueOf(analysisInfo.timeCostInMs));
            row.add(analysisInfo.state.toString());
            rows.add(row);
        }
        return new ShowResultSet(ROW_META_DATA, rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowAnalyzeTaskCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return ROW_META_DATA;
    }
}

