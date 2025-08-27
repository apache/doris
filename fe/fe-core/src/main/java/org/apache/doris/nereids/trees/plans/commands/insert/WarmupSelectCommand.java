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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.profile.ProfileManager;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor.InsertExecutorListener;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.BlackholeResultHandler.BeBlackholeAggregatedData;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TQueryStatistics;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class WarmupSelectCommand extends InsertIntoBlackholeCommand {

    public WarmupSelectCommand(LogicalPlan logicalQuery) {
        super(logicalQuery);
        super.setInsertExecutorListener(new InsertExecutorListener() {

            @Override
            public void beforeComplete(AbstractInsertExecutor insertExecutor, StmtExecutor executor, long jobId) throws Exception {
                // no-ops
            }

            @Override
            public void afterComplete(AbstractInsertExecutor insertExecutor, StmtExecutor executor, long jobId) throws Exception {
                if (insertExecutor.ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                    return;
                }
                String queryId = DebugUtil.printId(insertExecutor.ctx.queryId());
                Map<String, TQueryStatistics> statistics = ProfileManager.getInstance().getQueryStatistics(queryId);
                sendAggregatedBlackholeResults(statistics, executor);
            }
        });
    }

    // @Override
    // public RedirectStatus toRedirectStatus() {
    //     // must run by master
    //     return RedirectStatus.FORWARD_WITH_SYNC;
    // }

    public void sendAggregatedBlackholeResults(Map<String, TQueryStatistics> statistics, StmtExecutor executor) throws IOException {
        // Create a result set with aggregated data per BE
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("BackendId", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanRows", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanBytes", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanBytesFromLocalStorage", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanBytesFromRemoteStorage", ScalarType.createVarchar(20)))
                .build();

        List<List<String>> rows = Lists.newArrayList();
        long totalScanRows = 0;
        long totalScanBytes = 0;
        long totalScanBytesFromLocalStorage = 0;
        long totalScanBytesFromRemoteStorage = 0;

        // Add a row for each BE with its aggregated data
        for (Map.Entry<String, TQueryStatistics> entry : statistics.entrySet()) {
            String beId = entry.getKey();
            TQueryStatistics data = entry.getValue();

            List<String> row = Lists.newArrayList(
                    beId,
                    String.valueOf(data.getScanRows()),
                    String.valueOf(data.getScanBytes()),
                    String.valueOf(data.getScanBytesFromLocalStorage()),
                    String.valueOf(data.getScanBytesFromRemoteStorage())
            );

            rows.add(row);

            // Accumulate totals
            totalScanRows += data.getScanRows();
            totalScanBytes += data.getScanBytes();
            totalScanBytesFromLocalStorage += data.getScanBytesFromLocalStorage();
            totalScanBytesFromRemoteStorage += data.getScanBytesFromRemoteStorage();
        }

        // Add a total row
        List<String> totalRow = Lists.newArrayList(
                "TOTAL",
                String.valueOf(totalScanRows),
                String.valueOf(totalScanBytes),
                String.valueOf(totalScanBytesFromLocalStorage),
                String.valueOf(totalScanBytesFromRemoteStorage)
        );
        rows.add(totalRow);

        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        executor.sendResultSet(resultSet);
    }
}
