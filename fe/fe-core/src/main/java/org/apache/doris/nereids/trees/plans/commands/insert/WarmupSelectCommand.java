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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor.InsertExecutorListener;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.CoordInterface;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TQueryStatistics;
import org.apache.doris.thrift.TQueryStatisticsResult;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * File cache warm up select command, such as "warm up select * from table where ..."
 */
public class WarmupSelectCommand extends InsertIntoTableCommand {

    private static final long QUERY_STATISTICS_TIMEOUT_MS = 5000; // 5 seconds
    private static final long QUERY_STATISTICS_INTERVAL_MS = 1000; // 1 second

    /**
     * WarmupSelectCommand constructor
     */
    public WarmupSelectCommand(LogicalPlan logicalQuery) {
        super(PlanType.INSERT_INTO_BLACKHOLE_COMMAND, logicalQuery, Optional.empty(), Optional.empty(),
                Optional.empty(), true, Optional.empty());
        super.setInsertExecutorListener(new InsertExecutorListener() {

            @Override
            public void beforeComplete(AbstractInsertExecutor insertExecutor, StmtExecutor executor, long jobId)
                    throws Exception {
                // no-ops
            }

            @Override
            public void afterComplete(AbstractInsertExecutor insertExecutor, StmtExecutor executor, long jobId)
                    throws Exception {
                if (insertExecutor.ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                    return;
                }
                Map<Long, TQueryStatisticsResult> statistics = pollForQueryStatistics(insertExecutor.ctx.queryId(),
                        QUERY_STATISTICS_TIMEOUT_MS, QUERY_STATISTICS_INTERVAL_MS);
                sendAggregatedBlackholeResults(statistics, executor);
            }
        });
    }

    /**
     * Poll for query statistics until the query is finished or timeout is reached.
     * Wait for the latest stats from each BE until all BEs have reported isFinished or overall timeout.
     *
     * @param queryId The query ID to poll for
     * @param timeoutMs Maximum time to wait in milliseconds
     * @param intervalMs Time between polls in milliseconds
     * @return Map of backend ID to query statistics result
     */
    private Map<Long, TQueryStatisticsResult> pollForQueryStatistics(TUniqueId queryId,
                                                                     long timeoutMs,
                                                                     long intervalMs) {
        long startTime = System.currentTimeMillis();
        Map<Long, TQueryStatisticsResult> beIdToStatisticsResult;

        // Get the coordinator to know how many backends are involved
        CoordInterface coor = QeProcessorImpl.INSTANCE.getCoordinator(queryId);
        int expectedBackendCount = 0;
        if (coor != null) {
            expectedBackendCount = coor.getInvolvedBackends().size();
        }

        // Track which backends have finished and store the latest statistics for each backend
        Set<Long> finishedBackends = Sets.newHashSet();
        Map<Long, TQueryStatisticsResult> latestStatisticsResult = Maps.newHashMap();

        // Continue polling until all backends are finished or timeout
        while (System.currentTimeMillis() - startTime < timeoutMs && finishedBackends.size() < expectedBackendCount) {
            // Get current statistics
            beIdToStatisticsResult = Env.getCurrentEnv().getWorkloadRuntimeStatusMgr()
                    .getQueryStatistics(DebugUtil.printId(queryId));

            // Update the latest statistics for each backend and check which ones have finished
            if (beIdToStatisticsResult != null && !beIdToStatisticsResult.isEmpty()) {
                for (Map.Entry<Long, TQueryStatisticsResult> entry : beIdToStatisticsResult.entrySet()) {
                    Long beId = entry.getKey();
                    TQueryStatisticsResult result = entry.getValue();

                    // Always update the latest statistics for this backend
                    latestStatisticsResult.put(beId, result);

                    // If this backend has finished, mark it as finished
                    if (result.isSetQueryFinished() && result.isQueryFinished()) {
                        finishedBackends.add(beId);
                    }
                }
            }

            // If not all backends have finished, continue polling
            if (finishedBackends.size() < expectedBackendCount) {
                // Sleep before next poll
                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        return latestStatisticsResult;
    }

    /**
     * Send aggregated blackhole results to the client.
     * Aggregates statistics from all backends and sends a summary result set.
     *
     * @param statisticsResult Map of backend ID to query statistics result
     * @param executor The statement executor to send results through
     * @throws IOException If sending the result set fails
     */
    public void sendAggregatedBlackholeResults(Map<Long, TQueryStatisticsResult> statisticsResult,
            StmtExecutor executor) throws IOException {
        // Create a result set with aggregated data per BE
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("BackendId", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanRows", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanBytes", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanBytesFromLocalStorage", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanBytesFromRemoteStorage", ScalarType.createVarchar(20)))
                .addColumn(new Column("BytesWriteIntoCache", ScalarType.createVarchar(20)))
                .build();

        List<List<String>> rows = Lists.newArrayList();
        long totalScanRows = 0;
        long totalScanBytes = 0;
        long totalScanBytesFromLocalStorage = 0;
        long totalScanBytesFromRemoteStorage = 0;
        long totalBytesWriteIntoCache = 0;

        // Add a row for each BE with its aggregated data
        for (Map.Entry<Long, TQueryStatisticsResult> entry : statisticsResult.entrySet()) {
            Long beId = entry.getKey();
            TQueryStatisticsResult data = entry.getValue();
            if (!data.isSetStatistics()) {
                continue;
            }
            TQueryStatistics statistics = data.getStatistics();
            List<String> row = Lists.newArrayList(
                    beId.toString(),
                    String.valueOf(statistics.getScanRows()),
                    String.valueOf(statistics.getScanBytes()),
                    String.valueOf(statistics.getScanBytesFromLocalStorage()),
                    String.valueOf(statistics.getScanBytesFromRemoteStorage()),
                    String.valueOf(statistics.getBytesWriteIntoCache())
            );

            rows.add(row);

            // Accumulate totals
            totalScanRows += statistics.getScanRows();
            totalScanBytes += statistics.getScanBytes();
            totalScanBytesFromLocalStorage += statistics.getScanBytesFromLocalStorage();
            totalScanBytesFromRemoteStorage += statistics.getScanBytesFromRemoteStorage();
            totalBytesWriteIntoCache += statistics.getBytesWriteIntoCache();
        }

        // Add a total row
        List<String> totalRow = Lists.newArrayList(
                "TOTAL",
                String.valueOf(totalScanRows),
                String.valueOf(totalScanBytes),
                String.valueOf(totalScanBytesFromLocalStorage),
                String.valueOf(totalScanBytesFromRemoteStorage),
                String.valueOf(totalBytesWriteIntoCache)
        );
        rows.add(totalRow);

        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        executor.sendResultSet(resultSet);
    }
}
