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

package org.apache.doris.statistics;

import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.text.StringSubstitutor;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Each task analyze one column.
 */
public class OlapAnalysisTask extends BaseAnalysisTask {

    private static final String BASIC_STATS_TEMPLATE = "SELECT "
            + "MIN(`${colName}`) as min, "
            + "MAX(`${colName}`) as max "
            + "FROM `${dbName}`.`${tblName}`";

    @VisibleForTesting
    public OlapAnalysisTask() {
    }

    public OlapAnalysisTask(AnalysisInfo info) {
        super(info);
    }

    public void doExecute() throws Exception {

        if (tableSample != null) {
            doSample();
        } else {
            doFull();
        }
    }

    /**
     * 1. Get col stats in sample ways
     * 2. estimate partition stats
     * 3. insert col stats and partition stats
     */
    protected void doSample() throws Exception {
        LOG.debug("Will do sample collection for column {}", col.getName());
        Pair<List<Long>, Long> pair = calcActualSampleTablets(tbl.isPartitionColumn(col.getName()));
        LOG.info("Number of tablets selected {}, rows in tablets {}", pair.first.size(), pair.second);
        List<Long> tabletIds = pair.first;
        double scaleFactor = (double) tbl.getRowCount() / (double) pair.second;
        // might happen if row count in fe metadata hasn't been updated yet
        if (Double.isInfinite(scaleFactor) || Double.isNaN(scaleFactor)) {
            LOG.warn("Scale factor is infinite or Nan, will set scale factor to 1.");
            scaleFactor = 1;
            tabletIds = Collections.emptyList();
            pair.second = tbl.getRowCount();
        }
        String tabletStr = tabletIds.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(info.jobType.equals(JobType.SYSTEM))) {
            // Get basic stats, including min and max.
            ResultRow basicStats = collectBasicStat(r);
            long rowCount = tbl.getRowCount();
            String min = Base64.getEncoder().encodeToString(basicStats.get(0).getBytes(StandardCharsets.UTF_8));
            String max = Base64.getEncoder().encodeToString(basicStats.get(1).getBytes(StandardCharsets.UTF_8));

            boolean limitFlag = false;
            long rowsToSample = pair.second;
            Map<String, String> params = new HashMap<>();
            params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
            params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
            params.put("catalogId", String.valueOf(catalog.getId()));
            params.put("catalogName", catalog.getName());
            params.put("dbId", String.valueOf(db.getId()));
            params.put("tblId", String.valueOf(tbl.getId()));
            params.put("idxId", String.valueOf(info.indexId));
            params.put("colId", String.valueOf(info.colName));
            params.put("dataSizeFunction", getDataSizeFunction(col, false));
            params.put("dbName", db.getFullName());
            params.put("colName", info.colName);
            params.put("tblName", tbl.getName());
            params.put("scaleFactor", String.valueOf(scaleFactor));
            params.put("sampleHints", tabletStr.isEmpty() ? "" : String.format("TABLET(%s)", tabletStr));
            params.put("ndvFunction", getNdvFunction(String.valueOf(rowCount)));
            params.put("min", min);
            params.put("max", max);
            params.put("rowCount", String.valueOf(rowCount));
            params.put("type", col.getType().toString());
            params.put("limit", "");
            if (needLimit()) {
                // If the tablets to be sampled are too large, use limit to control the rows to read, and re-calculate
                // the scaleFactor.
                limitFlag = true;
                rowsToSample = Math.min(getSampleRows(), pair.second);
                params.put("limit", "limit " + rowsToSample);
                params.put("scaleFactor", String.valueOf(scaleFactor * (double) pair.second / rowsToSample));
            }
            StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
            String sql;
            // Single distribution column is not fit for DUJ1 estimator, use linear estimator.
            Set<String> distributionColumns = tbl.getDistributionColumnNames();
            if (distributionColumns.size() == 1 && distributionColumns.contains(col.getName().toLowerCase())) {
                params.put("min", StatisticsUtil.quote(min));
                params.put("max", StatisticsUtil.quote(max));
                sql = stringSubstitutor.replace(LINEAR_ANALYZE_TEMPLATE);
            } else {
                params.put("dataSizeFunction", getDataSizeFunction(col, true));
                sql = stringSubstitutor.replace(DUJ1_ANALYZE_TEMPLATE);
            }
            LOG.info("Sample for column [{}]. Total rows [{}], rows to sample [{}], scale factor [{}], "
                    + "limited [{}], distribute column [{}], partition column [{}], key column [{}]",
                    col.getName(), params.get("rowCount"), rowsToSample, params.get("scaleFactor"),
                    limitFlag, tbl.isDistributionColumn(col.getName()),
                    tbl.isPartitionColumn(col.getName()), col.isKey());
            runQuery(sql, false);
        }
    }

    protected ResultRow collectBasicStat(AutoCloseConnectContext context) {
        Map<String, String> params = new HashMap<>();
        params.put("dbName", db.getFullName());
        params.put("colName", info.colName);
        params.put("tblName", tbl.getName());
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        stmtExecutor = new StmtExecutor(context.connectContext, stringSubstitutor.replace(BASIC_STATS_TEMPLATE));
        return stmtExecutor.executeInternalQuery().get(0);
    }

    /**
     * 1. Get stats of each partition
     * 2. insert partition in batch
     * 3. calculate column stats based on partition stats
     */
    protected void doFull() throws Exception {
        LOG.debug("Will do full collection for column {}", col.getName());
        Set<String> partitionNames = info.colToPartitions.get(info.colName);
        if (partitionNames.isEmpty()) {
            job.appendBuf(this, Collections.emptyList());
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", String.valueOf(info.colName));
        params.put("dataSizeFunction", getDataSizeFunction(col, false));
        params.put("catalogName", catalog.getName());
        params.put("dbName", db.getFullName());
        params.put("colName", String.valueOf(info.colName));
        params.put("tblName", String.valueOf(tbl.getName()));
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String collectColStats = stringSubstitutor.replace(COLLECT_COL_STATISTICS);
        runQuery(collectColStats, true);
    }

    // Get sample tablets id and scale up scaleFactor
    protected Pair<List<Long>, Long> calcActualSampleTablets(boolean forPartitionColumn) {
        // Below code copied from OlapScanNode.java
        long sampleRows; // The total number of sample rows
        long totalRows = 0; // The total number of partition rows hit
        long totalTablet = 0; // The total number of tablets in the hit partition
        OlapTable olapTable = (OlapTable) tbl;
        sampleRows = getSampleRows();

        // calculate the number of tablets by each partition
        long avgRowsPerPartition = sampleRows / Math.max(olapTable.getPartitions().size(), 1);
        List<Long> sampleTabletIds = new ArrayList<>();
        long actualSampledRowCount = 0;
        boolean enough = false;
        for (Partition p : olapTable.getPartitions()) {
            List<Long> ids = p.getBaseIndex().getTabletIdsInOrder();

            if (ids.isEmpty()) {
                continue;
            }

            // Skip partitions with row count < row count / 2 expected to be sampled per partition.
            // It can be expected to sample a smaller number of partitions to avoid uneven distribution
            // of sampling results.
            if (p.getBaseIndex().getRowCount() < (avgRowsPerPartition / 2)) {
                continue;
            }
            MaterializedIndex baseIndex = p.getBaseIndex();
            long avgRowsPerTablet = Math.max(baseIndex.getRowCount() / ids.size(), 1);
            long tabletCounts = Math.max(
                    avgRowsPerPartition / avgRowsPerTablet + (avgRowsPerPartition % avgRowsPerTablet != 0 ? 1 : 0), 1);
            tabletCounts = Math.min(tabletCounts, ids.size());
            long seek = tableSample.getSeek() != -1
                    ? tableSample.getSeek() : (long) (new SecureRandom().nextDouble() * ids.size());
            for (int i = 0; i < tabletCounts; i++) {
                int seekTid = (int) ((i + seek) % ids.size());
                long tabletId = ids.get(seekTid);
                sampleTabletIds.add(tabletId);
                actualSampledRowCount += baseIndex.getTablet(tabletId).getRowCount(true);
                if (actualSampledRowCount >= sampleRows && !forPartitionColumn) {
                    enough = true;
                    break;
                }
            }
            totalRows += p.getBaseIndex().getRowCount();
            totalTablet += ids.size();
            if (enough) {
                break;
            }
        }

        // all hit, direct full
        if (totalRows < sampleRows) {
            // can't fill full sample rows
            sampleTabletIds.clear();
        } else if (sampleTabletIds.size() == totalTablet && !enough) {
            sampleTabletIds.clear();
        }
        return Pair.of(sampleTabletIds, actualSampledRowCount);
    }

    /**
     * For ordinary column (neither key column nor partition column), need to limit sample size to user specified value.
     * @return Return true when need to limit.
     */
    protected boolean needLimit() {
        // Key column is sorted, use limit will cause the ndv not accurate enough, so skip key columns.
        if (col.isKey()) {
            return false;
        }
        // Partition column need to scan tablets from all partitions.
        if (tbl.isPartitionColumn(col.getName())) {
            return false;
        }
        return true;
    }

    /**
     * Calculate rows to sample based on user given sample value.
     * @return Rows to sample.
     */
    protected long getSampleRows() {
        long sampleRows;
        if (tableSample.isPercent()) {
            sampleRows = (long) Math.max(tbl.getRowCount() * (tableSample.getSampleValue() / 100.0), 1);
        } else {
            sampleRows = Math.max(tableSample.getSampleValue(), 1);
        }
        return sampleRows;
    }
}
