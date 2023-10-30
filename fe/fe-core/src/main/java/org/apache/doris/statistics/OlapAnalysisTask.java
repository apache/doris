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

import java.security.SecureRandom;
import java.util.ArrayList;
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

    private static final String SAMPLE_COLUMN_SQL_TEMPLATE = "SELECT "
            + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "${catalogId} AS catalog_id, "
            + "${dbId} AS db_id, "
            + "${tblId} AS tbl_id, "
            + "${idxId} AS idx_id, "
            + "'${colId}' AS col_id, "
            + "NULL AS part_id, "
            + "ROUND(COUNT(1) * ${scaleFactor}) AS row_count, "
            + NDV_SAMPLE_TEMPLATE
            + "SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) * ${scaleFactor} AS null_count, "
            + "NULL AS min, "
            + "NULL AS max, "
            + "${dataSizeFunction} * ${scaleFactor} AS data_size, "
            + "NOW() "
            + "FROM `${dbName}`.`${tblName}`"
            + "${tablets}";

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
        Pair<List<Long>, Long> pair = calcActualSampleTablets();
        List<Long> tabletIds = pair.first;
        double scaleFactor = (double) tbl.getRowCount() / (double) pair.second;
        // might happen if row count in fe metadata hasn't been updated yet
        if (Double.isInfinite(scaleFactor) || Double.isNaN(scaleFactor)) {
            scaleFactor = 1;
            tabletIds = Collections.emptyList();
        }
        String tabletStr = tabletIds.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(info.jobType.equals(JobType.SYSTEM))) {
            Map<String, String> params = new HashMap<>();
            params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
            params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
            params.put("catalogId", String.valueOf(catalog.getId()));
            params.put("dbId", String.valueOf(db.getId()));
            params.put("tblId", String.valueOf(tbl.getId()));
            params.put("idxId", String.valueOf(info.indexId));
            params.put("colId", String.valueOf(info.colName));
            params.put("dataSizeFunction", getDataSizeFunction(col));
            params.put("dbName", db.getFullName());
            params.put("colName", info.colName);
            params.put("tblName", tbl.getName());
            params.put("scaleFactor", String.valueOf(scaleFactor));
            params.put("tablets", tabletStr.isEmpty() ? "" : String.format("TABLET(%s)", tabletStr));
            StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
            stmtExecutor = new StmtExecutor(r.connectContext, stringSubstitutor.replace(SAMPLE_COLUMN_SQL_TEMPLATE));
            // Scalar query only return one row
            ColStatsData colStatsData = new ColStatsData(stmtExecutor.executeInternalQuery().get(0));
            job.appendBuf(this, Collections.singletonList(colStatsData));
        }
    }

    /**
     * 1. Get stats of each partition
     * 2. insert partition in batch
     * 3. calculate column stats based on partition stats
     */
    protected void doFull() throws Exception {
        Set<String> partitionNames = info.colToPartitions.get(info.colName);
        if (partitionNames.isEmpty()) {
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
        params.put("dataSizeFunction", getDataSizeFunction(col));
        params.put("dbName", db.getFullName());
        params.put("colName", String.valueOf(info.colName));
        params.put("tblName", String.valueOf(tbl.getName()));
        execSQL(params);
    }

    @VisibleForTesting
    public void execSQL(Map<String, String> params) throws Exception {
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String collectColStats = stringSubstitutor.replace(COLLECT_COL_STATISTICS);
        runQuery(collectColStats);
    }

    // Get sample tablets id and scale up scaleFactor
    protected Pair<List<Long>, Long> calcActualSampleTablets() {
        // Below code copied from OlapScanNode.java
        long sampleRows; // The total number of sample rows
        long totalRows = 0; // The total number of partition rows hit
        long totalTablet = 0; // The total number of tablets in the hit partition
        OlapTable olapTable = (OlapTable) tbl;
        if (tableSample.isPercent()) {
            sampleRows = (long) Math.max(olapTable.getRowCount() * (tableSample.getSampleValue() / 100.0), 1);
        } else {
            sampleRows = Math.max(tableSample.getSampleValue(), 1);
        }

        // calculate the number of tablets by each partition
        long avgRowsPerPartition = sampleRows / Math.max(olapTable.getPartitions().size(), 1);
        List<Long> sampleTabletIds = new ArrayList<>();
        long actualSampledRowCount = 0;
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
            }

            totalRows += p.getBaseIndex().getRowCount();
            totalTablet += ids.size();
        }

        // all hit, direct full
        if (totalRows < sampleRows) {
            // can't fill full sample rows
            sampleTabletIds.clear();
        } else if (sampleTabletIds.size() == totalTablet) {
            // TODO add limit
            sampleTabletIds.clear();
        } else if (!sampleTabletIds.isEmpty()) {
            // TODO add limit
        }
        return Pair.of(sampleTabletIds, actualSampledRowCount);
    }
}
