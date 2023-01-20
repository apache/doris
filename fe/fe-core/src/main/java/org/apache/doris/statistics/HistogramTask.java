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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.text.StringSubstitutor;

import java.util.HashMap;
import java.util.Map;

/**
 * Each task analyze one column.
 */
public class HistogramTask extends BaseAnalysisTask {

    /** To avoid too much data, use the following efficient sampling method */
    private static final String ANALYZE_HISTOGRAM_SQL_TEMPLATE = "INSERT INTO "
            + "${internalDB}.${histogramStatTbl} "
            + "SELECT "
            + "    CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "    ${catalogId} AS catalog_id, "
            + "    ${dbId} AS db_id, "
            + "    ${tblId} AS tbl_id, "
            + "    ${idxId} AS idx_id, "
            + "    '${colId}' AS col_id, "
            + "    ${sampleRate} AS sample_rate, "
            + "    `HISTOGRAM`(`${colName}`, 1, ${maxBucketNum}) AS buckets, "
            + "    NOW() AS create_time "
            + "FROM "
            + "    `${dbName}`.`${tblName}` TABLESAMPLE (${percentValue} PERCENT)";

    @VisibleForTesting
    public HistogramTask() {
        super();
    }

    public HistogramTask(AnalysisTaskScheduler analysisTaskScheduler, AnalysisTaskInfo info) {
        super(analysisTaskScheduler, info);
    }

    @Override
    public void execute() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("histogramStatTbl", StatisticConstants.HISTOGRAM_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", "-1");
        params.put("colId", String.valueOf(info.colName));
        params.put("dbName", info.dbName);
        params.put("tblName", String.valueOf(info.tblName));
        params.put("colName", String.valueOf(info.colName));
        params.put("sampleRate", String.valueOf(info.sampleRate));
        params.put("maxBucketNum", String.valueOf(info.maxBucketNum));
        params.put("percentValue", String.valueOf((int) (info.sampleRate * 100)));

        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String histogramSql = stringSubstitutor.replace(ANALYZE_HISTOGRAM_SQL_TEMPLATE);
        LOG.info("SQL to collect the histogram:\n {}", histogramSql);

        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            this.stmtExecutor = new StmtExecutor(r.connectContext, histogramSql);
            this.stmtExecutor.execute();
        }

        Env.getCurrentEnv().getStatisticsCache().refreshSync(tbl.getId(), -1, col.getName());
    }
}
