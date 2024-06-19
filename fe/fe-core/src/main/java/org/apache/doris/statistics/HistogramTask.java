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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;

import java.util.HashMap;
import java.util.Map;

/**
 * Each task analyze one column.
 */
public class HistogramTask extends BaseAnalysisTask {

    private static final String ANALYZE_HISTOGRAM_SQL_TEMPLATE_TABLE = "INSERT INTO "
            + "${internalDB}.${histogramStatTbl} "
            + "SELECT "
            + "    CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "    ${catalogId} AS catalog_id, "
            + "    ${dbId} AS db_id, "
            + "    ${tblId} AS tbl_id, "
            + "    ${idxId} AS idx_id, "
            + "    '${colId}' AS col_id, "
            + "    ${sampleRate} AS sample_rate, "
            + "    HISTOGRAM(`${colName}`, ${maxBucketNum}) AS buckets, "
            + "    NOW() AS create_time "
            + "FROM "
            + "    `${dbName}`.`${tblName}`";

    public HistogramTask(AnalysisInfo info) {
        super(info);
    }

    @Override
    public void doExecute() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("histogramStatTbl", StatisticConstants.HISTOGRAM_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", String.valueOf(info.colName));
        params.put("dbName", db.getFullName());
        params.put("tblName", tbl.getName());
        params.put("colName", String.valueOf(info.colName));
        params.put("sampleRate", getSampleRateFunction());
        params.put("maxBucketNum", String.valueOf(info.maxBucketNum));

        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        StatisticsUtil.execUpdate(stringSubstitutor.replace(ANALYZE_HISTOGRAM_SQL_TEMPLATE_TABLE));
        Env.getCurrentEnv().getStatisticsCache().refreshHistogramSync(
                tbl.getDatabase().getCatalog().getId(), tbl.getDatabase().getId(), tbl.getId(), -1, col.getName());
    }

    @Override
    protected void doSample() {
    }

    @Override
    protected void deleteNotExistPartitionStats(AnalysisInfo jobInfo) throws DdlException {
    }

    private String getSampleRateFunction() {
        if (info.analysisMethod == AnalysisMethod.FULL) {
            return "0";
        }
        if (info.samplePercent > 0) {
            return String.valueOf(info.samplePercent / 100.0);
        } else {
            long rowCount = tbl.getRowCount() > 0 ? tbl.getRowCount() : 1;
            double sampRate = (double) info.sampleRows / rowCount;
            return sampRate >= 1 ? "1.0" : String.format("%.4f", sampRate);
        }
    }
}
