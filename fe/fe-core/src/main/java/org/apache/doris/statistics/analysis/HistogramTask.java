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

package org.apache.doris.statistics.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.analysis.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
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
            + "    HISTOGRAM(${colName}, ${maxBucketNum}) AS buckets, "
            + "    NOW() AS create_time "
            + "FROM "
            + "    ${dbName}.${tblName}";

    // ANALYZE WITH HISTOGRAM + MCV: top-N hot values and residual histogram in one scan.
    private static final String ANALYZE_MCV_HISTOGRAM_SQL_TEMPLATE_TABLE = "INSERT INTO "
            + "${internalDB}.${histogramStatTbl} "
            + "WITH src AS (SELECT ${colName} FROM ${dbName}.${tblName}), "
            + "nn AS (SELECT COUNT(${colName}) AS c FROM src), "
            + "hot AS (SELECT t.v, t.c FROM (SELECT ${colName} AS v, COUNT(*) AS c FROM src "
            + "    WHERE ${colName} IS NOT NULL GROUP BY ${colName}) t, nn "
            + "    WHERE t.c / nn.c >= " + StatisticsUtil.HOT_VALUE_MIN_RATIO_SQL
            + " ORDER BY t.c DESC LIMIT ${mcvCount}), "
            + "mcv AS (SELECT GROUP_CONCAT(CONCAT("
            + "    REPLACE(REPLACE(CAST(hot.v AS STRING), ':', '\\\\:'), ';', '\\\\;'), "
            + "    ' :', ROUND(hot.c / nn.c, 4)), ' ;') AS s FROM hot, nn), "
            + "excl AS (SELECT HISTOGRAM(${colName}, ${maxBucketNum}) AS h FROM src "
            + "    WHERE ${colName} NOT IN (SELECT v FROM hot)), "
            + "fh AS (SELECT HISTOGRAM(${colName}, ${maxBucketNum}) AS h FROM src) "
            + "SELECT "
            + "    CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "    ${catalogId} AS catalog_id, "
            + "    ${dbId} AS db_id, "
            + "    ${tblId} AS tbl_id, "
            + "    ${idxId} AS idx_id, "
            + "    '${colId}' AS col_id, "
            + "    ${sampleRate} AS sample_rate, "
            + "    JSON_INSERT(fh.h, '$.mcv_histogram', JSON_OBJECT("
            + "        'mcv', IFNULL(mcv.s, ''), "
            + "        'buckets', IFNULL(JSON_EXTRACT(excl.h, '$.buckets'), JSON_PARSE('[]')))) AS buckets, "
            + "    NOW() AS create_time "
            + "FROM fh, mcv, excl";

    public HistogramTask(AnalysisInfo info) {
        super(info);
    }

    @VisibleForTesting
    static String buildAnalyzeSql(Map<String, String> params, boolean collectMcvHistogram) {
        return new StringSubstitutor(params).replace(collectMcvHistogram
                ? ANALYZE_MCV_HISTOGRAM_SQL_TEMPLATE_TABLE : ANALYZE_HISTOGRAM_SQL_TEMPLATE_TABLE);
    }

    @Override
    public void doExecute() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", SqlUtils.getIdentSql(FeConstants.INTERNAL_DB_NAME));
        params.put("histogramStatTbl", SqlUtils.getIdentSql(StatisticConstants.HISTOGRAM_TBL_NAME));
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", StatisticsUtil.escapeSQL(String.valueOf(info.colName)));
        params.put("dbName", SqlUtils.getIdentSql(db.getFullName()));
        params.put("tblName", SqlUtils.getIdentSql(tbl.getName()));
        params.put("colName", SqlUtils.getIdentSql(String.valueOf(info.colName)));
        params.put("sampleRate", getSampleRateFunction());
        params.put("maxBucketNum", String.valueOf(info.maxBucketNum));
        params.put("mcvCount", String.valueOf(getHotValueCollectCount(info)));

        StatisticsUtil.execUpdate(buildAnalyzeSql(params, info.collectMcvHistogram));
        Env.getCurrentEnv().getStatisticsCache().refreshHistogramSync(
                tbl.getDatabase().getCatalog().getId(), tbl.getDatabase().getId(), tbl.getId(), info.indexId,
                col.getName());
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
