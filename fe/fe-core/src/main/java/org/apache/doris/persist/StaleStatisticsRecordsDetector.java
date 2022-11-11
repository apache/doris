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

package org.apache.doris.persist;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.AnalysisJobInfo;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.StatisticsUtil;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StaleStatisticsRecordsDetector extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StaleStatisticsRecordsDetector.class);

    private static final String FETCH_STALE_RECORDS_SQL_TEMPLATE = "SELECT * FROM "
            + StatisticConstants.STATISTIC_DB_NAME + "."
            + StatisticConstants.ANALYSIS_JOB_TABLE
            + " WHERE now() - last_exec_time_in_ms > ${expiredTime} "
            + " order by last_exec_time_in_ms";

    public StaleStatisticsRecordsDetector() {
        super("Stale Statistics Records Detector",
                (long) Config.statistics_outdated_record_detector_running_interval_in_minutes * 60 * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        Map<String, String> params = new HashMap<>();
        params.put("expiredTime", String.valueOf(Config.statistics_records_outdated_time_in_ms));
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        List<ResultRow> resultBatches =
                StatisticsUtil.execStatisticQuery(stringSubstitutor.replace(FETCH_STALE_RECORDS_SQL_TEMPLATE));
        List<AnalysisJobInfo> analysisJobInfos = null;
        try {
            analysisJobInfos = StatisticsUtil.deserializeToAnalysisJob(resultBatches);
        } catch (TException e) {
            LOG.warn("Deserialize returned thrift failed!", e);
            return;
        }
        AnalysisJobScheduler analysisJobScheduler = Env.getCurrentEnv().getAnalysisJobScheduler();
        analysisJobScheduler.scheduleJobs(analysisJobInfos);
    }
}
