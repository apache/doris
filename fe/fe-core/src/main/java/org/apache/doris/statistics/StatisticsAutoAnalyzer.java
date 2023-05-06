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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.TimeUnit;


public class StatisticsAutoAnalyzer extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoAnalyzer.class);

    public StatisticsAutoAnalyzer() {
        super("Automatic Analyzer", TimeUnit.SECONDS.toMillis(Config.auto_check_statistics_in_sec));
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!StatisticsUtil.statsTblAvailable()) {
            return;
        }
        if (Config.enable_auto_collect_statistics) {
            // periodic analyze
            periodicAnalyze();
            // TODO auto analyze
        }
    }

    private void periodicAnalyze() {
        List<ResultRow> resultRows = StatisticsRepository.fetchPeriodicAnalysisJobs();
        if (resultRows.isEmpty()) {
            return;
        }
        try {
            AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
            List<AnalysisTaskInfo> jobInfos = StatisticsUtil.deserializeToAnalysisJob(resultRows);
            for (AnalysisTaskInfo jobInfo : jobInfos) {
                analysisManager.createAnalysisJob(jobInfo);
            }
        } catch (TException | DdlException e) {
            LOG.warn("Failed to periodically analyze the statistics." + e);
        }
    }
}
