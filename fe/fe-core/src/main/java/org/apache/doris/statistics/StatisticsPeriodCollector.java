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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class StatisticsPeriodCollector extends StatisticsCollector {
    private static final Logger LOG = LogManager.getLogger(StatisticsPeriodCollector.class);

    public StatisticsPeriodCollector() {
        super("Automatic Analyzer",
                TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes) / 2,
                new AnalysisTaskExecutor(Config.period_analyze_simultaneously_running_task_num));
    }

    @Override
    protected void collect() {
        try {
            AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
            List<AnalysisInfo> jobInfos = analysisManager.findPeriodicJobs();
            for (AnalysisInfo jobInfo : jobInfos) {
                createSystemAnalysisJob(jobInfo);
            }
        } catch (Exception e) {
            LOG.warn("Failed to periodically analyze the statistics." + e);
        }
    }
}
