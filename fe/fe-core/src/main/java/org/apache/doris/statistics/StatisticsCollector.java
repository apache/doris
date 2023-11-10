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
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public abstract class StatisticsCollector extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(StatisticsCollector.class);

    protected final AnalysisTaskExecutor analysisTaskExecutor;


    public StatisticsCollector(String name, long intervalMs, AnalysisTaskExecutor analysisTaskExecutor) {
        super(name, intervalMs);
        this.analysisTaskExecutor = analysisTaskExecutor;
        analysisTaskExecutor.start();
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!StatisticsUtil.statsTblAvailable()) {
            LOG.info("Stats table not available, skip");
            return;
        }
        if (Env.isCheckpointThread()) {
            return;
        }

        if (!analysisTaskExecutor.idle()) {
            LOG.info("Analyze tasks those submitted in last time is not finished, skip");
            return;
        }
        collect();
    }

    protected abstract void collect();

    // Analysis job created by the system
    @VisibleForTesting
    protected void createSystemAnalysisJob(AnalysisInfo jobInfo)
            throws DdlException {
        if (jobInfo.colToPartitions.isEmpty()) {
            // No statistics need to be collected or updated
            return;
        }

        Map<Long, BaseAnalysisTask> analysisTasks = new HashMap<>();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        analysisManager.createTaskForEachColumns(jobInfo, analysisTasks, false);
        Env.getCurrentEnv().getAnalysisManager().constructJob(jobInfo, analysisTasks.values());
        if (StatisticsUtil.isExternalTable(jobInfo.catalogId, jobInfo.dbId, jobInfo.tblId)) {
            analysisManager.createTableLevelTaskForExternalTable(jobInfo, analysisTasks, false);
        }
        Env.getCurrentEnv().getAnalysisManager().registerSysJob(jobInfo, analysisTasks);
        analysisTasks.values().forEach(analysisTaskExecutor::submitTask);
    }

}
