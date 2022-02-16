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

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.catalog.Catalog;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

/*
For unified management of statistics job,
including job addition, cancellation, scheduling, etc.
 */
public class StatisticsJobManager {
    private static final Logger LOG = LogManager.getLogger(StatisticsJobManager.class);

    // statistics job
    private Map<Long, StatisticsJob> idToStatisticsJob = Maps.newConcurrentMap();

    public void createStatisticsJob(AnalyzeStmt analyzeStmt) {
        // step0: init statistics job by analyzeStmt
        StatisticsJob statisticsJob = StatisticsJob.fromAnalyzeStmt(analyzeStmt);
        // step1: get statistics to be analyzed
        Set<Long> tableIdList = statisticsJob.relatedTableId();
        // step2: check restrict
        checkRestrict(tableIdList);
        // step3: check permission
        checkPermission();
        // step4: create it
        createStatisticsJob(statisticsJob);
    }

    public void createStatisticsJob(StatisticsJob statisticsJob) {
        idToStatisticsJob.put(statisticsJob.getId(), statisticsJob);
        try {
            Catalog.getCurrentCatalog().getStatisticsJobScheduler().addPendingJob(statisticsJob);
        } catch (IllegalStateException e) {
            LOG.info("The pending statistics job is full. Please submit it again later.");
        }
    }

    // Rule1: The same table cannot have two unfinished statistics jobs
    // Rule2: The unfinished statistics job could not more then Config.max_statistics_job_num
    // Rule3: The job for external table is not supported
    private void checkRestrict(Set<Long> tableIdList) {
        // TODO
    }

    private void checkPermission() {
        // TODO
    }
}
