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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Queues;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/*
Schedule statistics job.
  1. divide job to multi task
  2. submit all task to StatisticsTaskScheduler
Switch job state from pending to scheduling.
 */
public class StatisticsJobScheduler extends MasterDaemon {

    public Queue<StatisticsJob> pendingJobQueue = Queues.newLinkedBlockingQueue();

    public StatisticsJobScheduler() {
        super("Statistics job scheduler", 0);
    }

    @Override
    protected void runAfterCatalogReady() {
        // TODO
        StatisticsJob pendingJob = pendingJobQueue.peek();
        // step0: check job state again
        // step1: divide statistics job to task
        List<StatisticsTask> statisticsTaskList = divide(pendingJob);
        // step2: submit
        Catalog.getCurrentCatalog().getStatisticsTaskScheduler().addTasks(statisticsTaskList);
    }

    public void addPendingJob(StatisticsJob statisticsJob) throws IllegalStateException {
        pendingJobQueue.add(statisticsJob);
    }


    private List<StatisticsTask> divide(StatisticsJob statisticsJob) {
        // TODO
        return new ArrayList<>();
    }
}
