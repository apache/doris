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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.AnalysisTaskInfo.JobType;

import com.google.common.base.Preconditions;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class AnalysisTaskScheduler {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskScheduler.class);

    private final PriorityQueue<BaseAnalysisTask> systemJobQueue =
            new PriorityQueue<>(Comparator.comparingInt(BaseAnalysisTask::getLastExecTime));

    private final Queue<BaseAnalysisTask> manualJobQueue = new LinkedList<>();

    private final Set<BaseAnalysisTask> systemJobSet = new HashSet<>();

    private final Set<BaseAnalysisTask> manualJobSet = new HashSet<>();

    public synchronized void scheduleJobs(List<AnalysisTaskInfo> analysisJobInfos) {
        for (AnalysisTaskInfo job : analysisJobInfos) {
            schedule(job);
        }
    }

    public synchronized void schedule(AnalysisTaskInfo analysisJobInfo) {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(analysisJobInfo.catalogName);
        Preconditions.checkArgument(catalog != null);
        DatabaseIf db = catalog.getDbNullable(analysisJobInfo.dbName);
        Preconditions.checkArgument(db != null);
        TableIf table = db.getTableNullable(analysisJobInfo.tblName);
        Preconditions.checkArgument(table != null);
        BaseAnalysisTask analysisTask = table.createAnalysisTask(this, analysisJobInfo);
        addToManualJobQueue(analysisTask);
        if (analysisJobInfo.jobType.equals(JobType.MANUAL)) {
            return;
        }
        addToSystemQueue(analysisTask);
    }

    private void removeFromSystemQueue(BaseAnalysisTask analysisJobInfo) {
        if (manualJobSet.contains(analysisJobInfo)) {
            systemJobQueue.remove(analysisJobInfo);
            manualJobSet.remove(analysisJobInfo);
        }
    }

    private void addToSystemQueue(BaseAnalysisTask analysisJobInfo) {
        if (systemJobSet.contains(analysisJobInfo)) {
            return;
        }
        systemJobSet.add(analysisJobInfo);
        systemJobQueue.add(analysisJobInfo);
        notify();
    }

    private void addToManualJobQueue(BaseAnalysisTask analysisJobInfo) {
        if (manualJobSet.contains(analysisJobInfo)) {
            return;
        }
        manualJobSet.add(analysisJobInfo);
        manualJobQueue.add(analysisJobInfo);
        notify();
    }

    public synchronized BaseAnalysisTask getPendingTasks() {
        while (true) {
            if (!manualJobQueue.isEmpty()) {
                return manualJobQueue.poll();
            }
            if (!systemJobQueue.isEmpty()) {
                return systemJobQueue.poll();
            }
            try {
                wait();
            } catch (Exception e) {
                LOG.warn("Thread get interrupted when waiting for pending jobs", e);
                return null;
            }
        }
    }
}
