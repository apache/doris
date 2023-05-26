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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class AnalysisTaskScheduler {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskScheduler.class);

    private final PriorityQueue<BaseAnalysisTask> systemJobQueue =
            new PriorityQueue<>(Comparator.comparingLong(BaseAnalysisTask::getLastExecTime));

    private final Queue<BaseAnalysisTask> manualJobQueue = new LinkedList<>();

    private final Set<BaseAnalysisTask> systemJobSet = new HashSet<>();

    private final Set<BaseAnalysisTask> manualJobSet = new HashSet<>();

    public synchronized void schedule(BaseAnalysisTask analysisTask) {
        try {

            switch (analysisTask.info.jobType) {
                case MANUAL:
                    addToManualJobQueue(analysisTask);
                    break;
                case SYSTEM:
                    addToSystemQueue(analysisTask);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown job type: " + analysisTask.info.jobType);
            }
        } catch (Throwable t) {
            Env.getCurrentEnv().getAnalysisManager().updateTaskStatus(
                    analysisTask.info, AnalysisState.FAILED, t.getMessage(), System.currentTimeMillis());
        }
    }

    // Make sure invoker of this method is synchronized on object.

    private void addToSystemQueue(BaseAnalysisTask analysisJobInfo) {
        if (systemJobSet.contains(analysisJobInfo)) {
            return;
        }
        systemJobSet.add(analysisJobInfo);
        systemJobQueue.add(analysisJobInfo);
        notify();
    }

    // Make sure invoker of this method is synchronized on object.
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
                return pollAndRemove(manualJobQueue, manualJobSet);
            }
            if (!systemJobQueue.isEmpty()) {
                return pollAndRemove(systemJobQueue, systemJobSet);
            }
            try {
                wait();
            } catch (Exception e) {
                LOG.warn("Thread get interrupted when waiting for pending jobs", e);
                return null;
            }
        }
    }

    // Poll from queue, remove from set. Make sure invoker of this method is synchronized on object.
    private BaseAnalysisTask pollAndRemove(Queue<BaseAnalysisTask> q, Set<BaseAnalysisTask> s) {
        BaseAnalysisTask t = q.poll();
        s.remove(t);
        return t;
    }
}
