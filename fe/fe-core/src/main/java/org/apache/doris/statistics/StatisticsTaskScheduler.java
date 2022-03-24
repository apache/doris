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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.clearspring.analytics.util.Lists;

/*
Schedule statistics task
 */
public class StatisticsTaskScheduler extends MasterDaemon {
    private final static Logger LOG = LogManager.getLogger(StatisticsTaskScheduler.class);

    private Queue<StatisticsTask> queue = Queues.newLinkedBlockingQueue();

    public StatisticsTaskScheduler() {
        super("Statistics task scheduler", 0);
    }

    @Override
    protected void runAfterCatalogReady() {
        // TODO
        // step1: task n concurrent tasks from the queue
        List<StatisticsTask> tasks = peek();
        // step2: execute tasks
        ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
        List<Future<StatisticsTaskResult>> taskResultList = null;
        try {
            taskResultList = executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute this turn of statistics tasks", e);
        }
        // step3: update job and statistics
        handleTaskResult(taskResultList);
        // step4: remove task from queue
        remove(tasks.size());

    }

    public void addTasks(List<StatisticsTask> statisticsTaskList) {
        queue.addAll(statisticsTaskList);
    }

    private List<StatisticsTask> peek() {
        List<StatisticsTask> tasks = Lists.newArrayList();
        int i = Config.cbo_concurrency_statistics_task_num;
        while (i > 0) {
            StatisticsTask task = queue.peek();
            if (task == null) {
                break;
            }
            tasks.add(task);
            i--;
        }
        return tasks;
    }

    private void remove(int size) {
        // TODO
    }

    private void handleTaskResult(List<Future<StatisticsTaskResult>> taskResultLists) {
        // TODO
    }
}
