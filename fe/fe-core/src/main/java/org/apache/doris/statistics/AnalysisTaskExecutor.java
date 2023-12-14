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
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.ThreadPoolManager.BlockedPolicy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AnalysisTaskExecutor {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskExecutor.class);

    protected final ThreadPoolExecutor executors;

    public AnalysisTaskExecutor(int simultaneouslyRunningTaskNum) {
        this(simultaneouslyRunningTaskNum, Integer.MAX_VALUE);
    }

    public AnalysisTaskExecutor(int simultaneouslyRunningTaskNum, int taskQueueSize) {
        if (!Env.isCheckpointThread()) {
            executors = ThreadPoolManager.newDaemonThreadPool(
                    simultaneouslyRunningTaskNum,
                    simultaneouslyRunningTaskNum, 0,
                    TimeUnit.DAYS, new LinkedBlockingQueue<>(taskQueueSize),
                    new BlockedPolicy("Analysis Job Executor", Integer.MAX_VALUE),
                    "Analysis Job Executor", true);
        } else {
            executors = null;
        }
    }

    public void submitTask(BaseAnalysisTask task) {
        AnalysisTaskWrapper taskWrapper = new AnalysisTaskWrapper(task);
        executors.submit(taskWrapper);
    }
}
