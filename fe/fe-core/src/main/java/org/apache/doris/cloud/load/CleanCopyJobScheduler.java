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

package org.apache.doris.cloud.load;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

public class CleanCopyJobScheduler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(CleanCopyJobScheduler.class);

    private LinkedBlockingQueue<CleanCopyJobTask> needScheduleJobs = Queues.newLinkedBlockingQueue();

    public CleanCopyJobScheduler() {
        super("Clean Copy job scheduler", Config.load_checker_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of CleanCopyJobScheduler with error message {}", e.getMessage(), e);
        }
    }

    private void process() {
        while (true) {
            if (needScheduleJobs.isEmpty()) {
                return;
            }
            CleanCopyJobTask task = needScheduleJobs.poll();
            try {
                task.execute();
            } catch (Exception e) {
                LOG.warn("Failed clean copy job", e);
            }
        }
    }

    public void submitJob(CleanCopyJobTask task) {
        needScheduleJobs.add(task);
    }
}
