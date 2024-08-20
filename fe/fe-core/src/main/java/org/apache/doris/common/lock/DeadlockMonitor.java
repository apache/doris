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

package org.apache.doris.common.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A utility class for monitoring and reporting deadlocks in a Java application.
 * <p>
 * This class uses the Java Management API to periodically check for deadlocked threads
 * and logs detailed information about any detected deadlocks. It can be configured to
 * run at a fixed interval.
 * </p>
 */
public class DeadlockMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(DeadlockMonitor.class);
    private final ThreadMXBean threadMXBean;
    private final ScheduledExecutorService scheduler;

    public DeadlockMonitor() {
        this.threadMXBean = ManagementFactory.getThreadMXBean();
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    /**
     * Starts monitoring for deadlocks at a fixed rate.
     *
     * @param period the period between successive executions
     * @param unit   the time unit of the period parameter
     */
    public void startMonitoring(long period, TimeUnit unit) {
        scheduler.scheduleAtFixedRate(this::detectAndReportDeadlocks, 5, period, unit);
    }

    /**
     * Detects and reports deadlocks if any are found.
     */
    public void detectAndReportDeadlocks() {
        // Get IDs of threads that are deadlocked
        long[] deadlockedThreadIds = threadMXBean.findDeadlockedThreads();

        // Check if there are no deadlocked threads
        if (deadlockedThreadIds == null || deadlockedThreadIds.length == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No deadlocks detected.");
            }
            return;
        }

        // Get information about deadlocked threads
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(deadlockedThreadIds, true, true);
        String deadlockReportString = Arrays.toString(threadInfos).replace("\n", "\\n");
        // Log the deadlock report
        LOG.warn("Deadlocks detected {}", deadlockReportString);
    }

}
