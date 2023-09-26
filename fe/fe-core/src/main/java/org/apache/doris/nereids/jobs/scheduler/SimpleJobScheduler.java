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

package org.apache.doris.nereids.jobs.scheduler;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.Job;

import java.util.concurrent.TimeUnit;

/**
 * Single thread, serial scheduler.
 */
public class SimpleJobScheduler implements JobScheduler {
    @Override
    public void executeJobPool(ScheduleContext scheduleContext) {
        JobPool pool = scheduleContext.getJobPool();
        while (!pool.isEmpty()) {
            CascadesContext context = (CascadesContext) scheduleContext;
            if (context.getConnectContext().getSessionVariable().enableNereidsTimeout
                    && context.getStatementContext().getStopwatch().elapsed(TimeUnit.MILLISECONDS) > 5000) {
                throw new RuntimeException("Nereids cost too much time ( > 5s )");
            }
            Job job = pool.pop();
            job.execute();
        }
    }
}
