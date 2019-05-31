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

package org.apache.doris.load.routineload;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class RoutineLoadScheduler extends Daemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadScheduler.class);
    private static final int DEFAULT_INTERVAL_SECONDS = 10;

    private RoutineLoadManager routineLoadManager;

    @VisibleForTesting
    public RoutineLoadScheduler() {
        super();
        routineLoadManager = Catalog.getInstance().getRoutineLoadManager();
    }

    public RoutineLoadScheduler(RoutineLoadManager routineLoadManager) {
        super("Routine load scheduler", DEFAULT_INTERVAL_SECONDS * 1000);
        this.routineLoadManager = routineLoadManager;
    }

    @Override
    protected void runOneCycle() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of RoutineLoadScheduler", e);
        }
    }

    private void process() throws UserException {
        // update
        routineLoadManager.updateRoutineLoadJob();
        // get need schedule routine jobs
        List<RoutineLoadJob> routineLoadJobList = null;
        try {
            routineLoadJobList = getNeedScheduleRoutineJobs();
        } catch (LoadException e) {
            LOG.warn("failed to get need schedule routine jobs", e);
        }

        LOG.info("there are {} job need schedule", routineLoadJobList.size());
        for (RoutineLoadJob routineLoadJob : routineLoadJobList) {
            RoutineLoadJob.JobState errorJobState = null;
            UserException userException = null;
            try {
                routineLoadJob.prepare();
                // judge nums of tasks more then max concurrent tasks of cluster
                int desiredConcurrentTaskNum = routineLoadJob.calculateCurrentConcurrentTaskNum();
                if (desiredConcurrentTaskNum <= 0) {
                    // the job will be rescheduled later.
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                                     .add("msg", "the current concurrent num is less then or equal to zero, "
                                             + "job will be rescheduled later")
                                     .build());
                    continue;
                }
                int currentTotalTaskNum = routineLoadManager.getSizeOfIdToRoutineLoadTask();
                int desiredTotalTaskNum = desiredConcurrentTaskNum + currentTotalTaskNum;
                if (desiredTotalTaskNum > routineLoadManager.getTotalMaxConcurrentTaskNum()) {
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                                     .add("desired_concurrent_task_num", desiredConcurrentTaskNum)
                                     .add("current_total_task_num", currentTotalTaskNum)
                                     .add("desired_total_task_num", desiredTotalTaskNum)
                                     .add("total_max_task_num", routineLoadManager.getTotalMaxConcurrentTaskNum())
                                     .add("msg", "skip this turn of job scheduler while there are not enough slot in backends")
                                     .build());
                    break;
                }
                // check state and divide job into tasks
                routineLoadJob.divideRoutineLoadJob(desiredConcurrentTaskNum);
            } catch (MetaNotFoundException e) {
                errorJobState = RoutineLoadJob.JobState.CANCELLED;
                userException = e;
            } catch (UserException e) {
                errorJobState = RoutineLoadJob.JobState.PAUSED;
                userException = e;
            }

            if (errorJobState != null) {
                LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                                 .add("current_state", routineLoadJob.getState())
                                 .add("desired_state", errorJobState)
                                 .add("warn_msg", "failed to scheduler job, change job state to desired_state with error reason " + userException.getMessage())
                                 .build(), userException);
                try {
                    routineLoadJob.updateState(errorJobState, userException.getMessage(), false);
                } catch (UserException e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                                     .add("current_state", routineLoadJob.getState())
                                     .add("desired_state", errorJobState)
                                     .add("warn_msg", "failed to change state to desired state")
                                     .build(), e);
                }
            }
        }

        LOG.debug("begin to check timeout tasks");
        // check timeout tasks
        routineLoadManager.processTimeoutTasks();

        LOG.debug("begin to clean old jobs ");
        routineLoadManager.cleanOldRoutineLoadJobs();
    }

    private List<RoutineLoadJob> getNeedScheduleRoutineJobs() throws LoadException {
        return routineLoadManager.getRoutineLoadJobByState(RoutineLoadJob.JobState.NEED_SCHEDULE);
    }


}
