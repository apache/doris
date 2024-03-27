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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class RoutineLoadScheduler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadScheduler.class);

    private RoutineLoadManager routineLoadManager;

    @VisibleForTesting
    public RoutineLoadScheduler() {
        super();
        routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();
    }

    public RoutineLoadScheduler(RoutineLoadManager routineLoadManager) {
        super("Routine load scheduler", FeConstants.default_scheduler_interval_millisecond);
        this.routineLoadManager = routineLoadManager;
    }

    @Override
    protected void runAfterCatalogReady() {
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

        if (!routineLoadJobList.isEmpty()) {
            LOG.info("there are {} job need schedule", routineLoadJobList.size());
        }

        for (RoutineLoadJob routineLoadJob : routineLoadJobList) {
            RoutineLoadJob.JobState errorJobState = null;
            UserException userException = null;
            try {
                if (Config.isCloudMode()) {
                    routineLoadJob.updateCloudProgress();
                }

                routineLoadJob.prepare();
                // judge nums of tasks more than max concurrent tasks of cluster
                int desiredConcurrentTaskNum = routineLoadJob.calculateCurrentConcurrentTaskNum();
                if (desiredConcurrentTaskNum <= 0) {
                    // the job will be rescheduled later.
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("msg", "the current concurrent num is less than or equal to zero, "
                                    + "job will be rescheduled later")
                            .build());
                    continue;
                }
                // check state and divide job into tasks
                routineLoadJob.divideRoutineLoadJob(desiredConcurrentTaskNum);
            } catch (MetaNotFoundException e) {
                errorJobState = RoutineLoadJob.JobState.CANCELLED;
                userException = e;
                LOG.warn(userException.getMessage());
            } catch (UserException e) {
                errorJobState = RoutineLoadJob.JobState.PAUSED;
                userException = e;
                LOG.warn(userException.getMessage());
            }

            if (errorJobState != null) {
                LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                        .add("current_state", routineLoadJob.getState())
                        .add("desired_state", errorJobState)
                        .add("warn_msg", "failed to scheduler job,"
                                + " change job state to desired_state with error reason " + userException.getMessage())
                        .build(), userException);
                try {
                    ErrorReason reason = new ErrorReason(userException.getErrorCode(), userException.getMessage());
                    routineLoadJob.updateState(errorJobState, reason, false);
                } catch (UserException e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("current_state", routineLoadJob.getState())
                            .add("desired_state", errorJobState)
                            .add("warn_msg", "failed to change state to desired state")
                            .build(), e);
                }
            }
        }

        // check timeout tasks
        routineLoadManager.processTimeoutTasks();

        routineLoadManager.cleanOldRoutineLoadJobs();

        routineLoadManager.cleanOverLimitRoutineLoadJobs();
    }

    private List<RoutineLoadJob> getNeedScheduleRoutineJobs() throws LoadException {
        return routineLoadManager.getRoutineLoadJobByState(Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE));
    }

}
