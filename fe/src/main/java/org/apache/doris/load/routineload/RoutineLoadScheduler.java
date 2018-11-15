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
import org.apache.doris.common.util.Daemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class RoutineLoadScheduler extends Daemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadScheduler.class);

    private RoutineLoad routineLoad = Catalog.getInstance().getRoutineLoadInstance();

    @Override
    protected void runOneCycle() {
        // get need scheduler routine jobs
        List<RoutineLoadJob> routineLoadJobList = null;
        try {
            routineLoadJobList = getNeedSchedulerRoutineJobs();
        } catch (LoadException e) {
            LOG.error("failed to get need scheduler routine jobs");
        }

        LOG.debug("there are {} job need scheduler", routineLoadJobList.size());
        for (RoutineLoadJob routineLoadJob : routineLoadJobList) {
            // judge nums of tasks more then max concurrent tasks of cluster
            List<RoutineLoadTask> routineLoadTaskList = null;
            try {
                routineLoadJob.writeLock();

                if (routineLoadJob.getState() == RoutineLoadJob.JobState.NEED_SCHEDULER) {
                    int currentConcurrentTaskNum = routineLoadJob.calculateCurrentConcurrentTaskNum();
                    int totalTaskNum = currentConcurrentTaskNum + routineLoad.getIdToRoutineLoadTask().size();
                    if (totalTaskNum > routineLoad.getTotalMaxConcurrentTaskNum()) {
                        LOG.info("job {} concurrent task num {}, current total task num {}. "
                                        + "desired total task num {} more then total max task num {}, "
                                        + "skip this turn of scheduler",
                                routineLoadJob.getId(), currentConcurrentTaskNum,
                                routineLoad.getIdToRoutineLoadTask().size(),
                                totalTaskNum, routineLoad.getTotalMaxConcurrentTaskNum());
                        break;
                    }
                    // divide job into tasks
                    routineLoadTaskList = routineLoadJob.divideRoutineLoadJob(currentConcurrentTaskNum);

                    // update tasks meta
                    routineLoad.addRoutineLoadTasks(routineLoadTaskList);
                    routineLoad.addNeedSchedulerRoutineLoadTasks(routineLoadTaskList);

                    // change job state to running
                    routineLoad.updateRoutineLoadJobState(routineLoadJob, RoutineLoadJob.JobState.RUNNING);
                }
            } catch (MetaNotFoundException e) {
                routineLoad.updateRoutineLoadJobStateNoValid(routineLoadJob, RoutineLoadJob.JobState.CANCELLED);
            } catch (LoadException e) {
                LOG.error("failed to scheduler job {} with error massage {}", routineLoadJob.getId(),
                        e.getMessage(), e);
                routineLoad.removeRoutineLoadTasks(routineLoadTaskList);
            } finally {
                routineLoadJob.writeUnlock();
            }
        }

    }

    private List<RoutineLoadJob> getNeedSchedulerRoutineJobs() throws LoadException {
        return routineLoad.getRoutineLoadJobByState(RoutineLoadJob.JobState.NEED_SCHEDULER);
    }


}
