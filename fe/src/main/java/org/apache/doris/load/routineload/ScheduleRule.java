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
import org.apache.doris.common.Config;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.system.SystemInfoService;

/**
 * ScheduleRule: RoutineLoad PAUSED -> NEED_SCHEDULE
 */
public class ScheduleRule {

    private static int deadBeCount(String clusterName) {
        SystemInfoService systemInfoService = Catalog.getCurrentSystemInfo();
        int total = systemInfoService.getClusterBackendIds(clusterName, false).size();
        int alive = systemInfoService.getClusterBackendIds(clusterName, true).size();
        return total - alive;
    }

    /**
     * check if RoutineLoadJob is auto schedule
     * @param jobRoutine
     * @return
     */
    public static boolean isNeedAutoSchedule(RoutineLoadJob jobRoutine) {
        if (jobRoutine.state != RoutineLoadJob.JobState.PAUSED) {
            return false;
        }
        if (jobRoutine.autoResumeLock) {//only manual resume for unlock
            return false;
        }

        /*
         * Handle all backends are down.
         */
        if (jobRoutine.pauseReason != null && jobRoutine.pauseReason.getCode() == InternalErrorCode.REPLICA_FEW_ERR) {
            int dead = deadBeCount(jobRoutine.clusterName);
            if (dead > Config.max_tolerable_backend_down_num) {
                return false;
            }
            if (jobRoutine.firstResumeTimestamp == 0) {//the first resume
                jobRoutine.firstResumeTimestamp = System.currentTimeMillis();
                jobRoutine.autoResumeCount = 1;
                return true;
            } else {
                long current = System.currentTimeMillis();
                if (current - jobRoutine.firstResumeTimestamp < Config.period_of_auto_resume_min * 60000) {
                    if (jobRoutine.autoResumeCount >= 3) {
                        jobRoutine.autoResumeLock = true;// locked Auto Resume RoutineLoadJob
                        return false;
                    }
                    jobRoutine.autoResumeCount++;
                    return true;
                } else {
                    /**
                     * for example：
                     *       the first resume time at 10:01
                     *       the second resume time at 10:03
                     *       the third resume time at 10:20
                     *           --> we must be reset counter because a new period for AutoResume RoutineLoadJob
                     */
                    jobRoutine.firstResumeTimestamp = current;
                    jobRoutine.autoResumeCount = 1;
                    return true;
                }
            }
        }
        return false;
    }
}
