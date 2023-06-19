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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.MVRefreshInfo.BuildMode;
import org.apache.doris.analysis.MVRefreshInfo.RefreshMethod;
import org.apache.doris.analysis.MVRefreshInfo.RefreshTrigger;
import org.apache.doris.analysis.MVRefreshIntervalTriggerInfo;
import org.apache.doris.analysis.MVRefreshTriggerInfo;
import org.apache.doris.catalog.MaterializedView;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mtmv.MTMVUtils.TriggerMode;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVJob.JobSchedule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MTMVJobFactory {
    private static final Logger LOG = LogManager.getLogger(MTMVTaskProcessor.class);

    public static boolean isGenerateJob(MaterializedView materializedView) {
        boolean completeRefresh = materializedView.getRefreshInfo().getRefreshMethod() == RefreshMethod.COMPLETE;
        BuildMode buildMode = materializedView.getBuildMode();
        MVRefreshTriggerInfo triggerInfo = materializedView.getRefreshInfo().getTriggerInfo();
        //can not generate a job when creating a temp materialized view.
        if (materializedView.getName().startsWith(FeConstants.TEMP_MATERIZLIZE_DVIEW_PREFIX)) {
            return false;
        }
        if (buildMode == BuildMode.IMMEDIATE) {
            return completeRefresh;
        } else {
            return completeRefresh && triggerInfo != null && triggerInfo.getRefreshTrigger() == RefreshTrigger.INTERVAL;
        }
    }

    public static List<MTMVJob> buildJob(MaterializedView materializedView, String dbName) {
        List<MTMVJob> jobs = new ArrayList<>();
        MVRefreshTriggerInfo triggerInfo = materializedView.getRefreshInfo().getTriggerInfo();
        boolean isRunPeriodJobImmediate = false;
        if (triggerInfo != null && triggerInfo.getRefreshTrigger() == RefreshTrigger.INTERVAL) {
            MTMVJob job = genPeriodicalJob(materializedView, dbName);
            isRunPeriodJobImmediate = MTMVUtils.getDelaySeconds(job) == 0;
            jobs.add(job);
        }

        // if the PeriodicalJob run immediate since an early start time, don't run the immediate build.
        if (!isRunPeriodJobImmediate && materializedView.getBuildMode() == BuildMode.IMMEDIATE) {
            jobs.add(genOnceJob(materializedView, dbName));
        }
        return jobs;
    }

    private static MTMVJob genPeriodicalJob(MaterializedView materializedView, String dbName) {
        String uid = UUID.randomUUID().toString();
        MTMVJob job = new MTMVJob(materializedView.getName() + "_" + uid);
        job.setTriggerMode(TriggerMode.PERIODICAL);
        job.setSchedule(genJobSchedule(materializedView));
        job.setDBName(dbName);
        job.setMVName(materializedView.getName());
        job.setQuery(materializedView.getQuery());
        job.setCreateTime(MTMVUtils.getNowTimeStamp());
        return job;
    }

    public static MTMVJob genOnceJob(MaterializedView materializedView, String dbName) {
        String uid = UUID.randomUUID().toString();
        MTMVJob job = new MTMVJob(materializedView.getName() + "_" + uid);
        job.setTriggerMode(TriggerMode.ONCE);
        job.setDBName(dbName);
        job.setMVName(materializedView.getName());
        job.setQuery(materializedView.getQuery());
        job.setCreateTime(MTMVUtils.getNowTimeStamp());
        return job;
    }

    private static JobSchedule genJobSchedule(MaterializedView materializedView) {
        MVRefreshIntervalTriggerInfo info = materializedView.getRefreshInfo().getTriggerInfo().getIntervalTrigger();
        long startTime;
        try {
            LocalDateTime dateTime = LocalDateTime.parse(info.getStartTime(), TimeUtils.DATETIME_FORMAT);
            startTime = dateTime.toEpochSecond(TimeUtils.TIME_ZONE.getRules().getOffset(dateTime));
        } catch (DateTimeParseException e) {
            throw new RuntimeException(e);
        }

        return new JobSchedule(startTime, info.getInterval(), MTMVUtils.getTimeUint(info.getTimeUnit()));
    }
}
