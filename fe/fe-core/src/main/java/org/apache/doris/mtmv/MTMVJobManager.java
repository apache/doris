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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;
import org.apache.doris.nereids.trees.plans.commands.info.CancelMTMVTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PauseMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ResumeMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * when do some operation, do something about job
 */
public class MTMVJobManager implements MTMVHookService {
    private static final Logger LOG = LogManager.getLogger(MTMVJobManager.class);

    public static final String MTMV_JOB_PREFIX = "inner_mtmv_";

    // if immediate, triggerJob after create MTMT
    @Override
    public void postCreateMTMV(MTMV mtmv) {
        if (!mtmv.getRefreshInfo().getBuildMode().equals(BuildMode.IMMEDIATE)) {
            return;
        }
        MTMVTaskContext mtmvTaskContext = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM, null, true);
        try {
            Env.getCurrentEnv().getJobManager().triggerJob(mtmv.getId(), mtmvTaskContext);
        } catch (JobException e) {
            // should not happen
            LOG.warn("triggerJob failed by mvName: {}", mtmv.getName(), e);
        }
    }

    private JobExecutionConfiguration getJobConfig(MTMV mtmv) {
        JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
        RefreshTrigger refreshTrigger = mtmv.getRefreshInfo().getRefreshTriggerInfo().getRefreshTrigger();
        // if immediate, mtmv will trigger it, not need job manager deal this
        jobExecutionConfiguration.setImmediate(false);
        if (refreshTrigger.equals(RefreshTrigger.SCHEDULE)) {
            setScheduleJobConfig(jobExecutionConfiguration, mtmv);
        } else if (refreshTrigger.equals(RefreshTrigger.MANUAL) || refreshTrigger.equals(RefreshTrigger.COMMIT)) {
            jobExecutionConfiguration.setExecuteType(JobExecuteType.MANUAL);
        }
        return jobExecutionConfiguration;
    }

    private void setScheduleJobConfig(JobExecutionConfiguration jobExecutionConfiguration, MTMV mtmv) {
        jobExecutionConfiguration.setExecuteType(JobExecuteType.RECURRING);
        MTMVRefreshInfo refreshMTMVInfo = mtmv.getRefreshInfo();
        TimerDefinition timerDefinition = new TimerDefinition();
        timerDefinition
                .setInterval(refreshMTMVInfo.getRefreshTriggerInfo().getIntervalTrigger().getInterval());
        timerDefinition
                .setIntervalUnit(refreshMTMVInfo.getRefreshTriggerInfo().getIntervalTrigger().getTimeUnit());
        if (!StringUtils
                .isEmpty(refreshMTMVInfo.getRefreshTriggerInfo().getIntervalTrigger().getStartTime())) {
            timerDefinition.setStartTimeMs(TimeUtils.timeStringToLong(
                    refreshMTMVInfo.getRefreshTriggerInfo().getIntervalTrigger().getStartTime()));
        }
        jobExecutionConfiguration.setTimerDefinition(timerDefinition);
    }

    @Override
    public void registerMTMV(MTMV mtmv, Long dbId) {

    }

    @Override
    public void unregisterMTMV(MTMV mtmv) {

    }

    public void createJob(MTMV mtmv, boolean isReplay) {
        MTMVJob job = new MTMVJob(mtmv.getDatabase().getId(), mtmv.getId());
        // The jobId should remain constant, as it serves as the unique identifier when updating the job.
        job.setJobId(mtmv.getId());
        job.setJobName(mtmv.getJobInfo().getJobName());
        job.setCreateUser(UserIdentity.ADMIN);
        job.setJobStatus(JobStatus.RUNNING);
        job.setJobConfig(getJobConfig(mtmv));
        job.initParams();
        try {
            Env.getCurrentEnv().getJobManager().createJobInternal(job, isReplay);
        } catch (JobException e) {
            // should not happen
            LOG.warn("triggerJob failed by mvName: {}", mtmv.getName(), e);
        }
    }

    public void dropJob(MTMV mtmv, boolean isReplay) {
        MTMVJob job = getJobByMTMV(mtmv);
        try {
            Env.getCurrentEnv().getJobManager().dropJobInternal(job, isReplay);
        } catch (JobException e) {
            // should not happen
            LOG.warn("dropJob failed by mvName: {}", mtmv.getName(), e);
        }
    }

    public void alterJob(MTMV mtmv, boolean isReplay) {
        dropJob(mtmv, isReplay);
        createJob(mtmv, isReplay);
    }

    /**
     * trigger MTMVJob
     *
     * @param info
     * @throws DdlException
     * @throws MetaNotFoundException
     */
    @Override
    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException, JobException {
        MTMVJob job = getJobByTableNameInfo(info.getMvName());
        MTMVTaskContext mtmvTaskContext = new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL, info.getPartitions(),
                info.isComplete());
        Env.getCurrentEnv().getJobManager().triggerJob(job.getJobId(), mtmvTaskContext);
    }

    @Override
    public void refreshComplete(MTMV mtmv, MTMVRelation relation, MTMVTask task) {

    }

    @Override
    public void dropTable(Table table) {

    }

    @Override
    public void alterTable(BaseTableInfo oldTableInfo, Optional<BaseTableInfo> newTableInfo, boolean isReplace) {

    }

    @Override
    public void pauseMTMV(PauseMTMVInfo info) throws MetaNotFoundException, DdlException, JobException {
        MTMVJob job = getJobByTableNameInfo(info.getMvName());
        Env.getCurrentEnv().getJobManager().alterJobStatus(job.getJobId(), JobStatus.PAUSED);
    }

    @Override
    public void resumeMTMV(ResumeMTMVInfo info) throws MetaNotFoundException, DdlException, JobException {
        MTMVJob job = getJobByTableNameInfo(info.getMvName());
        Env.getCurrentEnv().getJobManager().alterJobStatus(job.getJobId(), JobStatus.RUNNING);
    }

    @Override
    public void cancelMTMVTask(CancelMTMVTaskInfo info) throws DdlException, MetaNotFoundException, JobException {
        MTMVJob job = getJobByTableNameInfo(info.getMvName());
        job.cancelTaskById(info.getTaskId());
    }

    public void onCommit(MTMV mtmv) throws DdlException, JobException {
        MTMVJob job = getJobByMTMV(mtmv);
        if (!job.getJobStatus().equals(JobStatus.RUNNING)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("job status of async materialized view: [{}] is: [{}], ignore this event.", mtmv.getName(),
                        job.getJobStatus());
            }
            return;
        }
        MTMVTaskContext mtmvTaskContext = new MTMVTaskContext(MTMVTaskTriggerMode.COMMIT, Lists.newArrayList(),
                false);
        Env.getCurrentEnv().getJobManager().triggerJob(job.getJobId(), mtmvTaskContext);
    }

    private MTMVJob getJobByTableNameInfo(TableNameInfo info) throws DdlException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(info.getDb());
        MTMV mtmv = (MTMV) db.getTableOrMetaException(info.getTbl(), TableType.MATERIALIZED_VIEW);
        return getJobByMTMV(mtmv);
    }

    private MTMVJob getJobByMTMV(MTMV mtmv) {
        return (MTMVJob) Env.getCurrentEnv().getJobManager().getJob(mtmv.getId());
    }

}
