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
import org.apache.doris.job.common.JobType;
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
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * when do some operation, do something about job
 */
public class MTMVJobManager implements MTMVHookService {
    private static final Logger LOG = LogManager.getLogger(MTMVJobManager.class);

    public static final String MTMV_JOB_PREFIX = "inner_mtmv_";

    /**
     * create MTMVJob
     *
     * @param mtmv
     * @throws DdlException
     */
    @Override
    public void createMTMV(MTMV mtmv) throws DdlException {
        MTMVJob job = new MTMVJob(mtmv.getDatabase().getId(), mtmv.getId());
        job.setJobId(Env.getCurrentEnv().getNextId());
        job.setJobName(mtmv.getJobInfo().getJobName());
        job.setCreateUser(ConnectContext.get().getCurrentUserIdentity());
        job.setJobStatus(JobStatus.RUNNING);
        job.setJobConfig(getJobConfig(mtmv));
        try {
            Env.getCurrentEnv().getJobManager().registerJob(job);
        } catch (JobException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    private JobExecutionConfiguration getJobConfig(MTMV mtmv) {
        JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
        RefreshTrigger refreshTrigger = mtmv.getRefreshInfo().getRefreshTriggerInfo().getRefreshTrigger();
        if (refreshTrigger.equals(RefreshTrigger.SCHEDULE)) {
            setScheduleJobConfig(jobExecutionConfiguration, mtmv);
        } else if (refreshTrigger.equals(RefreshTrigger.MANUAL) || refreshTrigger.equals(RefreshTrigger.COMMIT)) {
            setManualJobConfig(jobExecutionConfiguration, mtmv);
        }
        return jobExecutionConfiguration;
    }

    private void setManualJobConfig(JobExecutionConfiguration jobExecutionConfiguration, MTMV mtmv) {
        jobExecutionConfiguration.setExecuteType(JobExecuteType.MANUAL);
        if (mtmv.getRefreshInfo().getBuildMode().equals(BuildMode.IMMEDIATE)) {
            jobExecutionConfiguration.setImmediate(true);
        } else {
            jobExecutionConfiguration.setImmediate(false);
        }
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
        if (refreshMTMVInfo.getBuildMode().equals(BuildMode.IMMEDIATE)) {
            jobExecutionConfiguration.setImmediate(true);
        }
        jobExecutionConfiguration.setTimerDefinition(timerDefinition);
    }

    /**
     * drop MTMVJob
     *
     * @param mtmv
     * @throws DdlException
     */
    @Override
    public void dropMTMV(MTMV mtmv) throws DdlException {
        try {
            Env.getCurrentEnv().getJobManager()
                    .unregisterJob(mtmv.getJobInfo().getJobName(), true);
        } catch (JobException e) {
            LOG.warn("drop mtmv job failed, mtmvName: {}", mtmv.getName(), e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public void registerMTMV(MTMV mtmv, Long dbId) {

    }

    @Override
    public void deregisterMTMV(MTMV mtmv) {

    }

    /**
     * drop MTMVJob and then create MTMVJob
     *
     * @param mtmv
     * @param alterMTMV
     * @throws DdlException
     */
    @Override
    public void alterMTMV(MTMV mtmv, AlterMTMV alterMTMV) throws DdlException {
        if (alterMTMV.isNeedRebuildJob()) {
            dropMTMV(mtmv);
            createMTMV(mtmv);
        }
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
    public void alterTable(Table table, String oldTableName) {

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

    private MTMVJob getJobByMTMV(MTMV mtmv) throws DdlException {
        List<MTMVJob> jobs = Env.getCurrentEnv().getJobManager()
                .queryJobs(JobType.MV, mtmv.getJobInfo().getJobName());
        if (CollectionUtils.isEmpty(jobs) || jobs.size() != 1) {
            throw new DdlException("jobs not normal,should have one job,but job num is: " + jobs.size());
        }
        return jobs.get(0);
    }

}
