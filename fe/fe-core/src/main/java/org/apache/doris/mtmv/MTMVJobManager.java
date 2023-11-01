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
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.constants.JobType;
import org.apache.doris.scheduler.job.Job;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class MTMVJobManager implements MTMVHookService {
    public static final String MTMV_JOB_PREFIX = "mtmv_";

    @Override
    public void createMTMV(MTMV materializedView) throws DdlException {
        if (materializedView.getRefreshInfo().getRefreshTriggerInfo().getRefreshTrigger()
                .equals(RefreshTrigger.SCHEDULE)) {
            createCycleJob(materializedView);
        } else if (materializedView.getRefreshInfo().getBuildMode().equals(BuildMode.IMMEDIATE)) {
            createManualJob(materializedView);
        }

    }

    private void createManualJob(MTMV mtmv) throws DdlException {
        Job job = new Job();
        job.setJobType(JobType.MANUAL);
        job.setBaseName(mtmv.getName());
        job.setDbName(ConnectContext.get().getDatabase());
        job.setJobName(mtmv.getJobInfo().getJobName());
        job.setExecutor(generateJobExecutor(mtmv));
        job.setImmediatelyStart(true);
        job.setUser(ConnectContext.get().getQualifiedUser());
        job.setComment("mvName:" + mtmv.getName());
        job.setJobCategory(JobCategory.MTMV);
        Env.getCurrentEnv().getJobRegister().registerJob(job);
    }

    private void createCycleJob(MTMV mtmv) throws DdlException {
        Job job = new Job();
        job.setJobType(JobType.RECURRING);
        job.setBaseName(mtmv.getName());
        job.setDbName(ConnectContext.get().getDatabase());
        job.setJobName(mtmv.getJobInfo().getJobName());
        job.setExecutor(generateJobExecutor(mtmv));
        MTMVRefreshSchedule intervalTrigger = mtmv.getRefreshInfo().getRefreshTriggerInfo()
                .getIntervalTrigger();
        job.setIntervalUnit(intervalTrigger.getTimeUnit());
        job.setOriginInterval(intervalTrigger.getInterval());
        if (mtmv.getRefreshInfo().getBuildMode().equals(BuildMode.IMMEDIATE)) {
            job.setImmediatelyStart(true);
        } else if (mtmv.getRefreshInfo().getBuildMode().equals(BuildMode.DEFERRED) && !StringUtils
                .isEmpty(intervalTrigger.getStartTime())) {
            job.setStartTimeMs(TimeUtils.timeStringToLong(intervalTrigger.getStartTime()));
        }
        job.setUser(ConnectContext.get().getQualifiedUser());
        job.setComment("mvName:" + mtmv.getName());
        job.setJobCategory(JobCategory.MTMV);
        Env.getCurrentEnv().getJobRegister().registerJob(job);
    }

    @Override
    public void dropMTMV(MTMV mtmv) throws DdlException {
        Env.getCurrentEnv().getJobRegister().stopJob(null, mtmv.getJobInfo().getJobName(), null);
    }

    @Override
    public void registerMTMV(MTMV materializedView) {

    }

    @Override
    public void deregisterMTMV(MTMV materializedView) {

    }

    @Override
    public void alterMTMV(MTMV materializedView, AlterMTMV alterMTMV) throws DdlException {
        if (alterMTMV.isNeedRebuildJob()) {
            dropMTMV(materializedView);
            createMTMV(materializedView);
        }
    }

    @Override
    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(info.getMvName().getDb());
        MTMV mtmv = (MTMV) db.getTableOrMetaException(info.getMvName().getTbl(), TableType.MATERIALIZED_VIEW);
        List<Job> jobs = Env.getCurrentEnv().getJobRegister()
                .getJobs(null, mtmv.getJobInfo().getJobName(), JobCategory.MTMV, null);
        if (CollectionUtils.isEmpty(jobs) || jobs.size() != 1) {
            throw new DdlException("jobs not normal");
        }
        Env.getCurrentEnv().getJobRegister().immediateExecuteTask(jobs.get(0).getJobId(), new MTMVTaskParams());
    }

    private static String generateSql(MTMV materializedView) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT OVERWRITE TABLE ");
        builder.append(materializedView.getDatabase().getCatalog().getName());
        builder.append(".");
        builder.append(ClusterNamespace.getNameFromFullName(materializedView.getQualifiedDbName()));
        builder.append(".");
        builder.append(materializedView.getName());
        builder.append(" ");
        builder.append(materializedView.getQuerySql());
        return builder.toString();
    }

    private MTMVJobExecutor generateJobExecutor(MTMV materializedView) {
        return new MTMVJobExecutor(materializedView.getQualifiedDbName(), materializedView.getName(),
                generateSql(materializedView));
    }

}
