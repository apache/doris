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
import org.apache.doris.catalog.MaterializedView;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshEnum.BuildMode;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshEnum.RefreshTrigger;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshSchedule;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.job.Job;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.UUID;

public class MTMVJobManager implements MTMVHookService {
    public final static String MTMV_JOB_PREFIX = "mtmv_";

    @Override
    public void createMTMV(MaterializedView materializedView) throws DdlException {
        if (materializedView.getRefreshInfo().getRefreshTriggerInfo().getRefreshTrigger()
                .equals(RefreshTrigger.SCHEDULE)) {
            createCycleJob(materializedView);
        } else if (materializedView.getRefreshInfo().getBuildMode().equals(BuildMode.IMMEDIATE)) {
            createOnceJob(materializedView);
        }

    }

    private void createOnceJob(MaterializedView materializedView) throws DdlException {
        String uid = UUID.randomUUID().toString().replace("-", "_");
        Job job = new Job();
        job.setCycleJob(false);
        job.setBaseName(materializedView.getName());
        job.setDbName(ConnectContext.get().getDatabase());
        job.setJobName(materializedView.getName() + "_" + uid);
        job.setExecutor(generateJobExecutor(materializedView));
        job.setImmediatelyStart(true);
        job.setUser(ConnectContext.get().getQualifiedUser());
        job.setComment("mvName:" + materializedView.getName());
        job.setJobCategory(JobCategory.MTMV);
        Env.getCurrentEnv().getJobRegister().registerJob(job);
    }

    private void createCycleJob(MaterializedView materializedView) throws DdlException {
        String uid = UUID.randomUUID().toString().replace("-", "_");
        Job job = new Job();
        job.setCycleJob(true);
        job.setBaseName(materializedView.getName());
        job.setDbName(ConnectContext.get().getDatabase());
        job.setJobName(materializedView.getName() + "_" + uid);
        job.setExecutor(generateJobExecutor(materializedView));
        MTMVRefreshSchedule intervalTrigger = materializedView.getRefreshInfo().getRefreshTriggerInfo()
                .getIntervalTrigger();
        job.setIntervalUnit(intervalTrigger.getTimeUnit());
        job.setOriginInterval(intervalTrigger.getInterval());
        if (materializedView.getRefreshInfo().getBuildMode().equals(BuildMode.IMMEDIATE)) {
            job.setImmediatelyStart(true);
        } else if (materializedView.getRefreshInfo().getBuildMode().equals(BuildMode.DEFERRED) && !StringUtils
                .isEmpty(intervalTrigger.getStartTime())) {
            job.setStartTimeMs(TimeUtils.timeStringToLong(intervalTrigger.getStartTime()));
        }
        job.setUser(ConnectContext.get().getQualifiedUser());
        job.setComment("mvName:" + materializedView.getName());
        job.setJobCategory(JobCategory.MTMV);
        Env.getCurrentEnv().getJobRegister().registerJob(job);
    }

    @Override
    public void dropMTMV(MaterializedView table) {
        List<Job> jobs = Env.getCurrentEnv().getJobRegister()
                .getJobs(table.getQualifiedDbName(), null, JobCategory.MTMV, null);
        for (Job job : jobs) {
            // TODO: 2023/9/12 JobRegister should provide interface filter by baseName
            if (table.getName().equals(job.getBaseName())) {
                Env.getCurrentEnv().getJobRegister().stopJob(job.getJobId());
            }
        }
    }

    @Override
    public void registerMTMV(MaterializedView materializedView) {

    }

    @Override
    public void deregisterMTMV(MaterializedView materializedView) {

    }

    @Override
    public void alterMTMV(MaterializedView materializedView, AlterMTMV alterMTMV) throws DdlException {
        if (alterMTMV.isNeedRebuildJob()) {
            dropMTMV(materializedView);
            createMTMV(materializedView);
        }
    }

    @Override
    public void refreshMTMV(RefreshMTMVInfo info) throws DdlException, MetaNotFoundException {
        TableNameInfo mvName = info.getMvName();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(mvName.getDb());
        MaterializedView mv = (MaterializedView) db
                .getTableOrMetaException(mvName.getTbl(), TableType.MATERIALIZED_VIEW);
        createOnceJob(mv);
    }

    private static String generateSql(MaterializedView materializedView) {
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

    private MTMVJobExecutor generateJobExecutor(MaterializedView materializedView) {
        return new MTMVJobExecutor(materializedView.getQualifiedDbName(), materializedView.getName(),
                generateSql(materializedView));
    }

}
