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

package org.apache.doris.task;

import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.ExportFailMsg;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportJob.JobState;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ExportExportingTask extends MasterTask {
    private static final Logger LOG = LogManager.getLogger(ExportExportingTask.class);

    protected final ExportJob job;

    private RuntimeProfile profile = new RuntimeProfile("Export");
    private List<RuntimeProfile> fragmentProfiles = Lists.newArrayList();

    private StmtExecutor stmtExecutor;

    public ExportExportingTask(ExportJob job) {
        this.job = job;
        this.signature = job.getId();
    }

    public StmtExecutor getStmtExecutor() {
        return stmtExecutor;
    }

    @Override
    protected void exec() {
        if (job.getState() == JobState.IN_QUEUE) {
            handleInQueueState();
        }

        if (job.getState() != ExportJob.JobState.EXPORTING) {
            return;
        }
        LOG.info("begin execute export job in exporting state. job: {}", job);

        synchronized (job) {
            if (job.getDoExportingThread() != null) {
                LOG.warn("export task is already being executed.");
                return;
            }
            job.setDoExportingThread(Thread.currentThread());
        }

        List<QueryStmt> selectStmtList = job.getSelectStmtList();
        boolean isFailed = false;
        ExportFailMsg errorMsg = null;
        int completeTaskNum = 0;
        List<ExportJob.OutfileInfo> outfileInfoList = Lists.newArrayList();
        // begin exporting
        for (int i = 0; i < selectStmtList.size(); ++i) {
            // maybe user cancelled this job
            if (job.getState() != JobState.EXPORTING) {
                isFailed = true;
                break;
            }
            try (AutoCloseConnectContext r = buildConnectContext()) {
                this.stmtExecutor = new StmtExecutor(r.connectContext, selectStmtList.get(i));
                this.stmtExecutor.execute();
                if (r.connectContext.getState().getStateType() == MysqlStateType.ERR) {
                    errorMsg = new ExportFailMsg(ExportFailMsg.CancelType.RUN_FAIL,
                            r.connectContext.getState().getErrorMessage());
                    isFailed = true;
                    break;
                }
                ExportJob.OutfileInfo outfileInfo = getOutFileInfo(r.connectContext.getResultAttachedInfo());
                outfileInfoList.add(outfileInfo);
                ++completeTaskNum;
            } catch (Exception e) {
                errorMsg = new ExportFailMsg(ExportFailMsg.CancelType.RUN_FAIL, e.getMessage());
                isFailed = true;
                break;
            } finally {
                this.stmtExecutor.addProfileToSpan();
            }
        }

        int progress = completeTaskNum * 100 / selectStmtList.size();
        if (progress >= 100) {
            progress = 99;
        }
        job.setProgress(progress);
        LOG.info("Exporting task progress is {}%, export job: {}", progress, job.getId());

        if (isFailed) {
            registerProfile();
            job.cancel(errorMsg.getCancelType(), errorMsg.getMsg());
            LOG.warn("Exporting task failed because Exception: {}", errorMsg.getMsg());
            return;
        }

        registerProfile();
        if (job.finish(outfileInfoList)) {
            LOG.info("export job success. job: {}", job);
            // TODO(ftw): when we implement exporting tablet one by one, we should release snapshot here
            // release snapshot
            // Status releaseSnapshotStatus = job.releaseSnapshotPaths();
            // if (!releaseSnapshotStatus.ok()) {
            //     // even if release snapshot failed, do not cancel this job.
            //     // snapshot will be removed by GC thread on BE, finally.
            //     LOG.warn("failed to release snapshot for export job: {}. err: {}", job.getId(),
            //             releaseSnapshotStatus.getErrorMsg());
            // }
        }

        synchronized (this) {
            job.setDoExportingThread(null);
        }
    }

    private AutoCloseConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setSessionVariable(job.getSessionVariables());
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setDatabase(job.getTableName().getDb());
        connectContext.setQualifiedUser(job.getQualifiedUser());
        connectContext.setCurrentUserIdentity(job.getUserIdentity());
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        connectContext.setQueryId(queryId);
        connectContext.setStartTime();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        return new AutoCloseConnectContext(connectContext);
    }

    private ExportJob.OutfileInfo getOutFileInfo(Map<String, String> resultAttachedInfo) {
        ExportJob.OutfileInfo outfileInfo = new ExportJob.OutfileInfo();
        outfileInfo.setFileNumber(resultAttachedInfo.get(OutFileClause.FILE_NUMBER));
        outfileInfo.setTotalRows(resultAttachedInfo.get(OutFileClause.TOTAL_ROWS));
        outfileInfo.setFileSize(resultAttachedInfo.get(OutFileClause.FILE_SIZE) + "bytes");
        outfileInfo.setUrl(resultAttachedInfo.get(OutFileClause.URL));
        return outfileInfo;
    }

    private void initProfile() {
        profile = new RuntimeProfile("ExportJob");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.JOB_ID, String.valueOf(job.getId()));
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, job.getQueryId());
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(job.getStartTimeMs()));

        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - job.getStartTimeMs();
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(currentTimestamp));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Export");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, job.getState().toString());
        summaryProfile.addInfoString(ProfileManager.DORIS_VERSION, Version.DORIS_BUILD_VERSION);
        summaryProfile.addInfoString(ProfileManager.USER, job.getQualifiedUser());
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, String.valueOf(job.getDbId()));
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, job.getSql());
        profile.addChild(summaryProfile);
    }

    private void registerProfile() {
        if (!job.getEnableProfile()) {
            return;
        }
        initProfile();
        for (RuntimeProfile p : fragmentProfiles) {
            profile.addChild(p);
        }
        ProfileManager.getInstance().pushProfile(profile);
    }

    private void handleInQueueState() {
        long dbId = job.getDbId();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            job.cancel(ExportFailMsg.CancelType.RUN_FAIL, "database does not exist");
            return;
        }

        // TODO(ftw): when we implement exporting tablet one by one, we should makeSnapshots here
        // Status snapshotStatus = job.makeSnapshots();
        // if (!snapshotStatus.ok()) {
        //     job.cancel(ExportFailMsg.CancelType.RUN_FAIL, snapshotStatus.getErrorMsg());
        //     return;
        // }

        if (job.updateState(ExportJob.JobState.EXPORTING)) {
            LOG.info("Exchange pending status to exporting status success. job: {}", job);
        }
    }
}
