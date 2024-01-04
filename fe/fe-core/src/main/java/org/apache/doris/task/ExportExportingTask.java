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
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.load.ExportFailMsg;
import org.apache.doris.load.ExportFailMsg.CancelType;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportJob.JobState;
import org.apache.doris.load.ExportJob.OutfileInfo;
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
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class ExportExportingTask extends MasterTask {
    private static final Logger LOG = LogManager.getLogger(ExportExportingTask.class);

    protected final ExportJob job;

    ThreadPoolExecutor exportExecPool = ThreadPoolManager.newDaemonCacheThreadPool(
            Config.maximum_parallelism_of_export_job, "exporting-pool-", false);

    public ExportExportingTask(ExportJob job) {
        this.job = job;
        this.signature = job.getId();
    }

    private class ExportResult {
        private boolean isFailed;

        private ExportFailMsg failMsg;

        private ExportJob.OutfileInfo outfileInfo;

        public ExportResult(boolean isFailed, ExportFailMsg failMsg, ExportJob.OutfileInfo outfileInfo) {
            this.isFailed = isFailed;
            this.failMsg = failMsg;
            this.outfileInfo = outfileInfo;
        }


        public boolean isFailed() {
            return isFailed;
        }

        public ExportFailMsg getFailMsg() {
            return failMsg;
        }

        public OutfileInfo getOutfileInfo() {
            return outfileInfo;
        }
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

        List<SelectStmt> selectStmtList = job.getSelectStmtList();
        int completeTaskNum = 0;
        List<ExportJob.OutfileInfo> outfileInfoList = Lists.newArrayList();

        int parallelNum = selectStmtList.size();
        CompletionService<ExportResult> completionService = new ExecutorCompletionService<>(exportExecPool);

        // begin exporting
        for (int i = 0; i < parallelNum; ++i) {
            final int idx = i;
            completionService.submit(() -> {
                // maybe user cancelled this job
                if (job.getState() != JobState.EXPORTING) {
                    return new ExportResult(true, null, null);
                }
                try {
                    Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException(
                            job.getTableName().getDb());
                    OlapTable table = db.getOlapTableOrAnalysisException(job.getTableName().getTbl());
                    table.readLock();
                    try {
                        SelectStmt selectStmt = selectStmtList.get(idx);
                        List<Long> tabletIds = selectStmt.getTableRefs().get(0).getSampleTabletIds();
                        for (Long tabletId : tabletIds) {
                            TabletMeta tabletMeta = Env.getCurrentEnv().getTabletInvertedIndex().getTabletMeta(
                                    tabletId);
                            Partition partition = table.getPartition(tabletMeta.getPartitionId());
                            long nowVersion = partition.getVisibleVersion();
                            long oldVersion = job.getPartitionToVersion().get(partition.getName());
                            if (nowVersion != oldVersion) {
                                LOG.warn("Tablet {} has changed version, old version = {}, now version = {}",
                                        tabletId, oldVersion, nowVersion);
                                return new ExportResult(true, new ExportFailMsg(
                                        ExportFailMsg.CancelType.RUN_FAIL,
                                        "Tablet {" + tabletId + "} has changed"), null);
                            }
                        }
                    } finally {
                        table.readUnlock();
                    }
                } catch (AnalysisException e) {
                    return new ExportResult(true,
                            new ExportFailMsg(ExportFailMsg.CancelType.RUN_FAIL, e.getMessage()), null);
                }
                try (AutoCloseConnectContext r = buildConnectContext()) {
                    StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, selectStmtList.get(idx));
                    job.setStmtExecutor(idx, stmtExecutor);
                    stmtExecutor.execute();
                    if (r.connectContext.getState().getStateType() == MysqlStateType.ERR) {
                        return new ExportResult(true, new ExportFailMsg(ExportFailMsg.CancelType.RUN_FAIL,
                                r.connectContext.getState().getErrorMessage()), null);
                    }
                    ExportJob.OutfileInfo outfileInfo = getOutFileInfo(r.connectContext.getResultAttachedInfo());
                    return new ExportResult(false, null, outfileInfo);
                } catch (Exception e) {
                    return new ExportResult(true, new ExportFailMsg(ExportFailMsg.CancelType.RUN_FAIL,
                            e.getMessage()),
                            null);
                } finally {
                    job.getStmtExecutor(idx).addProfileToSpan();
                }
            });
        }

        Boolean isFailed = false;
        ExportFailMsg failMsg = new ExportFailMsg();
        try {
            for (int i = 0; i < parallelNum; ++i) {
                Future<ExportResult> future = completionService.take();
                ExportResult result = future.get();
                if (!result.isFailed) {
                    outfileInfoList.add(result.getOutfileInfo());
                    ++completeTaskNum;
                    int progress = completeTaskNum * 100 / selectStmtList.size();
                    if (progress >= 100) {
                        progress = 99;
                    }
                    job.setProgress(progress);
                    LOG.info("Export Job {} finished {} outfile export and it's progress is {}%", job.getId(),
                            completeTaskNum, progress);
                } else {
                    isFailed = true;
                    failMsg.setCancelType(result.failMsg.getCancelType());
                    failMsg.setMsg(result.failMsg.getMsg());
                    LOG.warn("Exporting task failed because: {}", result.failMsg.getMsg());
                    break;
                }
            }
        } catch (Exception e) {
            isFailed = true;
            failMsg.setCancelType(CancelType.RUN_FAIL);
            failMsg.setMsg(e.getMessage());
        } finally {
            // cancel all executor
            if (isFailed) {
                for (int idx = 0; idx < parallelNum; ++idx) {
                    //if tablet version changed, we don't need to execute task, so stmtExecutor is null,
                    //and there is no need to cancel it.
                    if (null != job.getStmtExecutor(idx)) {
                        job.getStmtExecutor(idx).cancel();
                    }
                }
            }
            exportExecPool.shutdownNow();
        }

        if (isFailed) {
            job.cancel(failMsg.getCancelType(), failMsg.getMsg());
            return;
        }

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
