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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.ExportFailMsg;
import org.apache.doris.load.ExportJob;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerRenamePathRequest;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class ExportExportingTask extends MasterTask {
    private static final Logger LOG = LogManager.getLogger(ExportExportingTask.class);
    private static final int RETRY_NUM = 2;

    protected final ExportJob job;

    private boolean isCancelled = false;
    private Status failStatus = Status.OK;
    private ExportFailMsg.CancelType cancelType = ExportFailMsg.CancelType.UNKNOWN;

    private RuntimeProfile profile = new RuntimeProfile("Export");
    private List<RuntimeProfile> fragmentProfiles = Lists.newArrayList();

    public ExportExportingTask(ExportJob job) {
        this.job = job;
        this.signature = job.getId();
    }

    @Override
    protected void exec() {
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

        if (job.isReplayed()) {
            // If the job is created from replay thread, all plan info will be lost.
            // so the job has to be cancelled.
            String failMsg = "FE restarted or Master changed during exporting. Job must be cancelled.";
            job.cancel(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            return;
        }

        // if one instance finished, we send request to BE to exec next instance
        List<Coordinator> coords = job.getCoordList();
        int coordSize = coords.size();
        for (int i = 0; i < coordSize; i++) {
            if (isCancelled) {
                break;
            }
            Coordinator coord = coords.get(i);
            for (int j = 0; j < RETRY_NUM; ++j) {
                execOneCoord(coord);
                if (coord.getExecStatus().ok()) {
                    break;
                }
                if (j < RETRY_NUM - 1) {
                    TUniqueId queryId = coord.getQueryId();
                    coord.clearExportStatus();

                    // generate one new queryId here, to avoid being rejected by BE,
                    // because the request is considered as a repeat request.
                    // we make the high part of query id unchanged to facilitate tracing problem by log.
                    UUID uuid = UUID.randomUUID();
                    TUniqueId newQueryId = new TUniqueId(queryId.hi, uuid.getLeastSignificantBits());
                    coord.setQueryId(newQueryId);
                    LOG.warn("export exporting job fail. err: {}. query_id: {}, job: {}. retry. {}, new query id: {}",
                            coord.getExecStatus().getErrorMsg(), DebugUtil.printId(queryId), job.getId(), j,
                            DebugUtil.printId(newQueryId));
                }
            }

            if (!coord.getExecStatus().ok()) {
                onFailed(coord);
            } else {
                int progress = (int) (i + 1) * 100 / coordSize;
                if (progress >= 100) {
                    progress = 99;
                }
                job.setProgress(progress);
                LOG.info("finish coordinator with query id {}, export job: {}. progress: {}",
                        DebugUtil.printId(coord.getQueryId()), job.getId(), progress);
            }

            RuntimeProfile queryProfile = coord.getQueryProfile();
            if (queryProfile != null) {
                queryProfile.getCounterTotalTime().setValue(TimeUtils.getEstimatedTime(job.getStartTimeMs()));
            }
            coord.endProfile();
            fragmentProfiles.add(coord.getQueryProfile());
        }

        if (isCancelled) {
            job.cancel(cancelType, null /* error msg is already set */);
            registerProfile();
            return;
        }

        if (job.getBrokerDesc().getStorageType() == StorageBackend.StorageType.BROKER) {
            // move tmp file to final destination
            Status mvStatus = moveTmpFiles();
            if (!mvStatus.ok()) {
                String failMsg = "move tmp file to final destination fail.";
                failMsg += mvStatus.getErrorMsg();
                job.cancel(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
                LOG.warn("move tmp file to final destination fail. job:{}", job);
                registerProfile();
                return;
            }
        }

        // release snapshot
        Status releaseSnapshotStatus = job.releaseSnapshotPaths();
        if (!releaseSnapshotStatus.ok()) {
            // even if release snapshot failed, do nothing cancel this job.
            // snapshot will be removed by GC thread on BE, finally.
            LOG.warn("failed to release snapshot for export job: {}. err: {}", job.getId(),
                    releaseSnapshotStatus.getErrorMsg());
        }

        if (job.updateState(ExportJob.JobState.FINISHED)) {
            LOG.warn("export job success. job: {}", job);
            registerProfile();
        }

        synchronized (this) {
            job.setDoExportingThread(null);
        }
    }

    private Status execOneCoord(Coordinator coord) {
        TUniqueId queryId = coord.getQueryId();
        boolean needUnregister = false;
        try {
            QeProcessorImpl.INSTANCE.registerQuery(queryId, coord);
            needUnregister = true;
            actualExecCoord(queryId, coord);
        } catch (UserException e) {
            LOG.warn("export exporting internal error, job: {}", job.getId(), e);
            return new Status(TStatusCode.INTERNAL_ERROR, e.getMessage());
        } finally {
            if (needUnregister) {
                QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
            }
        }
        return Status.OK;
    }

    private void actualExecCoord(TUniqueId queryId, Coordinator coord) {
        int leftTimeSecond = getLeftTimeSecond();
        if (leftTimeSecond <= 0) {
            onTimeout();
            return;
        }
        
        try {
            coord.setTimeout(leftTimeSecond);
            coord.exec();
        } catch (Exception e) {
            LOG.warn("export Coordinator execute failed. job: {}", job.getId(), e);
        }

        if (coord.join(leftTimeSecond)) {
            Status status = coord.getExecStatus();
            if (status.ok()) {
                onSubTaskFinished(coord.getExportFiles());
            }
        } else {
            coord.cancel();
        }
    }

    private int getLeftTimeSecond() {
        return (int) (job.getTimeoutSecond() - (System.currentTimeMillis() - job.getCreateTimeMs()) / 1000);
    }

    private synchronized void onSubTaskFinished(List<String> exportFiles) {
        job.addExportedFiles(exportFiles);
    }

    private synchronized void onFailed(Coordinator coordinator) {
        isCancelled = true;
        this.failStatus = coordinator.getExecStatus();
        cancelType = ExportFailMsg.CancelType.RUN_FAIL;
        String failMsg = "export exporting job fail. query id: " + DebugUtil.printId(coordinator.getQueryId())
                + ", ";
        failMsg += failStatus.getErrorMsg();
        job.setFailMsg(new ExportFailMsg(cancelType, failMsg));
        LOG.warn("export exporting job fail. err: {}. job: {}", failMsg, job);
    }

    public synchronized void onTimeout() {
        isCancelled = true;
        this.failStatus = new Status(TStatusCode.TIMEOUT, "timeout");
        cancelType = ExportFailMsg.CancelType.TIMEOUT;
        String failMsg = "export exporting job timeout.";
        job.setFailMsg(new ExportFailMsg(cancelType, failMsg));
        LOG.warn("export exporting job timeout. job: {}", job);
    }

    private void initProfile() {
        profile = new RuntimeProfile("ExportJob");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, String.valueOf(job.getId()));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(job.getStartTimeMs()));

        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - job.getStartTimeMs();
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(currentTimestamp));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, job.getState().toString());
        summaryProfile.addInfoString(ProfileManager.DORIS_VERSION, Version.DORIS_BUILD_VERSION);
        summaryProfile.addInfoString(ProfileManager.USER, "xxx");
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, String.valueOf(job.getDbId()));
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, job.getSql());
        profile.addChild(summaryProfile);
    }

    private void registerProfile() {
        initProfile();
        for (RuntimeProfile p : fragmentProfiles) {
            profile.addChild(p);
        }
        ProfileManager.getInstance().pushProfile(profile);
    }

    private Status moveTmpFiles() {
        FsBroker broker = null;
        try {
            String localIP = FrontendOptions.getLocalHostAddress();
            broker = Catalog.getCurrentCatalog().getBrokerMgr().getBroker(job.getBrokerDesc().getName(), localIP);
        } catch (AnalysisException e) {
            String failMsg = "get broker failed. export job: " + job.getId() + ". msg: " + e.getMessage();
            LOG.warn(failMsg);
            return new Status(TStatusCode.CANCELLED, failMsg);
        }
        TNetworkAddress address = new TNetworkAddress(broker.ip, broker.port);
        TPaloBrokerService.Client client = null;
        try {
            client = ClientPool.brokerPool.borrowObject(address);
        } catch (Exception e) {
            try {
                client = ClientPool.brokerPool.borrowObject(address);
            } catch (Exception e1) {
                String failMsg = "create connection to broker(" + address + ") failed";
                LOG.warn(failMsg);
                return new Status(TStatusCode.CANCELLED, failMsg);
            }
        }
        boolean failed = false;
        Set<String> exportedFiles = job.getExportedFiles();
        List<String> newFiles = Lists.newArrayList();
        String exportPath = job.getExportPath();
        for (String exportedFile : exportedFiles) {
            // move exportPath/__doris_tmp/file to exportPath/file
            String file = exportedFile.substring(exportedFile.lastIndexOf("/") + 1);
            String destPath = exportPath + "/" + file;
            LOG.debug("rename {} to {}, export job: {}", exportedFile, destPath, job.getId());
            String failMsg = "";
            try {
                TBrokerRenamePathRequest request = new TBrokerRenamePathRequest(
                        TBrokerVersion.VERSION_ONE, exportedFile, destPath, job.getBrokerDesc().getProperties());
                TBrokerOperationStatus tBrokerOperationStatus = null;
                tBrokerOperationStatus = client.renamePath(request);
                if (tBrokerOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    failed = true;
                    failMsg = "Broker renamePath failed. srcPath=" + exportedFile + ", destPath=" + destPath
                            + ", broker=" + address  + ", msg=" + tBrokerOperationStatus.getMessage();
                    return new Status(TStatusCode.CANCELLED, failMsg);
                } else {
                    newFiles.add(destPath);
                }
            } catch (TException e) {
                failed = true;
                failMsg = "Broker renamePath failed. srcPath=" + exportedFile + ", destPath=" + destPath
                        + ", broker=" + address  + ", msg=" + e.getMessage();
                return new Status(TStatusCode.CANCELLED, failMsg);
            } finally {
                if (failed) {
                    ClientPool.brokerPool.invalidateObject(address, client);
                }
            }
        }

        if (!failed) {
            exportedFiles.clear();
            job.addExportedFiles(newFiles);
            ClientPool.brokerPool.returnObject(address, client);
        }

        return Status.OK;
    }
}
