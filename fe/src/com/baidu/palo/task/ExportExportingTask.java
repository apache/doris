// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.task;

import com.baidu.palo.catalog.BrokerMgr;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ClientPool;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.Status;
import com.baidu.palo.common.util.DebugUtil;
import com.baidu.palo.common.util.ProfileManager;
import com.baidu.palo.common.util.RuntimeProfile;
import com.baidu.palo.common.util.TimeUtils;
import com.baidu.palo.load.ExportFailMsg;
import com.baidu.palo.load.ExportJob;
import com.baidu.palo.qe.Coordinator;
import com.baidu.palo.qe.QeProcessor;
import com.baidu.palo.service.FrontendOptions;
import com.baidu.palo.system.Backend;
import com.baidu.palo.thrift.TAgentResult;
import com.baidu.palo.thrift.TBrokerOperationStatus;
import com.baidu.palo.thrift.TBrokerOperationStatusCode;
import com.baidu.palo.thrift.TBrokerRenamePathRequest;
import com.baidu.palo.thrift.TBrokerVersion;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TPaloBrokerService;
import com.baidu.palo.thrift.TStatusCode;
import com.baidu.palo.thrift.TUniqueId;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Set;

public class ExportExportingTask extends MasterTask {
    private static final Logger LOG = LogManager.getLogger(ExportExportingTask.class);

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
        LOG.warn("begin exec export job. job: {}", job);

        synchronized (job) {
            if (job.getDoExportingThread() != null) {
                LOG.warn("export task already executing.");
                return;
            }
            job.setDoExportingThread(Thread.currentThread());
        }

        // Check exec fragments should already generated
        if (job.isReplayed()) {
            String failMsg = "do not have exec request.";
            job.cancel(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            return;
        }

        // if one instance finished, we send request to BE to exec next instance
        // TODO(lingbin): add retry sending logic if send fail
        List<Coordinator> coords = job.getCoordList();
        int coordSize = coords.size();
        for (int i = 0; i < coordSize; i++) {
            if (isCancelled) {
                break;
            }
            Coordinator coord = coords.get(i);
            execOneCoord(coord);
            int progress = (int) (i + 1) * 100 / coordSize;
            if (progress >= 100) {
                progress = 99;
            }
            job.setProgress(progress);

            coord.getQueryProfile().getCounterTotalTime().setValue(TimeUtils.getEstimatedTime(job.getStartTimeMs()));
            coord.endProfile();
            fragmentProfiles.add(coord.getQueryProfile());
        }

        if (isCancelled) {
            String failMsg = "export exporting job fail. ";
            failMsg += failStatus.getErrorMsg();
            job.cancel(cancelType, failMsg);
            LOG.warn("export exporting job fail. job: {}", job);
            registerProfile();
            return;
        }

        // release snapshot
        Status releaseSnapshotStatus = releaseSnapshotPaths();
        if (!releaseSnapshotStatus.ok()) {
            String failMsg = "release snapshot fail.";
            failMsg += releaseSnapshotStatus.getErrorMsg();
            job.cancel(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            LOG.warn("release snapshot fail. job:{}", job);
            registerProfile();
            return;
        }

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

        if (job.updateState(ExportJob.JobState.FINISHED)) {
            LOG.warn("export job successed. job: {}", job);
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
            QeProcessor.registerQuery(queryId, coord);
            needUnregister = true;
            actualExecCoord(queryId, coord);
        } catch (InternalException e) {
            onFailed(new Status(TStatusCode.INTERNAL_ERROR, e.getMessage()));
        } finally {
            if (needUnregister) {
                QeProcessor.unregisterQuery(queryId);
            }
        }

        return Status.OK;
    }

    private void actualExecCoord(TUniqueId queryId, Coordinator coord) {
        int waitSecond = Config.export_task_default_timeout_second;
        if (waitSecond <= 0) {
            onTimeout();
            return;
        }

        try {
            coord.exec();
        } catch (Exception e) {
            onFailed(new Status(TStatusCode.INTERNAL_ERROR, "export Coordinator execute failed."));
        }

        if (coord.join(waitSecond)) {
            Status status = coord.getExecStatus();
            if (status.ok()) {
                onFinished(coord.getExportFiles());
            } else {
                onFailed(status);
            }
        } else {
            onTimeout();
        }
    }

    private synchronized void onFinished(List<String> exportFiles) {
        job.addExportedFiles(exportFiles);
    }

    private synchronized void onFailed(Status failStatus) {
        isCancelled = true;
        this.failStatus = failStatus;
        cancelType = ExportFailMsg.CancelType.RUN_FAIL;
        String failMsg = "export exporting job fail. ";
        failMsg += failStatus.getErrorMsg();
        job.cancel(cancelType, failMsg);
        LOG.warn("export exporting job fail. job: {}", job);
    }

    public synchronized void onTimeout() {
        isCancelled = true;
        this.failStatus = new Status(TStatusCode.TIMEOUT, "timeout");
        cancelType = ExportFailMsg.CancelType.TIMEOUT;
        String failMsg = "export exporting job timeout";
        job.cancel(cancelType, failMsg);
        LOG.warn("export exporting job timeout. job: {}", job);
    }

    private void initProfile() {
        profile = new RuntimeProfile("Query");
        RuntimeProfile summaryProfile = new RuntimeProfile("Query");
        summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, String.valueOf(job.getId()));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(job.getStartTimeMs()));

        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - job.getStartTimeMs();
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(currentTimestamp));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, job.getState().toString());
        summaryProfile.addInfoString("Palo Version", "Palo version 2.0");
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

    private Status releaseSnapshotPaths() {
        List<Pair<TNetworkAddress, String>> snapshotPaths = job.getSnapshotPaths();
        LOG.debug("snapshotPaths:{}", snapshotPaths);
        for (Pair<TNetworkAddress, String> snapshotPath : snapshotPaths) {
            TNetworkAddress address = snapshotPath.first;
            String host = address.getHostname();
            int port = address.getPort();
            Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(host, port);
            if (backend == null) {
                return Status.CANCELLED;
            }
            long backendId = backend.getId();
            if (!Catalog.getCurrentSystemInfo().checkBackendAvailable(backendId)) {
                return Status.CANCELLED;
            }

            AgentClient client = new AgentClient(host, port);
            TAgentResult result = client.releaseSnapshot(snapshotPath.second);
            if (result.getStatus().getStatus_code() != TStatusCode.OK) {
                return Status.CANCELLED;
            }
        }
        snapshotPaths.clear();
        return Status.OK;
    }

    private Status moveTmpFiles() {
        BrokerMgr.BrokerAddress brokerAddress = null;
        try {
            String localIP = FrontendOptions.getLocalHostAddress();
            brokerAddress = Catalog.getInstance().getBrokerMgr().getBroker(job.getBrokerDesc().getName(), localIP);
        } catch (AnalysisException e) {
            String failMsg = "Broker rename failed. msg=" + e.getMessage();
            LOG.warn(failMsg);
            return new Status(TStatusCode.CANCELLED, failMsg);
        }
        TNetworkAddress address = new TNetworkAddress(brokerAddress.ip, brokerAddress.port);
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
            String file = exportedFile.substring(exportedFile.lastIndexOf("/") + 1);
            String destPath = exportPath + "/" + file;
            LOG.debug("rename {} to {}", exportedFile, destPath);
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
