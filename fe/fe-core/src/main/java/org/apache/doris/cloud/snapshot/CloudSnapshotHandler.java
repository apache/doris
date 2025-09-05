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

package org.apache.doris.cloud.snapshot;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.EditLogFileOutputStream;
import org.apache.doris.persist.Storage;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class CloudSnapshotHandler extends MasterDaemon {

    private static final String SNAPSHOT_DIR = "/snapshot";
    private static final Logger LOG = LogManager.getLogger(CloudSnapshotHandler.class);
    private String snapshotDir;

    // auto snapshot job
    private CloudSnapshotJob autoSnapshotJob = null;
    private long autoSnapshotInterval; // seconds
    private long lastFinishedAutoSnapshotTime = -1; // second
    private boolean autoSnapshotJobInitialized = false;

    // manual snapshot jobs
    private LinkedBlockingQueue<CloudSnapshotJob> manualSnapshotJobs = Queues.newLinkedBlockingQueue();

    public CloudSnapshotHandler() {
        super("cloud snapshot handler", Config.cloud_snapshot_handler_interval_second * 1000);
        this.snapshotDir = Config.meta_dir + SNAPSHOT_DIR;
    }

    public void initialize() {
        File snapshotDir = new File(this.snapshotDir);
        if (snapshotDir.exists()) {
            if (snapshotDir.isDirectory()) {
                for (File file : snapshotDir.listFiles()) {
                    LOG.info("delete snapshot file: {}", file.getAbsolutePath());
                    file.delete();
                }
            }
            snapshotDir.delete();
        }
        snapshotDir.mkdirs();
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            getLastFinishedAutoSnapshotTime();
            if (!autoSnapshotJobInitialized) {
                refreshAutoSnapshotJob();
            }
            process();
        } catch (Throwable e) {
            LOG.warn("failed to process one round of cloud snapshot", e);
        }
    }

    public synchronized void refreshAutoSnapshotJob() {
        Cloud.GetInstanceResponse response = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudInstance();
        Cloud.SnapshotSwitchStatus switchStatus = response.getInstance().getSnapshotSwitchStatus();
        if (switchStatus == Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON
                && response.getInstance().getMaxReservedSnapshot() > 0) {
            if (this.autoSnapshotJob == null) {
                this.autoSnapshotJob = new CloudSnapshotJob(true);
            }
            this.autoSnapshotInterval = response.getInstance().getSnapshotIntervalSeconds();
        } else {
            this.autoSnapshotJob = null;
        }
        autoSnapshotJobInitialized = true;
    }

    private void getLastFinishedAutoSnapshotTime() {
        if (lastFinishedAutoSnapshotTime >= 0) {
            return;
        }
        try {
            Cloud.ListSnapshotRequest request = Cloud.ListSnapshotRequest.newBuilder().setIncludeAborted(false)
                    .build();
            Cloud.ListSnapshotResponse response = MetaServiceProxy.getInstance().listSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("listSnapshot response: {} ", response);
                return;
            }
            for (Cloud.SnapshotInfoPB snapshotInfoPB : response.getSnapshotsList()) {
                if (!snapshotInfoPB.getAutoSnapshot()) {
                    continue;
                }
                if (snapshotInfoPB.getFinishAt() > lastFinishedAutoSnapshotTime) {
                    lastFinishedAutoSnapshotTime = snapshotInfoPB.getFinishAt();
                }
            }
            if (lastFinishedAutoSnapshotTime == -1) {
                lastFinishedAutoSnapshotTime = 0;
            }
            LOG.info("lastFinishedAutoSnapshotTime: {}", lastFinishedAutoSnapshotTime);
        } catch (RpcException e) {
            LOG.warn("failed to list snapshot", e);
        }
    }

    public void submitJob(CloudSnapshotJob job) {
        manualSnapshotJobs.add(job);
    }

    private void process() {
        while (true) {
            if (manualSnapshotJobs.isEmpty()) {
                break;
            }
            CloudSnapshotJob job = manualSnapshotJobs.poll();
            try {
                execute(job);
            } catch (Exception e) {
                LOG.warn("manual snapshot job failed", e);
            }
        }
        if (autoSnapshotJob != null && lastFinishedAutoSnapshotTime + autoSnapshotInterval * 60
                < System.currentTimeMillis() / 1000) {
            try {
                execute(autoSnapshotJob);
            } catch (Exception e) {
                LOG.warn("auto snapshot job failed", e);
            }
        }
    }

    private void execute(CloudSnapshotJob job) {
        String snapshotId = null;
        String imageUrl = null;
        try {
            // 0. begin snapshot
            long logId;
            Cloud.ObjectStoreInfoPB objInfo;
            synchronized (Env.getCurrentEnv().getEditLog()) {
                Cloud.BeginSnapshotResponse response = beginSnapshot(job);
                snapshotId = response.getSnapshotId();
                imageUrl = response.getImageUrl();
                objInfo = response.getObjInfo();
                // 1. write edit log
                SnapshotState snapshotState = new SnapshotState(snapshotId, imageUrl);
                logId = Env.getCurrentEnv().getEditLog().logBeginSnapshot(snapshotState);
            }
            // 2. upload image
            uploadImage(snapshotId, imageUrl, objInfo, logId);
            // 3. commit snapshot
            commitSnapshot(snapshotId, imageUrl, logId);
            if (job.isAuto()) {
                lastFinishedAutoSnapshotTime = System.currentTimeMillis() / 1000;
            }
            LOG.info("succeed to snapshot for id: {}, imageUrl: {}, logId: {}, auto: {}, label: {}", snapshotId,
                    imageUrl, logId, job.isAuto(), job.getLabel());
        } catch (Exception e) {
            LOG.warn("failed to snapshot for id: {}, imageUrl: {}, auto: {}, label: {}", snapshotId, imageUrl,
                    job.isAuto(), job.getLabel(), e);
            // abort snapshot
            try {
                if (snapshotId != null) {
                    abortSnapshot(snapshotId, e.getMessage());
                }
            } catch (Exception e1) {
                LOG.warn("failed to abort snapshot for id: {}", snapshotId, e1);
            }
        }
    }

    private Cloud.BeginSnapshotResponse beginSnapshot(CloudSnapshotJob job) throws Exception {
        Cloud.BeginSnapshotRequest.Builder builder = Cloud.BeginSnapshotRequest.newBuilder()
                .setTimeoutSeconds(Config.cloud_snapshot_timeout_seconds).setAutoSnapshot(job.isAuto());
        if (job.getTtl() > 0) {
            builder.setTtlSeconds(job.getTtl());
        }
        if (job.getLabel() != null) {
            builder.setSnapshotLabel(job.getLabel());
        }
        try {
            Cloud.BeginSnapshotResponse response = MetaServiceProxy.getInstance().beginSnapshot(builder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("beginSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            return response;
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private void commitSnapshot(String snapshotId, String imageUrl, long logId) throws Exception {
        try {
            Cloud.CommitSnapshotRequest request = Cloud.CommitSnapshotRequest.newBuilder().setSnapshotId(snapshotId)
                    .setImageUrl(imageUrl).setLastJournalId(logId).build();
            Cloud.CommitSnapshotResponse response = MetaServiceProxy.getInstance().commitSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("commitSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private void uploadImage(String snapshotId, String imageUrl, Cloud.ObjectStoreInfoPB objInfo, long logId)
            throws Exception {
        LOG.info("start to snapshot for id: {}, imageUrl: {}, logId: {}", snapshotId, imageUrl, logId);
        // scan edit logs between imageVersion + 1 and logId
        long imageVersion = getImageVersion();
        if (imageVersion + 1 < logId) {
            writeSnapshotEditLogFile(imageVersion + 1, logId, snapshotId);
        }
        // use lock to prevent checkpoint
        // upload image files
        String imageDir = Env.getServingEnv().getImageDir();
        String imageFileName = "image." + imageVersion;
        File imageFile = new File(imageDir + "/" + imageFileName);
        if (!imageFile.exists()) {
            LOG.error("image file does not exist: {}", imageFile.getAbsoluteFile());
            throw new DdlException("image file does not exist: " + imageFile.getAbsoluteFile());
        }
        RemoteBase.ObjectInfo objectInfo = new RemoteBase.ObjectInfo(objInfo);
        RemoteBase remote = RemoteBase.newInstance(objectInfo);
        remote.putObject(imageFile, imageUrl + "/" + imageFileName);
        // edit log
        File snapshotEditLogFile = new File(snapshotDir, "edits." + logId);
        remote.putObject(snapshotEditLogFile, imageUrl + "/" + snapshotEditLogFile.getName());
        snapshotEditLogFile.delete();
    }

    private long getImageVersion() throws DdlException {
        try {
            Storage storage = new Storage(Env.getServingEnv().getImageDir());
            return storage.getLatestImageSeq();
        } catch (Throwable e) {
            LOG.warn("get image version failed", e);
            throw new DdlException("get image version failed: " + e.getMessage());
        }
    }

    private void writeSnapshotEditLogFile(long fromJournalId, long toJournalId, String snapshotId)
            throws IOException, DdlException {
        LOG.info("scan journal from {} to {} for snapshotId: {}", fromJournalId, toJournalId, snapshotId);
        JournalCursor cursor = Env.getCurrentEnv().getEditLog().read(fromJournalId, toJournalId);
        if (cursor == null) {
            LOG.warn("failed to get cursor from {} to {}", fromJournalId, toJournalId);
            throw new DdlException("failed to get cursor from " + fromJournalId + " to " + toJournalId);
        }

        File snapshotEditLogFile = new File(snapshotDir, "edits." + toJournalId);
        if (snapshotEditLogFile.exists()) {
            snapshotEditLogFile.delete();
        }
        snapshotEditLogFile.createNewFile();
        EditLogFileOutputStream outputStream = null;
        try {
            outputStream = new EditLogFileOutputStream(snapshotEditLogFile);
            while (true) {
                Pair<Long, JournalEntity> kv = cursor.next();
                if (kv == null) {
                    break;
                }
                // Long logId = kv.first;
                JournalEntity entity = kv.second;
                if (entity == null) {
                    break;
                }
                outputStream.write(entity.getOpCode(), entity.getData());
            }
            outputStream.setReadyToFlush();
            outputStream.flush();
            outputStream.close();
        } catch (Exception e) {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ex) {
                    LOG.warn("failed to close output stream for id: {}", snapshotId, ex);
                }
            }
            try {
                if (snapshotEditLogFile.exists()) {
                    snapshotEditLogFile.delete();
                }
            } catch (Exception ex) {
                LOG.warn("failed to delete snapshot file for id: {}", snapshotId, ex);
            }
            throw new DdlException(e.getMessage());
        }
    }

    private void abortSnapshot(String snapshotId, String reason) throws Exception {
        try {
            Cloud.AbortSnapshotRequest request = Cloud.AbortSnapshotRequest.newBuilder().setSnapshotId(snapshotId)
                    .setReason(reason).build();
            Cloud.AbortSnapshotResponse response = MetaServiceProxy.getInstance().abortSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("abortSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }
}
