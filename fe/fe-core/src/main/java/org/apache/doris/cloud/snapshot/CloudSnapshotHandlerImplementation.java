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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.storage.ObjectInfo;
import org.apache.doris.cloud.storage.ObjectInfoAdapter;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.FileSystemTransferUtil;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.UploadPartResult;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.master.Checkpoint;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.EditLogFileInputStream;
import org.apache.doris.persist.EditLogFileOutputStream;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.Storage;
import org.apache.doris.persist.meta.MetaReader;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class CloudSnapshotHandlerImplementation extends CloudSnapshotHandler {

    private static final Logger LOG = LogManager.getLogger(CloudSnapshotHandlerImplementation.class);

    private static final String SNAPSHOT_DIR = "/snapshot/";
    private static final String CLONE_SNAPSHOT_DIR = "/clone-snapshot/";
    private String snapshotDir;
    private String cloneSnapshotDir;

    // auto snapshot job
    private CloudSnapshotJob autoSnapshotJob = null;
    private long autoSnapshotIntervalSeconds;
    private long lastFinishedAutoSnapshotTime = -1; // second

    // manual snapshot jobs
    private LinkedBlockingQueue<CloudSnapshotJob> manualSnapshotJobs = Queues.newLinkedBlockingQueue();

    @Override
    public void initialize() {
        this.snapshotDir = Config.meta_dir + SNAPSHOT_DIR;
        createDir(this.snapshotDir);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            getLastFinishedAutoSnapshotTime();
            refreshAutoSnapshotJob();
            executeJobs();
        } catch (Throwable e) {
            LOG.warn("failed to process one round of cloud snapshot", e);
        }
    }

    @Override
    public void submitJob(long ttl, String label) throws Exception {
        CloudSnapshotJob job = new CloudSnapshotJob(false, ttl, label);
        beginSnapshotAndWriteEditLog(job);
        manualSnapshotJobs.add(job);
    }

    @Override
    public synchronized void refreshAutoSnapshotJob() {
        Cloud.GetInstanceResponse response = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudInstance();
        Cloud.InstanceInfoPB instanceInfo = response.getInstance();
        long maxReservedSnapshot = instanceInfo.hasMaxReservedSnapshot() ? instanceInfo.getMaxReservedSnapshot() : 0;
        long autoSnapshotIntervalSeconds = instanceInfo.hasSnapshotIntervalSeconds()
                ? instanceInfo.getSnapshotIntervalSeconds() : 3600;
        if (instanceInfo.hasSnapshotSwitchStatus()
                && instanceInfo.getSnapshotSwitchStatus() == Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON
                && maxReservedSnapshot > 0) {
            if (this.autoSnapshotJob == null) {
                this.autoSnapshotJob = new CloudSnapshotJob(true);
            }
            this.autoSnapshotIntervalSeconds = autoSnapshotIntervalSeconds;
        } else {
            this.autoSnapshotJob = null;
        }
        LOG.debug("auto snapshot job is {}, interval: {}", this.autoSnapshotJob != null ? "ON" : "OFF",
                this.autoSnapshotIntervalSeconds);
    }

    private void getLastFinishedAutoSnapshotTime() {
        if (lastFinishedAutoSnapshotTime >= 0) {
            return;
        }
        try {
            Cloud.ListSnapshotResponse response = listSnapshot(false);
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
        } catch (DdlException e) {
            LOG.warn("failed to list snapshot", e);
        }
    }

    private void executeJobs() {
        while (true) {
            if (manualSnapshotJobs.isEmpty()) {
                break;
            }
            CloudSnapshotJob job = manualSnapshotJobs.poll();
            try {
                executeJob(job);
            } catch (Exception e) {
                LOG.warn("manual snapshot job failed: {}", job, e);
            }
        }
        if (autoSnapshotJob != null && lastFinishedAutoSnapshotTime + autoSnapshotIntervalSeconds
                < System.currentTimeMillis() / 1000) {
            try {
                String label = "auto_snapshot_" + System.currentTimeMillis();
                autoSnapshotJob.setLabel(label);
                executeJob(autoSnapshotJob);
            } catch (Exception e) {
                LOG.warn("auto snapshot job failed: {}", autoSnapshotJob, e);
            } finally {
                autoSnapshotJob.setLabel(null);
            }
        }
    }

    private void beginSnapshotAndWriteEditLog(CloudSnapshotJob job) throws Exception {
        synchronized (Env.getCurrentEnv().getEditLog()) {
            // begin snapshot
            Cloud.BeginSnapshotResponse response = beginSnapshot(job);
            job.setBeginSnapshotResponse(response);
            // write edit log
            SnapshotState snapshotState = new SnapshotState(response.getSnapshotId(), response.getImageUrl());
            long logId = Env.getCurrentEnv().getEditLog().logBeginSnapshot(snapshotState);
            job.setLogId(logId);
        }
        job.setSnapshotDataSize(getSnapshotDataSize());
    }

    private void executeJob(CloudSnapshotJob job) {
        String snapshotId = null;
        long logId = 0;
        try {
            LOG.info("start to snapshot for job: {}", job);
            // 1. begin snapshot and write edit log
            if (job.isAuto()) {
                beginSnapshotAndWriteEditLog(job);
            }
            if (job.getBeginSnapshotResponse() == null) {
                throw new DdlException("snapshot failed because begin snapshot response is null");
            }
            if (job.getLogId() == 0) {
                throw new DdlException("snapshot failed because log id is 0");
            }
            Cloud.BeginSnapshotResponse beginSnapshotResponse = job.getBeginSnapshotResponse();
            snapshotId = beginSnapshotResponse.getSnapshotId();
            String imageUrl = beginSnapshotResponse.getImageUrl();
            Cloud.ObjectStoreInfoPB objInfo = beginSnapshotResponse.getObjInfo();
            logId = job.getLogId();
            // 2. upload image
            Checkpoint checkpoint = Env.getCurrentEnv().getCheckpointer();
            long imageFileSize;
            File imageZipFile;
            checkpoint.getLock().readLock().lock();
            try {
                if (DebugPointUtil.isEnable("CloudSnapshotHandler.uploadImage.fail")) {
                    throw new Exception("inject CloudSnapshotHandler.uploadImage.fail");
                }
                imageZipFile = generateImage(snapshotId, imageUrl, logId);
                imageFileSize = imageZipFile.length();
            } finally {
                checkpoint.getLock().readLock().unlock();
            }
            uploadImage(snapshotId, imageUrl, objInfo, imageZipFile);
            // 3. commit snapshot
            commitSnapshot(snapshotId, imageUrl, logId, imageFileSize, job.getSnapshotDataSize());
            if (job.isAuto()) {
                lastFinishedAutoSnapshotTime = System.currentTimeMillis() / 1000;
            }
            LOG.info("succeed to snapshot for job: {}, id: {}, imageUrl: {}, logId: {}", job, snapshotId, imageUrl,
                    logId);
        } catch (Exception e) {
            LOG.warn("failed to snapshot for job: {}", job, e);
            // abort snapshot
            try {
                if (snapshotId != null) {
                    abortSnapshot(snapshotId, e.getMessage());
                }
            } catch (Exception e1) {
                LOG.warn("failed to abort snapshot for job: {}", job, e1);
            }
            // delete edit log file and zip file
            deleteFiles(getEditLogFile(logId), getImageZipFile(snapshotId));
        } finally {
            job.setBeginSnapshotResponse(null);
            job.setLogId(0);
        }
    }

    private Cloud.BeginSnapshotResponse beginSnapshot(CloudSnapshotJob job) throws Exception {
        Cloud.BeginSnapshotRequest.Builder builder = Cloud.BeginSnapshotRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id).setTimeoutSeconds(Config.cloud_snapshot_timeout_seconds)
                .setAutoSnapshot(job.isAuto());
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

    private void commitSnapshot(String snapshotId, String imageUrl, long logId, long imageFileSize,
            long snapshotDataSize) throws Exception {
        try {
            Cloud.CommitSnapshotRequest request = Cloud.CommitSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setSnapshotId(snapshotId).setImageUrl(imageUrl)
                    .setLastJournalId(logId).setSnapshotMetaImageSize(imageFileSize)
                    .setSnapshotLogicalDataSize(snapshotDataSize).build();
            Cloud.CommitSnapshotResponse response = MetaServiceProxy.getInstance().commitSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("commitSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private Pair<Boolean, String> updateSnapshotUploadId(String snapshotId, String uploadFile, String uploadId) {
        try {
            Cloud.UpdateSnapshotRequest request = Cloud.UpdateSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setSnapshotId(snapshotId).setUploadFile(uploadFile)
                    .setUploadId(uploadId).build();
            Cloud.UpdateSnapshotResponse response = MetaServiceProxy.getInstance().updateSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("updateSnapshot response: {} ", response);
                return Pair.of(false, response.getStatus().getMsg());
            }
            return Pair.of(true, null);
        } catch (RpcException e) {
            LOG.warn("failed to update snapshot for snapshotId: {}, fileName: {}, uploadId: {}",
                    snapshotId, uploadFile, uploadId, e);
            return Pair.of(false, e.getMessage());
        }
    }

    private File generateImage(String snapshotId, String imageUrl, long logId) throws Exception {
        LOG.info("start to snapshot for id: {}, imageUrl: {}, logId: {}", snapshotId, imageUrl, logId);
        List<File> files = new ArrayList<>();

        // 1. check image file exist
        long imageVersion = getImageVersion();
        if (imageVersion > logId) {
            throw new DdlException("image version " + imageVersion + " is larger than log id " + logId);
        }
        if (imageVersion > 0) {
            String imageDir = Env.getServingEnv().getImageDir();
            String imageFileName = "image." + imageVersion;
            File imageFile = new File(imageDir + "/" + imageFileName);
            if (!imageFile.exists()) {
                LOG.error("image file does not exist: {}", imageFile.getAbsoluteFile());
                throw new DdlException("image file does not exist: " + imageFile.getAbsoluteFile());
            }
            files.add(imageFile);
        }

        // 2. scan edit logs between [imageVersion + 1, logId], write edit log file
        File snapshotEditLogFile = null;
        if (imageVersion + 1 < logId) {
            snapshotEditLogFile = writeSnapshotEditLogFile(imageVersion + 1, logId, snapshotId);
            files.add(snapshotEditLogFile);
        }

        // 3. compress files
        File zipFile = compressFiles(snapshotId, files);
        // 4. delete edit log file
        deleteFile(snapshotEditLogFile);
        return zipFile;
    }

    private void uploadImage(String snapshotId, String imageUrl, Cloud.ObjectStoreInfoPB objInfo, File zipFile)
            throws Exception {
        while (DebugPointUtil.isEnable("CloudSnapshotHandler.uploadImage.wait")) {
            Thread.sleep(5000);
        }

        // 4, upload zip file
        ObjectInfo objectInfo = new ObjectInfo(objInfo);
        try (ObjFileSystem fs = createObjFileSystem(objectInfo)) {
            uploadFileByMultipart(fs, zipFile,
                    formatRemotePath(objectInfo.getBucket(),
                            formatRemoteKey(objectInfo.getPrefix(), imageUrl, zipFile.getName())),
                    (Function<String, Pair<Boolean, String>>) uploadId -> updateSnapshotUploadId(snapshotId,
                            zipFile.getName(), uploadId));
        }

        // 5. delete zip file
        deleteFile(zipFile);
    }

    private File getEditLogFile(long logId) {
        return new File(this.snapshotDir, "edits." + logId);
    }

    private File getImageZipFile(String snapshotId) {
        File directory = new File(this.snapshotDir);
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles(
                    (dir, name) -> name.startsWith(snapshotId + ".") && name.endsWith(".zip"));
            if (files.length > 0) {
                return files[0];
            }
        }
        return null;
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

    private File writeSnapshotEditLogFile(long fromJournalId, long toJournalId, String snapshotId) throws Exception {
        LOG.info("scan journal from {} to {} for snapshotId: {}", fromJournalId, toJournalId, snapshotId);
        JournalCursor cursor = Env.getCurrentEnv().getEditLog().getJournal().read(fromJournalId, toJournalId, false);
        if (cursor == null) {
            LOG.warn("failed to get cursor from {} to {}", fromJournalId, toJournalId);
            throw new DdlException("failed to get cursor from " + fromJournalId + " to " + toJournalId);
        }

        File snapshotEditLogFile = getEditLogFile(toJournalId);
        deleteFile(snapshotEditLogFile);
        if (!snapshotEditLogFile.createNewFile()) {
            LOG.warn("failed to create snapshot edits log file {}", snapshotEditLogFile.getAbsolutePath());
            throw new Exception("failed to create snapshot edits log file " + snapshotEditLogFile.getAbsolutePath());
        }
        EditLogFileOutputStream outputStream = null;
        try {
            long dataSize = 0;
            outputStream = new EditLogFileOutputStream(snapshotEditLogFile);
            while (true) {
                Pair<Long, JournalEntity> kv = cursor.next();
                if (kv == null) {
                    break;
                }
                JournalEntity entity = kv.second;
                if (entity == null) {
                    break;
                }
                outputStream.write(entity.getOpCode(), entity.getData());
                dataSize += entity.getDataSize() + 2; // 2 bytes for op code
                if (dataSize > 1048576) { // 1 MB
                    outputStream.setReadyToFlush();
                    outputStream.flush();
                    dataSize = 0;
                }
            }
            if (dataSize > 0) {
                outputStream.setReadyToFlush();
                outputStream.flush();
            }
            outputStream.close();
            LOG.info("finish write snapshot edit file, snapshot_id: {}, file_name: {}, file_size: {}", snapshotId,
                    snapshotEditLogFile.getName(), snapshotEditLogFile.length());
            return snapshotEditLogFile;
        } catch (Exception e) {
            LOG.warn("write snapshot edit log failed for id: {}", snapshotId, e);
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ex) {
                    LOG.warn("failed to close output stream for id: {}", snapshotId, ex);
                }
            }
            deleteFile(snapshotEditLogFile);
            throw e;
        }
    }

    private void abortSnapshot(String snapshotId, String reason) throws Exception {
        try {
            Cloud.AbortSnapshotRequest request = Cloud.AbortSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id).setSnapshotId(snapshotId).setReason(reason).build();
            Cloud.AbortSnapshotResponse response = MetaServiceProxy.getInstance().abortSnapshot(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("abortSnapshot response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    // ==== for clone cluster snapshot ====

    @Override
    public void cloneSnapshot(String clusterSnapshotFile) throws Exception {
        CloneSnapshotState cloneSnapshotState = parseClusterSnapshotFile(clusterSnapshotFile);
        Cloud.CloneInstanceResponse response = cloneSnapshot(cloneSnapshotState);
        this.cloneSnapshotDir = Config.meta_dir + CLONE_SNAPSHOT_DIR;
        createDir(this.cloneSnapshotDir);
        Pair<File, File> files = downloadImage(cloneSnapshotState.getFromSnapshotId(), response);
        loadSnapshotImage(files.first, files.second);
        truncateDir(this.cloneSnapshotDir);
        LOG.info("clone snapshot {} successfully", cloneSnapshotState.getFromSnapshotId());
    }

    private CloneSnapshotState parseClusterSnapshotFile(String clusterSnapshotFile) {
        LOG.info("load cluster snapshot from file: {}", clusterSnapshotFile);
        File file = new File(clusterSnapshotFile);
        if (!file.exists()) {
            LOG.error("cluster snapshot file {} does not exist", clusterSnapshotFile);
            System.exit(-1);
        }

        CloneSnapshotState cloneSnapshotState = null;
        try {
            cloneSnapshotState = new ObjectMapper().readValue(file, CloneSnapshotState.class);
            cloneSnapshotState.check();
        } catch (Exception e) {
            LOG.error("failed to parse cluster snapshot file {}", clusterSnapshotFile, e);
            System.exit(-1);
        }
        return cloneSnapshotState;
    }

    private Cloud.CloneInstanceResponse cloneSnapshot(CloneSnapshotState cloneSnapshotState) throws Exception {
        try {
            Cloud.CloneInstanceRequest.Builder requestBuilder = Cloud.CloneInstanceRequest.newBuilder()
                    .setFromSnapshotId(cloneSnapshotState.getFromSnapshotId())
                    .setFromInstanceId(cloneSnapshotState.getFromInstanceId())
                    .setNewInstanceId(cloneSnapshotState.getInstanceId())
                    .setSnapshotName(cloneSnapshotState.getName())
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached());
            if (cloneSnapshotState.isSuccessor()) {
                requestBuilder.setCloneType(Cloud.CloneInstanceRequest.CloneType.ROLLBACK);
            } else if (cloneSnapshotState.isReadOnly()) {
                requestBuilder.setCloneType(Cloud.CloneInstanceRequest.CloneType.READ_ONLY);
            } else {
                requestBuilder.setCloneType(Cloud.CloneInstanceRequest.CloneType.WRITABLE)
                        .setObjInfo(cloneSnapshotState.getObjectStoreInfoPB());
                if (cloneSnapshotState.hasStorageVault()) {
                    requestBuilder.setStorageVault(cloneSnapshotState.getStorageVaultPB());
                }
            }
            Cloud.CloneInstanceResponse response = MetaServiceProxy.getInstance().cloneInstance(requestBuilder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("cloneInstance response: {} ", response);
                throw new DdlException(response.getStatus().getMsg());
            }
            return response;
        } catch (RpcException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private Pair<File, File> downloadImage(String snapshotId, Cloud.CloneInstanceResponse response) throws Exception {
        LOG.info("start to download snapshot id: {}, image url: {}", snapshotId, response.getImageUrl());
        // download zip file
        ObjectInfo objectInfo = new ObjectInfo(response.getObjInfo());
        try (ObjFileSystem fs = createObjFileSystem(objectInfo)) {
            String imageUrl = normalizeRelativePrefix(response.getImageUrl());
            String continuationToken = null;
            while (true) {
                RemoteObjects listObjectsResult = fs.listObjectsWithPrefix(
                        objectInfo.getPrefix(), imageUrl, continuationToken);
                for (RemoteObject objectFile : listObjectsResult.getObjectList()) {
                    String lastPart = objectFile.getKey().substring(objectFile.getKey().lastIndexOf("/") + 1);
                    String localPath = cloneSnapshotDir + lastPart;
                    LOG.info("download objectFile: {}  to local path: {}", objectFile.toString(), localPath);
                    FileSystemTransferUtil.download(fs,
                            Location.of(formatRemotePath(objectInfo.getBucket(), objectFile.getKey())),
                            new File(localPath).toPath(), objectFile.getSize());
                }
                if (!listObjectsResult.isTruncated()) {
                    break;
                }
                continuationToken = listObjectsResult.getContinuationToken();
            }
        }

        // check zip file and md5
        File dir = new File(this.cloneSnapshotDir);
        File[] files = dir.listFiles();
        if (files.length != 1) {
            LOG.error("clone snapshot directory: {} contains {} files, should only have 1 zip file",
                    dir.getAbsolutePath(), files.length);
            System.exit(-1);
        }
        File zipFile = files[0];
        if (!zipFile.getName().endsWith(".zip")) {
            LOG.error("clone snapshot file: {} is not a zip file", zipFile.getAbsolutePath());
            System.exit(-1);
        }
        compareFileMd5(zipFile, parseFileName(zipFile, 3, 1));

        // decompress zip file
        decompressZip(zipFile, this.cloneSnapshotDir);
        deleteFile(zipFile);

        // check image file, edit log file and md5
        files = dir.listFiles();
        if (files.length == 0 || files.length > 2) {
            LOG.error("clone snapshot directory: {} contains {} files", dir.getAbsolutePath(), files.length);
            System.exit(-1);
        }
        File imageFile = null;
        File editLogFile = null;
        for (File file : files) {
            if (file.getName().startsWith("image.")) {
                imageFile = file;
            } else {
                editLogFile = file;
            }
            compareFileMd5(file, parseFileName(file, 3, 2));
        }
        return Pair.of(imageFile, editLogFile);
    }

    private void loadSnapshotImage(File imageFile, File editLogFile) throws IOException, DdlException {
        try {
            CloudSnapshotEnv cloudSnapshotEnv = CloudSnapshotEnv.createAndGetInstance();
            // load image
            long imageJournalId = 0;
            if (imageFile != null) {
                imageJournalId = Long.parseLong(parseFileName(imageFile, 3, 1));
                MetaReader.read(imageFile, cloudSnapshotEnv);
                LOG.info("finished load image from cluster snapshot: {}, imageJournalId: {}",
                        imageFile.getAbsolutePath(), imageJournalId);
            }

            // replay edit log
            long replayedJournalId = imageJournalId;
            if (editLogFile != null) {
                DataInputStream currentStream = new DataInputStream(
                        new BufferedInputStream(new EditLogFileInputStream(editLogFile)));
                try {
                    while (true) {
                        JournalEntity entity = new JournalEntity();
                        entity.readFields(currentStream);
                        if (entity.getOpCode() == OperationType.OP_LOCAL_EOF) {
                            break;
                        }
                        replayedJournalId++;
                        EditLog.loadJournal(cloudSnapshotEnv, replayedJournalId, entity);
                    }
                } catch (IOException e) {
                    try {
                        currentStream.close();
                    } catch (IOException e1) {
                        LOG.error("failed to close cluster snapshot edit log", e1);
                    }
                    if (!(e instanceof EOFException)) {
                        LOG.error("failed to replay cluster snapshot edit log", e);
                        System.exit(-1);
                    }
                }
                LOG.info("finished replay edit logs from cluster snapshot: {}, replayedJournalId: {}",
                        editLogFile.getAbsolutePath(), replayedJournalId);
            }

            // generate new image
            cloudSnapshotEnv.setReplayedJournalId(replayedJournalId);
            String latestImageFilePath = cloudSnapshotEnv.saveImage();
            LOG.info("save image to {}, replayedJournalId: {}", latestImageFilePath, replayedJournalId);
        } finally {
            CloudSnapshotEnv.resetInstance();
            Preconditions.checkState(CloudSnapshotEnv.getInstance() == null);
        }
    }

    private void createDir(String dir) {
        truncateDir(dir);
        File directory = new File(dir);
        if (!directory.mkdir()) {
            LOG.error("failed to create directory: {}", directory.getAbsolutePath());
        }
    }

    private void truncateDir(String dir) {
        File directory = new File(dir);
        if (directory.exists()) {
            if (directory.isDirectory()) {
                for (File file : directory.listFiles()) {
                    deleteFile(file);
                }
            }
            deleteFile(directory);
        }
    }

    private void deleteFiles(File... files) {
        for (File file : files) {
            deleteFile(file);
        }
    }

    private void deleteFile(File file) {
        if (file == null) {
            return;
        }
        try {
            if (file.exists()) {
                if (file.delete()) {
                    LOG.info("delete file: {}", file.getAbsolutePath());
                } else {
                    LOG.warn("failed to delete file: {}", file.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to delete file: {}", file.getAbsolutePath(), e);
        }
    }

    private String formatRemoteKey(String prefix, String imageUrl, String fileName) {
        String newPrefix = prefix;
        if (prefix.endsWith("/")) {
            newPrefix = prefix.substring(0, prefix.length() - 1);
        }
        String newImageUrl = imageUrl;
        if (!newImageUrl.startsWith("/")) {
            newImageUrl = "/" + newImageUrl;
        }
        if (!newImageUrl.endsWith("/")) {
            newImageUrl = newImageUrl + "/";
        }
        return newPrefix + newImageUrl + fileName;
    }

    private String parseFileName(File file, int expectedParts, int returnPart) {
        String[] split = file.getName().split("\\.");
        if (split.length != expectedParts) {
            LOG.error("file name {} is invalid", file.getAbsolutePath());
            System.exit(-1);
        }
        return split[returnPart];
    }

    private void compareFileMd5(File file, String md5) throws IOException {
        String calculatedMd5 = calculateMd5(file);
        if (!md5.equals(calculatedMd5)) {
            LOG.error("file {} md5 is invalid, expected: {}, actual: {}", file.getAbsolutePath(), md5,
                    calculatedMd5);
            System.exit(-1);
        }
    }

    private String calculateMd5(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            return DigestUtils.md5Hex(fis);
        }
    }

    private ObjFileSystem createObjFileSystem(ObjectInfo objectInfo) throws IOException {
        StorageProperties storageProps = ObjectInfoAdapter.toStorageProperties(objectInfo);
        org.apache.doris.filesystem.FileSystem rawFs = FileSystemFactory.getFileSystem(storageProps);
        Preconditions.checkState(rawFs instanceof ObjFileSystem,
                "Snapshot operations require ObjFileSystem, but got: %s", rawFs.getClass().getSimpleName());
        return (ObjFileSystem) rawFs;
    }

    private void uploadFileByMultipart(ObjFileSystem fs, File localFile, String remotePath,
            Function<String, Pair<Boolean, String>> uploadIdRecorder) throws IOException {
        final int chunkSize = 5 * 1024 * 1024;
        String uploadId = fs.getObjStorage().initiateMultipartUpload(remotePath);
        Pair<Boolean, String> updateResult = uploadIdRecorder.apply(uploadId);
        if (!updateResult.first) {
            try {
                fs.getObjStorage().abortMultipartUpload(remotePath, uploadId);
            } catch (IOException abortException) {
                LOG.warn("failed to abort multipart upload, remotePath={}, uploadId={}",
                        remotePath, uploadId, abortException);
            }
            throw new IOException("failed to update snapshot upload id, reason=" + updateResult.second);
        }

        List<UploadPartResult> partResults = new ArrayList<>();
        byte[] buffer = new byte[chunkSize];
        int partNum = 1;
        try (FileInputStream inputStream = new FileInputStream(localFile)) {
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                partResults.add(fs.getObjStorage().uploadPart(remotePath, uploadId, partNum++,
                        RequestBody.of(new ByteArrayInputStream(buffer, 0, bytesRead), bytesRead)));
            }
            fs.getObjStorage().completeMultipartUpload(remotePath, uploadId, partResults);
        } catch (IOException e) {
            try {
                fs.getObjStorage().abortMultipartUpload(remotePath, uploadId);
            } catch (IOException abortException) {
                LOG.warn("failed to abort multipart upload after upload failure, remotePath={}, uploadId={}",
                        remotePath, uploadId, abortException);
            }
            throw e;
        }
    }

    private String normalizeRelativePrefix(String prefix) {
        String normalizedPrefix = prefix;
        if (normalizedPrefix.startsWith("/")) {
            normalizedPrefix = normalizedPrefix.substring(1);
        }
        if (!normalizedPrefix.isEmpty() && !normalizedPrefix.endsWith("/")) {
            normalizedPrefix = normalizedPrefix + "/";
        }
        return normalizedPrefix;
    }

    private String formatRemotePath(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }

    private File compressFiles(String snapshotId, List<File> sourceFiles) throws IOException {
        // the file name is: image.{version}.{md5}, edits.{logId}.{md5}, {snapshotId}.{md5}.zip
        String zipFileName = this.snapshotDir + snapshotId;
        try (FileOutputStream fos = new FileOutputStream(zipFileName);
                BufferedOutputStream bos = new BufferedOutputStream(fos);
                ZipOutputStream zos = new ZipOutputStream(bos)) {
            for (File fileToZip : sourceFiles) {
                if (!fileToZip.exists()) {
                    throw new IOException("source file does not exist: " + fileToZip.getAbsolutePath());
                }
                // calculate md5
                String md5 = calculateMd5(fileToZip);
                try (FileInputStream fis = new FileInputStream(fileToZip);
                        BufferedInputStream bis = new BufferedInputStream(fis)) {
                    ZipEntry zipEntry = new ZipEntry(fileToZip.getName() + "." + md5);
                    zos.putNextEntry(zipEntry);

                    byte[] buffer = new byte[1024];
                    int length;
                    while ((length = bis.read(buffer)) > 0) {
                        zos.write(buffer, 0, length);
                    }
                    zos.closeEntry();
                }
            }
        }
        File zipFile = new File(zipFileName);
        if (!zipFile.exists()) {
            throw new IOException("zip file does not exist: " + zipFile.getAbsoluteFile());
        }
        String md5 = calculateMd5(zipFile);
        File destFile = new File(zipFileName + "." + md5 + ".zip");
        zipFile.renameTo(destFile);
        return destFile;
    }

    private void decompressZip(File zipFile, String destDir) throws IOException {
        File dir = new File(destDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        try (FileInputStream fis = new FileInputStream(zipFile);
                BufferedInputStream bis = new BufferedInputStream(fis);
                ZipInputStream zis = new ZipInputStream(bis)) {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                String filePath = destDir + File.separator + zipEntry.getName();
                if (!zipEntry.isDirectory()) {
                    extractFile(zis, filePath);
                } else {
                    File dirToCreate = new File(filePath);
                    dirToCreate.mkdirs();
                }
                zis.closeEntry();
                zipEntry = zis.getNextEntry();
            }
        }
    }

    private void extractFile(ZipInputStream zis, String filePath) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
            byte[] buffer = new byte[1024];
            int read;
            while ((read = zis.read(buffer)) != -1) {
                bos.write(buffer, 0, read);
            }
        }
    }

    private long getSnapshotDataSize() {
        long snapshotDataSize = 0;
        List<String> dbNames = Env.getCurrentInternalCatalog().getDbNames();
        Preconditions.checkNotNull(dbNames);
        Map<Long, Pair<Long, Long>> dbToRecycleSize = Env.getCurrentRecycleBin().getDbToRecycleSize();
        for (String dbName : dbNames) {
            DatabaseIf db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
            if (db == null) {
                continue;
            }
            // Size, RemoteSize
            Pair<Long, Long> usedSize = ((Database) db).getUsedDataSize();
            snapshotDataSize += usedSize.first + usedSize.second;
            // RecycleSize, RecycleRemoteSize
            Pair<Long, Long> recycleSize = dbToRecycleSize.getOrDefault(db.getId(), Pair.of(0L, 0L));
            snapshotDataSize += recycleSize.first + recycleSize.second;
        }
        return snapshotDataSize;
    }
}
