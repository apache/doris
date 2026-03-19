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
import org.apache.doris.cloud.storage.ListObjectsResult;
import org.apache.doris.cloud.storage.ObjectFile;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.master.Checkpoint;
import org.apache.doris.common.CheckpointException;
import org.apache.doris.persist.Storage;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Production implementation of {@link CloudSnapshotHandler}.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #initialize()} — creates single-thread executor for snapshot workflows.</li>
 *   <li>{@link #runAfterCatalogReady()} — runs periodically on Master, triggers auto-snapshot
 *       and aborts timed-out PREPARE snapshots.</li>
 *   <li>{@link #submitJob(long, String)} — submits a manual snapshot (SQL command entry-point).</li>
 *   <li>{@link #refreshAutoSnapshotJob()} — reloads scheduling interval from MetaService.</li>
 *   <li>{@link #cloneSnapshot(String)} — restores FE from a cluster snapshot JSON file.</li>
 * </ol>
 */
public class DorisCloudSnapshotHandler extends CloudSnapshotHandler {

    private static final Logger LOG = LogManager.getLogger(DorisCloudSnapshotHandler.class);

    /**
     * Single-thread executor serializing all snapshot workflows.
     * Only one snapshot can be in progress at a time.
     */
    private ExecutorService snapshotExecutor;

    /**
     * Tracks the currently in-progress snapshot (null if idle).
     */
    private final AtomicReference<SnapshotState> currentSnapshot = new AtomicReference<>(null);

    // ============================================================================
    // Constructor (no-arg, required by reflection in CloudSnapshotHandler.getInstance)
    // ============================================================================

    public DorisCloudSnapshotHandler() {
        super();
    }

    // ============================================================================
    // Initialization
    // ============================================================================

    @Override
    public void initialize() {
        snapshotExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat("snapshot-worker-%d")
                        .setDaemon(true)
                        .build());
        LOG.info("DorisCloudSnapshotHandler initialized, interval={}s",
                Config.cloud_snapshot_handler_interval_second);
    }

    // ============================================================================
    // submitJob — manual snapshot entry point (called by AdminCreateClusterSnapshotCommand)
    // ============================================================================

    /**
     * Submit a snapshot job.  The actual workflow runs asynchronously in
     * {@link #snapshotExecutor} so that the SQL command returns immediately.
     *
     * @param ttl   snapshot time-to-live in seconds
     * @param label user-provided snapshot label
     */
    @Override
    public void submitJob(long ttl, String label) throws Exception {
        SnapshotState sentinel = new SnapshotState("pending_" + label, "");
        if (!currentSnapshot.compareAndSet(null, sentinel)) {
            SnapshotState existing = currentSnapshot.get();
            throw new DdlException(
                    "Another snapshot is already in progress: "
                            + (existing != null ? existing.getSnapshotId() : "unknown"));
        }

        snapshotExecutor.submit(() -> {
            try {
                executeWorkflow(ttl, label, false);
            } catch (Exception e) {
                LOG.error("Snapshot workflow failed for label={}", label, e);
            }
        });
        LOG.info("Snapshot job submitted, label={}, ttl={}s", label, ttl);
    }

    // ============================================================================
    // Core five-step workflow
    // ============================================================================

    /**
     * Executes the complete snapshot workflow:
     * <pre>
     * Step 1: beginSnapshot RPC         -> get snapshot_id, image_url, obj_info
     * Step 2: Create FE image (checkpoint) -> get local image file + journal_id
     * Step 3: Update snapshot with upload info (updateSnapshot RPC)
     * Step 4: Multipart-upload image to object storage
     * Step 5: commitSnapshot RPC          -> finalize
     * </pre>
     * Any failure triggers an abortSnapshot RPC.
     *
     * @param isAuto true for auto-snapshot, false for manual
     */
    private void executeWorkflow(long ttl, String label, boolean isAuto) {
        String snapshotId = null;
        try {
            // ---- Step 1: Begin Snapshot ----
            Cloud.BeginSnapshotRequest beginReq = Cloud.BeginSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setSnapshotLabel(label)
                    .setAutoSnapshot(isAuto)
                    .setTimeoutSeconds(Config.cloud_snapshot_timeout_seconds)
                    .setTtlSeconds(ttl)
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                    .build();
            Cloud.BeginSnapshotResponse beginResp =
                    MetaServiceProxy.getInstance().beginSnapshot(beginReq);
            if (beginResp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("beginSnapshot failed: {}", beginResp.getStatus().getMsg());
                if (isAuto) {
                    return;
                }
                throw new DdlException(
                        "beginSnapshot failed: " + beginResp.getStatus().getMsg());
            }
            snapshotId = beginResp.getSnapshotId();
            String imageUrl = beginResp.getImageUrl();
            Cloud.ObjectStoreInfoPB objInfo = beginResp.getObjInfo();

            currentSnapshot.set(new SnapshotState(snapshotId, imageUrl));
            LOG.info("{}beginSnapshot ok, snapshot_id={}, image_url={}",
                    isAuto ? "Auto " : "", snapshotId, imageUrl);

            // ---- Step 2: Create FE Image (Checkpoint) ----
            Pair<File, Long> imagePair = createFEImage();
            File imageFile = imagePair.first;
            long journalId = imagePair.second;
            long imageSize = imageFile.length();
            LOG.info("FE image created, path={}, journal_id={}, size={}",
                    imageFile.getAbsolutePath(), journalId, imageSize);

            // ---- Step 3: Upload image via multipart upload ----
            String objectKey = buildObjectKey(objInfo, imageUrl, imageFile.getName());
            RemoteBase.ObjectInfo remoteObjInfo = new RemoteBase.ObjectInfo(objInfo);
            RemoteBase remote = RemoteBase.newInstance(remoteObjInfo);
            try {
                final String snapshotIdFinal = snapshotId;
                remote.multipartUploadObject(imageFile, objectKey, (uploadId) -> {
                    try {
                        updateSnapshotUpload(snapshotIdFinal, objectKey, uploadId);
                        return Pair.of(true, "");
                    } catch (Exception e) {
                        LOG.warn("updateSnapshot failed during multipart upload", e);
                        return Pair.of(false, e.getMessage());
                    }
                });
            } finally {
                remote.close();
            }
            LOG.info("Image uploaded to object storage, key={}", objectKey);

            // ---- Step 4: Commit Snapshot ----
            Cloud.CommitSnapshotRequest commitReq = Cloud.CommitSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setSnapshotId(snapshotId)
                    .setImageUrl(imageUrl)
                    .setLastJournalId(journalId)
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                    .setSnapshotMetaImageSize(imageSize)
                    .setSnapshotLogicalDataSize(0)
                    .build();
            Cloud.CommitSnapshotResponse commitResp =
                    MetaServiceProxy.getInstance().commitSnapshot(commitReq);
            if (commitResp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                throw new DdlException(
                        (isAuto ? "Auto " : "")
                                + "commitSnapshot failed: "
                                + commitResp.getStatus().getMsg());
            }

            LOG.info("{}Snapshot committed, snapshot_id={}, label={}, journal_id={}",
                    isAuto ? "Auto " : "", snapshotId, label, journalId);
        } catch (Exception e) {
            LOG.error("{}Snapshot workflow error, attempting abort. label={}",
                    isAuto ? "Auto " : "", label, e);
            if (snapshotId != null) {
                abortSnapshot(snapshotId,
                        (isAuto ? "Auto w" : "W") + "orkflow failed: " + e.getMessage());
            }
        } finally {
            currentSnapshot.set(null);
        }
    }

    /**
     * Create a fresh FE image by saving the current serving Env state.
     * Returns the image file and the replayed journal ID.
     */
    private Pair<File, Long> createFEImage() throws Exception {
        Checkpoint checkpoint = Env.getCurrentEnv().getCheckpoint();
        if (checkpoint == null) {
            throw new DdlException("Checkpoint service is not initialized");
        }
        try {
            checkpoint.doCheckpoint();
        } catch (CheckpointException e) {
            throw new DdlException("Failed to run FE checkpoint before snapshot", e);
        }

        String imageDir = Config.meta_dir + "/image";
        Storage storage = new Storage(imageDir);
        long latestSeq = storage.getLatestImageSeq();
        File imageFile = storage.getImageFile(latestSeq);

        if (!imageFile.exists()) {
            throw new IOException("Latest FE image file not found: " + imageFile.getAbsolutePath());
        }

        return Pair.of(imageFile, latestSeq);
    }

    /**
     * Build the full object key for uploading image.
     * The key combines the obj_info prefix, the image_url, and the file name.
     */
    private String buildObjectKey(
            Cloud.ObjectStoreInfoPB objInfo, String imageUrl, String fileName) {
        String prefix = objInfo.hasPrefix() ? objInfo.getPrefix() : "";
        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }
        String url = imageUrl.endsWith("/") ? imageUrl : imageUrl + "/";
        return prefix + url + fileName;
    }

    /**
     * Notify MetaService about the multipart upload ID for crash recovery.
     */
    private void updateSnapshotUpload(String snapshotId, String uploadFile, String uploadId)
            throws RpcException, DdlException {
        Cloud.UpdateSnapshotRequest req = Cloud.UpdateSnapshotRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setSnapshotId(snapshotId)
                .setUploadFile(uploadFile)
                .setUploadId(uploadId)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .build();
        Cloud.UpdateSnapshotResponse resp = MetaServiceProxy.getInstance().updateSnapshot(req);
        if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException("updateSnapshot failed: " + resp.getStatus().getMsg());
        }
        LOG.info("updateSnapshot ok, snapshot_id={}, upload_file={}, upload_id={}",
                snapshotId, uploadFile, uploadId);
    }

    /**
     * Send an abort RPC to clean up a failed snapshot.
     */
    private void abortSnapshot(String snapshotId, String reason) {
        try {
            Cloud.AbortSnapshotRequest req = Cloud.AbortSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setSnapshotId(snapshotId)
                    .setReason(reason)
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                    .build();
            Cloud.AbortSnapshotResponse resp = MetaServiceProxy.getInstance().abortSnapshot(req);
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("abortSnapshot failed: snapshot_id={}, msg={}",
                        snapshotId, resp.getStatus().getMsg());
            } else {
                LOG.info("abortSnapshot ok, snapshot_id={}, reason={}", snapshotId, reason);
            }
        } catch (Exception e) {
            LOG.warn("abortSnapshot RPC exception, snapshot_id={}", snapshotId, e);
        }
    }

    // ============================================================================
    // runAfterCatalogReady — auto-snapshot scheduling (called periodically by Daemon)
    // ============================================================================

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }

        try {
            // 1. Get instance info from MetaService to read snapshot properties
            Cloud.InstanceInfoPB instanceInfo = getInstanceInfo();
            if (instanceInfo == null) {
                return;
            }

            // 2. Check snapshot switch — must be ON
            if (!instanceInfo.hasSnapshotSwitchStatus()
                    || instanceInfo.getSnapshotSwitchStatus()
                    != Cloud.SnapshotSwitchStatus.SNAPSHOT_SWITCH_ON) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Snapshot switch is not ON, skipping auto-snapshot check");
                }
                return;
            }

            // 3. List current snapshots (exclude aborted)
            Cloud.ListSnapshotResponse listResp = listSnapshot(false);
            List<Cloud.SnapshotInfoPB> normalSnapshots = new ArrayList<>();
            for (Cloud.SnapshotInfoPB s : listResp.getSnapshotsList()) {
                if (s.getStatus() == Cloud.SnapshotStatus.SNAPSHOT_NORMAL) {
                    normalSnapshots.add(s);
                }
            }

            // 4. Abort timed-out PREPARE snapshots
            long now = System.currentTimeMillis() / 1000;
            Cloud.ListSnapshotResponse fullListResp = listSnapshot(true);
            for (Cloud.SnapshotInfoPB s : fullListResp.getSnapshotsList()) {
                if (s.getStatus() == Cloud.SnapshotStatus.SNAPSHOT_PREPARE) {
                    long timeout = s.hasTimeoutSeconds() ? s.getTimeoutSeconds()
                            : Config.cloud_snapshot_timeout_seconds;
                    if (now > s.getCreateAt() + timeout) {
                        LOG.info("Aborting timed-out PREPARE snapshot: id={}, created_at={}, timeout={}",
                                s.getSnapshotId(), s.getCreateAt(), timeout);
                        abortSnapshot(s.getSnapshotId(), "PREPARE timed out (auto-cleanup)");
                    }
                }
            }

            // 5. Determine snapshot interval
            long interval = instanceInfo.hasSnapshotIntervalSeconds()
                    ? instanceInfo.getSnapshotIntervalSeconds()
                    : Config.cloud_auto_snapshot_min_interval_seconds;

            // 6. Decide whether to create a new snapshot
            boolean shouldCreate = false;
            if (normalSnapshots.isEmpty()) {
                shouldCreate = true;
                LOG.info("No NORMAL snapshots exist, triggering auto-snapshot");
            } else {
                normalSnapshots.sort(Comparator.comparingLong(Cloud.SnapshotInfoPB::getFinishAt).reversed());
                Cloud.SnapshotInfoPB latest = normalSnapshots.get(0);
                long elapsed = now - latest.getFinishAt();
                if (elapsed >= interval) {
                    shouldCreate = true;
                    LOG.info("Auto-snapshot interval elapsed: {}s >= {}s, triggering", elapsed, interval);
                }
            }

            if (shouldCreate) {
                // 7. Recycle old snapshots if exceeding max_reserved
                long maxReserved = instanceInfo.hasMaxReservedSnapshot()
                        ? instanceInfo.getMaxReservedSnapshot()
                        : Config.cloud_auto_snapshot_max_reserved_num;
                if (maxReserved > 0 && normalSnapshots.size() >= maxReserved) {
                    normalSnapshots.sort(Comparator.comparingLong(Cloud.SnapshotInfoPB::getCreateAt));
                    int toRecycle = (int) (normalSnapshots.size() - maxReserved + 1);
                    for (int i = 0; i < toRecycle && i < normalSnapshots.size(); i++) {
                        String sid = normalSnapshots.get(i).getSnapshotId();
                        try {
                            dropOldSnapshot(sid);
                            LOG.info("Dropped old snapshot to stay within max_reserved: {}", sid);
                        } catch (Exception e) {
                            LOG.warn("Failed to drop old snapshot: {}", sid, e);
                        }
                    }
                }

                // 8. Submit auto-snapshot job
                try {
                    long autoTtl = Config.cloud_auto_snapshot_min_interval_seconds;
                    String autoLabel = "auto_snapshot_" + now;
                    submitAutoJob(autoTtl, autoLabel);
                } catch (Exception e) {
                    LOG.warn("Failed to submit auto snapshot job", e);
                }
            }
        } catch (Exception e) {
            LOG.warn("Error in auto-snapshot check", e);
        }
    }

    /**
     * Submit an auto-snapshot job. Similar to submitJob but marks auto_snapshot=true.
     */
    private void submitAutoJob(long ttl, String label) throws Exception {
        SnapshotState sentinel = new SnapshotState("pending_auto_" + label, "");
        if (!currentSnapshot.compareAndSet(null, sentinel)) {
            LOG.info("Skipping auto-snapshot, another snapshot in progress: {}",
                    currentSnapshot.get() != null
                            ? currentSnapshot.get().getSnapshotId() : "unknown");
            return;
        }

        snapshotExecutor.submit(() -> {
            try {
                executeWorkflow(ttl, label, true);
            } catch (Exception e) {
                LOG.error("Auto snapshot workflow failed, label={}", label, e);
            }
        });
        LOG.info("Auto snapshot job submitted, label={}, ttl={}s", label, ttl);
    }

    // ============================================================================
    // refreshAutoSnapshotJob
    // ============================================================================

    /**
     * Reload auto-snapshot scheduling configuration from MetaService and adjust the
     * daemon interval accordingly.
     */
    @Override
    public synchronized void refreshAutoSnapshotJob() throws Exception {
        Cloud.InstanceInfoPB instanceInfo = getInstanceInfo();
        if (instanceInfo == null) {
            LOG.warn("refreshAutoSnapshotJob: cannot get instance info");
            return;
        }

        if (instanceInfo.hasSnapshotIntervalSeconds() && instanceInfo.getSnapshotIntervalSeconds() > 0) {
            long newIntervalMs = instanceInfo.getSnapshotIntervalSeconds() * 1000L;
            if (newIntervalMs != getInterval()) {
                LOG.info("Updating snapshot handler interval from {}ms to {}ms", getInterval(), newIntervalMs);
                setInterval(newIntervalMs);
            }
        }

        LOG.info("refreshAutoSnapshotJob done, switch={}, max_reserved={}, interval={}",
                instanceInfo.hasSnapshotSwitchStatus() ? instanceInfo.getSnapshotSwitchStatus() : "N/A",
                instanceInfo.hasMaxReservedSnapshot() ? instanceInfo.getMaxReservedSnapshot() : "N/A",
                instanceInfo.hasSnapshotIntervalSeconds() ? instanceInfo.getSnapshotIntervalSeconds() : "N/A");
    }

    // ============================================================================
    // cloneSnapshot — restore FE from a cluster snapshot
    // ============================================================================

    /**
     * Restore FE metadata from a cluster snapshot.
     *
     * <p>The {@code clusterSnapshotFile} is a JSON file with fields:
     * <ul>
     *   <li>{@code snapshot_id} — the snapshot to clone from</li>
     *   <li>{@code from_instance_id} — source instance ID</li>
     *   <li>{@code new_instance_id} — target instance ID for the clone</li>
     *   <li>{@code clone_type} — "READ_ONLY", "WRITABLE", or "ROLLBACK"</li>
     * </ul>
     *
     * <p>Flow:
     * <ol>
     *   <li>Parse JSON file</li>
     *   <li>Call {@code cloneInstance} RPC</li>
     *   <li>Download FE image from object storage to local image directory</li>
     * </ol>
     */
    @Override
    public void cloneSnapshot(String clusterSnapshotFile) throws Exception {
        LOG.info("cloneSnapshot from file: {}", clusterSnapshotFile);

        // 1. Parse the cluster snapshot JSON file
        String jsonContent = new String(Files.readAllBytes(Paths.get(clusterSnapshotFile)), StandardCharsets.UTF_8);
        JsonObject json = JsonParser.parseString(jsonContent).getAsJsonObject();

        String snapshotId = getJsonString(json, "snapshot_id");
        String fromInstanceId = getJsonString(json, "from_instance_id");
        String newInstanceId = getJsonString(json, "new_instance_id");
        String cloneTypeStr = getJsonString(json, "clone_type");

        if (snapshotId == null || fromInstanceId == null || newInstanceId == null) {
            throw new DdlException("cloneSnapshot: missing required fields in JSON file: "
                    + "snapshot_id, from_instance_id, new_instance_id");
        }

        Cloud.CloneInstanceRequest.CloneType cloneType = parseCloneType(cloneTypeStr);

        // 2. Build CloneInstance RPC request
        Cloud.CloneInstanceRequest.Builder reqBuilder = Cloud.CloneInstanceRequest.newBuilder()
                .setFromSnapshotId(snapshotId)
                .setFromInstanceId(fromInstanceId)
                .setNewInstanceId(newInstanceId)
                .setCloneType(cloneType)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached());

        // For WRITABLE clone, obj_info and storage_vault may be in the JSON
        if (json.has("obj_info")) {
            // Parse obj_info from JSON and set on request
            JsonObject objInfoJson = json.getAsJsonObject("obj_info");
            Cloud.ObjectStoreInfoPB.Builder objBuilder = Cloud.ObjectStoreInfoPB.newBuilder();
            if (objInfoJson.has("ak")) {
                objBuilder.setAk(objInfoJson.get("ak").getAsString());
            }
            if (objInfoJson.has("sk")) {
                objBuilder.setSk(objInfoJson.get("sk").getAsString());
            }
            if (objInfoJson.has("bucket")) {
                objBuilder.setBucket(objInfoJson.get("bucket").getAsString());
            }
            if (objInfoJson.has("endpoint")) {
                objBuilder.setEndpoint(objInfoJson.get("endpoint").getAsString());
            }
            if (objInfoJson.has("region")) {
                objBuilder.setRegion(objInfoJson.get("region").getAsString());
            }
            if (objInfoJson.has("prefix")) {
                objBuilder.setPrefix(objInfoJson.get("prefix").getAsString());
            }
            reqBuilder.setObjInfo(objBuilder.build());
        }

        Cloud.CloneInstanceResponse resp = MetaServiceProxy.getInstance().cloneInstance(reqBuilder.build());
        if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException("cloneInstance failed: " + resp.getStatus().getMsg());
        }

        LOG.info("cloneInstance succeeded, downloading image...");

        // 3. Download FE Image from object storage
        Cloud.ObjectStoreInfoPB objInfo = resp.getObjInfo();
        String imageUrl = resp.getImageUrl();

        if (!objInfo.hasBucket() || !objInfo.hasEndpoint()) {
            throw new DdlException("cloneSnapshot: invalid obj_info in cloneInstance response");
        }

        downloadSnapshotImage(objInfo, imageUrl);

        LOG.info("cloneSnapshot completed successfully, image saved to local");
    }

    /**
     * Download the snapshot FE image from object storage to the local image directory.
     */
    private void downloadSnapshotImage(Cloud.ObjectStoreInfoPB objInfo, String imageUrl) throws Exception {
        RemoteBase.ObjectInfo remoteObjInfo = new RemoteBase.ObjectInfo(objInfo);
        RemoteBase remote = RemoteBase.newInstance(remoteObjInfo);
        try {
            // List files in the image URL prefix to find the actual image file
            String prefix = imageUrl.endsWith("/") ? imageUrl : imageUrl + "/";
            ListObjectsResult listing = remote.listObjects(prefix, null);

            if (listing.getObjectInfoList().isEmpty()) {
                throw new DdlException("No image files found at: " + prefix);
            }

            // Download the first (and usually only) image file
            ObjectFile objectFile = listing.getObjectInfoList().get(0);
            String localImageDir = Config.meta_dir + "/image";
            File localDir = new File(localImageDir);
            if (!localDir.exists() && !localDir.mkdirs()) {
                throw new IOException("Cannot create image directory: " + localImageDir);
            }

            // Extract file name and determine local path
            String remoteKey = objectFile.getKey();
            String fileName = remoteKey.substring(remoteKey.lastIndexOf('/') + 1);
            String localPath = localImageDir + "/" + fileName;

            remote.getObject(remoteKey, localPath);
            LOG.info("Downloaded snapshot image: remote={}, local={}", remoteKey, localPath);
        } finally {
            remote.close();
        }
    }

    // ============================================================================
    // Helper methods
    // ============================================================================

    /**
     * Get snapshot properties from MetaService via GetInstance RPC.
     */
    private Cloud.InstanceInfoPB getInstanceInfo() {
        try {
            Cloud.GetInstanceRequest req = Cloud.GetInstanceRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                    .build();
            Cloud.GetInstanceResponse resp = MetaServiceProxy.getInstance().getInstance(req);
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("getInstance failed: {}", resp.getStatus().getMsg());
                return null;
            }
            return resp.getInstance();
        } catch (Exception e) {
            LOG.warn("getInstance RPC error", e);
            return null;
        }
    }

    /**
     * Drop a snapshot by ID (used for auto-cleanup of old snapshots).
     */
    private void dropOldSnapshot(String snapshotId) throws DdlException {
        try {
            Cloud.DropSnapshotRequest req = Cloud.DropSnapshotRequest.newBuilder()
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .setSnapshotId(snapshotId)
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                    .build();
            Cloud.DropSnapshotResponse resp = MetaServiceProxy.getInstance().dropSnapshot(req);
            if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                throw new DdlException("dropSnapshot failed: " + resp.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException("dropSnapshot RPC error: " + e.getMessage());
        }
    }

    /**
     * Parse clone type string to proto enum.
     */
    private Cloud.CloneInstanceRequest.CloneType parseCloneType(String typeStr) {
        if (typeStr == null || typeStr.isEmpty()) {
            return Cloud.CloneInstanceRequest.CloneType.READ_ONLY;
        }
        switch (typeStr.toUpperCase()) {
            case "READ_ONLY":
                return Cloud.CloneInstanceRequest.CloneType.READ_ONLY;
            case "WRITABLE":
                return Cloud.CloneInstanceRequest.CloneType.WRITABLE;
            case "ROLLBACK":
                return Cloud.CloneInstanceRequest.CloneType.ROLLBACK;
            default:
                LOG.warn("Unknown clone type '{}', defaulting to READ_ONLY", typeStr);
                return Cloud.CloneInstanceRequest.CloneType.READ_ONLY;
        }
    }

    /**
     * Safely get a string field from JsonObject.
     */
    private String getJsonString(JsonObject json, String key) {
        JsonElement elem = json.get(key);
        if (elem == null || elem.isJsonNull()) {
            return null;
        }
        return elem.getAsString();
    }
}
