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

package org.apache.doris.backup;

import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.foundation.fs.FsStorageType;
import org.apache.doris.fs.FileSystemDescriptor;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/*
 * Repository represents a remote storage for backup to or restore from
 * File organization in repository is:
 *
 * * __palo_repository_repo_name/
 *   * __repo_info
 *   * __ss_my_ss1/
 *     * __meta__DJdwnfiu92n
 *     * __info_2018-01-01-08-00-00.OWdn90ndwpu
 *     * __info_2018-01-02-08-00-00.Dnvdio298da
 *     * __info_2018-01-03-08-00-00.O79adbneJWk
 *     * __ss_content/
 *       * __db_10001/
 *         * __tbl_10010/
 *         * __tbl_10020/
 *           * __part_10021/
 *           * __part_10031/
 *             * __idx_10041/
 *             * __idx_10020/
 *               * __10022/
 *               * __10023/
 *                 * __10023_seg1.dat.NUlniklnwDN67
 *                 * __10023_seg2.dat.DNW231dnklawd
 *                 * __10023.hdr.dnmwDDWI92dDko
 */
public class Repository implements Writable, GsonPostProcessable {
    public static final String PREFIX_REPO = "__palo_repository_";
    public static final String PREFIX_SNAPSHOT_DIR = "__ss_";
    public static final String PREFIX_DB = "__db_";
    public static final String PREFIX_TBL = "__tbl_";
    public static final String PREFIX_PART = "__part_";
    public static final String PREFIX_IDX = "__idx_";
    public static final String PREFIX_COMMON = "__";
    public static final String PREFIX_JOB_INFO = "__info_";
    public static final String SUFFIX_TMP_FILE = "part";
    public static final String FILE_REPO_INFO = "__repo_info";
    public static final String FILE_META_INFO = "__meta";
    public static final String DIR_SNAPSHOT_CONTENT = "__ss_content";
    public static final String KEEP_ON_LOCAL_REPO_NAME = "__keep_on_local__";
    public static final long KEEP_ON_LOCAL_REPO_ID = -1;
    private static final Logger LOG = LogManager.getLogger(Repository.class);
    private static final String PATH_DELIMITER = "/";
    private static final String CHECKSUM_SEPARATOR = ".";

    @SerializedName("id")
    private long id;
    @SerializedName("n")
    private String name;
    private String errMsg;
    @SerializedName("ct")
    private long createTime;

    // If True, user can not backup data to this repo.
    @SerializedName("iro")
    private boolean isReadOnly;

    // BOS location should start with "bos://your_bucket_name/"
    // and the specified bucket should exist.
    @SerializedName("lo")
    private String location;

    /** New field: lightweight descriptor used for new metadata serialization. */
    @SerializedName("fs_descriptor")
    private FileSystemDescriptor fileSystemDescriptor;

    /**
     * Legacy field: used by Doris versions prior to the filesystem SPI refactoring.
     * Retained read-only for backward-compatible deserialization — never written in new code.
     * In old JSON this was serialized as {@code "fs": {"n": "...", "prop": {...}}}.
     */
    @SerializedName("fs")
    private LegacyFsRecord legacyFs;

    /** Minimal POJO that captures the old {@code PersistentFileSystem} serialization shape. */
    private static class LegacyFsRecord {
        @SerializedName("n")
        String name;
        @SerializedName("prop")
        Map<String, String> properties;
    }

    /** SPI filesystem for I/O operations; transient — rebuilt in {@link #gsonPostProcess()}.
     *  Null for BROKER repositories, which resolve a live broker endpoint per I/O call. */
    private transient org.apache.doris.filesystem.FileSystem spiFs;

    public FileSystemDescriptor getFileSystemDescriptor() {
        return fileSystemDescriptor;
    }

    private Repository() {
        // for persist
    }

    public Repository(long id, String name, boolean isReadOnly, String location,
            StorageProperties storageProperties) {
        this.id = id;
        this.name = name;
        this.isReadOnly = isReadOnly;
        this.location = location;
        String fsName = (storageProperties instanceof BrokerProperties)
                ? ((BrokerProperties) storageProperties).getBrokerName() : "";
        this.fileSystemDescriptor = FileSystemDescriptor.fromStorageProperties(storageProperties, fsName);
        this.createTime = System.currentTimeMillis();
        // Initialize SPI filesystem for I/O; broker resolves a live endpoint per I/O call
        if (fileSystemDescriptor.getStorageType() != FsStorageType.BROKER
                && !FeConstants.runningUnitTest) {
            try {
                this.spiFs = FileSystemFactory.getFileSystem(storageProperties);
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to initialize SPI filesystem for repository '" + name + "': " + e.getMessage(), e);
            }
        }
    }


    // eg: __info_2018-01-01-08-00-00
    private static String jobInfoFileNameWithTimestamp(long createTime) {
        if (createTime == -1) {
            return PREFIX_JOB_INFO;
        } else {
            return PREFIX_JOB_INFO
                    + TimeUtils.longToTimeString(createTime, TimeUtils.getDatetimeFormatWithHyphenWithTimeZone());
        }
    }

    // join the name with specified prefix
    private static String joinPrefix(String prefix, Object name) {
        return prefix + name;
    }

    // disjoint the name with specified prefix
    private static String disjoinPrefix(String prefix, String nameWithPrefix) {
        return nameWithPrefix.substring(prefix.length());
    }

    private static String assembleFileNameWithSuffix(String filePath, String md5sum) {
        return filePath + CHECKSUM_SEPARATOR + md5sum;
    }

    public static Pair<String, String> decodeFileNameWithChecksum(String fileNameWithChecksum) {
        int index = fileNameWithChecksum.lastIndexOf(CHECKSUM_SEPARATOR);
        if (index == -1) {
            return null;
        }
        String fileName = fileNameWithChecksum.substring(0, index);
        String md5sum = fileNameWithChecksum.substring(index + CHECKSUM_SEPARATOR.length());

        if (md5sum.length() != 32) {
            return null;
        }

        return Pair.of(fileName, md5sum);
    }

    // in: /path/to/orig_file
    // out: /path/to/orig_file.BUWDnl831e4nldsf
    public static String replaceFileNameWithChecksumFileName(String origPath, String fileNameWithChecksum) {
        return origPath.substring(0, origPath.lastIndexOf(PATH_DELIMITER) + 1) + fileNameWithChecksum;
    }

    public static Repository read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), Repository.class);
    }

    @Override
    public void gsonPostProcess() {
        // Determine source of properties: prefer new descriptor, fall back to legacy field.
        Map<String, String> fsProps;
        if (fileSystemDescriptor != null) {
            fsProps = fileSystemDescriptor.getProperties();
        } else if (legacyFs != null) {
            // Backward compatibility: upgrade from pre-SPI serialization format.
            // The old "fs" field used PersistentFileSystem with "n" (name) and "prop" (properties).
            LOG.info("Repository '{}': migrating legacy 'fs' field to 'fs_descriptor'", name);
            Map<String, String> props = legacyFs.properties != null ? legacyFs.properties : new HashMap<>();
            String fsName = legacyFs.name != null ? legacyFs.name : "";
            try {
                StorageProperties storageProperties = StorageProperties.createPrimary(props);
                fileSystemDescriptor = FileSystemDescriptor.fromStorageProperties(storageProperties, "");
            } catch (RuntimeException e) {
                LOG.warn("Repository '{}': primary storage migration failed ({}), trying broker fallback",
                        name, e.getMessage());
                try {
                    BrokerProperties brokerProperties = BrokerProperties.of(fsName, props);
                    fileSystemDescriptor = FileSystemDescriptor.fromStorageProperties(brokerProperties, fsName);
                } catch (RuntimeException e2) {
                    LOG.error("Repository '{}': failed to migrate legacy filesystem metadata: {}",
                            name, e2.getMessage());
                    return;
                }
            }
            fsProps = fileSystemDescriptor.getProperties();
        } else {
            LOG.error("Repository '{}': metadata corrupt — both 'fs' and 'fs_descriptor' fields are missing", name);
            return;
        }
        // Initialize SPI filesystem for I/O; broker resolves a live endpoint per I/O call
        if (fileSystemDescriptor.getStorageType() != FsStorageType.BROKER) {
            try {
                this.spiFs = FileSystemFactory.getFileSystem(StorageProperties.createPrimary(fsProps));
            } catch (IOException | RuntimeException e) {
                LOG.warn("Failed to initialize SPI filesystem for repository {}: {}", name, e.getMessage());
            }
        }
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public String getLocation() {
        if (fileSystemDescriptor == null
                || fileSystemDescriptor.getStorageType() == FsStorageType.BROKER) {
            return location;
        }
        try {
            return StorageProperties.createPrimary(fileSystemDescriptor.getProperties())
                    .validateAndNormalizeUri(location);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    public String getErrorMsg() {
        return errMsg;
    }

    /**
     * Acquires an SPI FileSystem for I/O operations.
     * <ul>
     *   <li>Non-broker: returns the shared {@link #spiFs} instance (do not close it).</li>
     *   <li>Broker: resolves a live broker endpoint via BrokerMgr and creates a per-call
     *       instance that <b>must</b> be closed by calling {@link #releaseSpiFs}.</li>
     * </ul>
     */
    private org.apache.doris.filesystem.FileSystem acquireSpiFs() throws IOException {
        if (spiFs != null) {
            return spiFs;
        }
        if (fileSystemDescriptor.getStorageType() != FsStorageType.BROKER) {
            // spiFs should have been initialized in the constructor or gsonPostProcess.
            // If it is null here, initialization failed silently during deserialization.
            throw new IOException("Repository '" + name + "' filesystem is not available — "
                    + "SPI filesystem failed to initialize during metadata load. "
                    + "Check the prior WARN log for the root cause.");
        }
        // Broker: resolve live endpoint and create a per-call filesystem
        try {
            BrokerProperties bp = BrokerProperties.of(fileSystemDescriptor.getName(),
                    fileSystemDescriptor.getProperties());
            FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getBroker(fileSystemDescriptor.getName(),
                    FrontendOptions.getLocalHostAddress());
            String clientId = NetUtils.getHostPortInAccessibleFormat(
                    FrontendOptions.getLocalHostAddress(), Config.edit_log_port);
            return FileSystemFactory.getBrokerFileSystem(broker.host, broker.port, clientId,
                    bp.getBrokerParams());
        } catch (AnalysisException e) {
            throw new IOException("Failed to acquire broker filesystem for repository " + name
                    + ": " + e.getMessage(), e);
        }
    }

    /**
     * Releases an SPI FileSystem acquired via {@link #acquireSpiFs}.
     * Closes broker per-call instances; leaves the shared non-broker instance open.
     */
    private void releaseSpiFs(org.apache.doris.filesystem.FileSystem fs) {
        if (fs != spiFs) {
            try {
                fs.close();
            } catch (IOException e) {
                LOG.warn("Failed to close broker filesystem for repository {}", name, e);
            }
        }
    }

    /** Uploads a local file to a remote path via the SPI filesystem. */
    private static void spiUploadFile(org.apache.doris.filesystem.FileSystem fs,
            String localFilePath, String remotePath) throws IOException {
        DorisOutputFile outputFile = fs.newOutputFile(Location.of(remotePath));
        try (java.io.InputStream in = Files.newInputStream(Paths.get(localFilePath));
                java.io.OutputStream out = outputFile.create()) {
            copyStream(in, out);
        }
    }

    /** Copies all bytes from {@code in} to {@code out} (Java-8-compatible alternative to transferTo). */
    private static void copyStream(java.io.InputStream in, java.io.OutputStream out) throws IOException {
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) >= 0) {
            out.write(buf, 0, n);
        }
    }

    public long getCreateTime() {
        return createTime;
    }

    // create repository dir and repo info file
    public Status initRepository() {
        if (FeConstants.runningUnitTest) {
            return Status.OK;
        }

        String repoInfoFilePath = assembleRepoInfoFilePath();
        org.apache.doris.filesystem.FileSystem fs;
        try {
            fs = acquireSpiFs();
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to acquire filesystem: " + e.getMessage());
        }
        try {
            if (fs.exists(Location.of(repoInfoFilePath))) {
                // Repo info file exists: download and parse it
                String localFilePath = BackupHandler.BACKUP_ROOT_DIR + "/tmp_info_" + allocLocalFileSuffix();
                try {
                    DorisInputFile inputFile = fs.newInputFile(Location.of(repoInfoFilePath));
                    try (java.io.InputStream in = inputFile.newStream();
                            java.io.OutputStream localOut = Files.newOutputStream(Paths.get(localFilePath))) {
                        copyStream(in, localOut);
                    }
                    byte[] bytes = Files.readAllBytes(Paths.get(localFilePath));
                    String json = new String(bytes, StandardCharsets.UTF_8);
                    JSONObject root = (JSONObject) JSONValue.parse(json);
                    if (name.compareTo((String) root.get("name")) != 0) {
                        return new Status(ErrCode.COMMON_ERROR,
                                "Invalid repository __repo_info, expected repo '" + name + "', but get name '"
                                        + root.get("name") + "' from " + repoInfoFilePath);
                    }
                    name = (String) root.get("name");
                    createTime = TimeUtils.timeStringToLong((String) root.get("create_time"));
                    if (createTime == -1) {
                        return new Status(ErrCode.COMMON_ERROR,
                                "failed to parse create time of repository: " + root.get("create_time"));
                    }
                    return Status.OK;
                } catch (IOException e) {
                    return new Status(ErrCode.COMMON_ERROR, "failed to read repo info file: " + e.getMessage());
                } finally {
                    new File(localFilePath).delete();
                }
            } else {
                // Repo doesn't exist yet: create the info file
                JSONObject root = new JSONObject();
                root.put("name", name);
                root.put("create_time", TimeUtils.longToTimeString(createTime));
                String repoInfoContent = root.toString();
                DorisOutputFile outputFile = fs.newOutputFile(Location.of(repoInfoFilePath));
                try (java.io.OutputStream out = outputFile.create()) {
                    out.write(repoInfoContent.getBytes(StandardCharsets.UTF_8));
                }
                return Status.OK;
            }
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to init repository: " + e.getMessage());
        } finally {
            releaseSpiFs(fs);
        }
    }

    // eg: location/__palo_repository_repo_name/__repo_info
    public String assembleRepoInfoFilePath() {
        return Joiner.on(PATH_DELIMITER).join(getLocation(),
                joinPrefix(PREFIX_REPO, name),
                FILE_REPO_INFO);
    }

    // eg: location/__palo_repository_repo_name/__my_sp1/__meta
    public String assembleMetaInfoFilePath(String label) {
        return Joiner.on(PATH_DELIMITER).join(getLocation(), joinPrefix(PREFIX_REPO, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                FILE_META_INFO);
    }

    // eg: location/__palo_repository_repo_name/__my_sp1/__info_2018-01-01-08-00-00
    public String assembleJobInfoFilePath(String label, long createTime) {
        return Joiner.on(PATH_DELIMITER).join(getLocation(), joinPrefix(PREFIX_REPO, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                jobInfoFileNameWithTimestamp(createTime));
    }

    // eg:
    // __palo_repository_repo_name/__ss_my_ss1/__ss_content/__db_10001/__tbl_10020/__part_10031/__idx_10020/__10022/
    public String getRepoTabletPathBySnapshotInfo(String label, SnapshotInfo info) {
        String path = Joiner.on(PATH_DELIMITER).join(getLocation(), joinPrefix(PREFIX_REPO, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                DIR_SNAPSHOT_CONTENT,
                joinPrefix(PREFIX_DB, info.getDbId()),
                joinPrefix(PREFIX_TBL, info.getTblId()),
                joinPrefix(PREFIX_PART, info.getPartitionId()),
                joinPrefix(PREFIX_IDX, info.getIndexId()),
                joinPrefix(PREFIX_COMMON, info.getTabletId()));
        try {
            // we need to normalize the path to avoid double "/" in path, or else some client such as S3 sdk can not
            // handle it correctly.
            return new URI(path).normalize().toString();
        } catch (URISyntaxException e) {
            LOG.warn("failed to normalize path: {}", path, e);
            return null;
        }
    }

    public String getRepoPath(String label, String childPath) {
        String path = Joiner.on(PATH_DELIMITER).join(getLocation(), joinPrefix(PREFIX_REPO, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                DIR_SNAPSHOT_CONTENT,
                childPath);
        try {
            URI uri = new URI(path);
            return uri.normalize().toString();
        } catch (Exception e) {
            LOG.warn("Invalid path: " + path, e);
            return null;
        }
    }

    // Check if this repo is available.
    // If failed to connect this repo, set errMsg and return false.
    public boolean ping() {
        if (FeConstants.runningUnitTest) {
            return true;
        }
        // for s3 sdk, the headObject() method does not support list "dir",
        // so we check FILE_REPO_INFO instead.
        String path = location + "/" + joinPrefix(PREFIX_REPO, name) + "/" + FILE_REPO_INFO;
        try {
            URI checkUri = new URI(path);
            org.apache.doris.filesystem.FileSystem fs = acquireSpiFs();
            try {
                boolean exists = fs.exists(Location.of(checkUri.normalize().toString()));
                if (!exists) {
                    errMsg = TimeUtils.longToTimeString(System.currentTimeMillis())
                            + ": path does not exist: " + path;
                    return false;
                }
                errMsg = null;
                return true;
            } finally {
                releaseSpiFs(fs);
            }
        } catch (URISyntaxException e) {
            errMsg = TimeUtils.longToTimeString(System.currentTimeMillis())
                    + ": Invalid path. " + path + ", error: " + e.getMessage();
            return false;
        } catch (IOException e) {
            errMsg = TimeUtils.longToTimeString(System.currentTimeMillis()) + ": " + e.getMessage();
            return false;
        }
    }

    // Visit the repository, and list all existing snapshot names
    public Status listSnapshots(List<String> snapshotNames) {
        // List repo root directory and extract unique snapshot names.
        // Object stores (S3/OSS) return a flat list of all objects under the prefix, so we
        // identify snapshot directories by finding "/<PREFIX_SNAPSHOT_DIR>" in each URI and
        // extracting the name segment that follows. HDFS/Broker return real directory entries
        // whose names start with PREFIX_SNAPSHOT_DIR, which is handled by the same path.
        //
        // We do NOT append "*" here because:
        //   - Broker: list() passes the path to Hadoop globStatus and handles it correctly even
        //     without a trailing wildcard when given a directory path.
        //   - HDFS: listStatusIterator does not support glob; it needs an actual directory path.
        //   - S3/OSS: list() is prefix-based; a trailing "*" would be treated as a literal key
        //             character, matching nothing.
        String repoRootPath = Joiner.on(PATH_DELIMITER).join(getLocation(), joinPrefix(PREFIX_REPO, name))
                + PATH_DELIMITER;
        org.apache.doris.filesystem.FileSystem fs;
        try {
            fs = acquireSpiFs();
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to acquire filesystem: " + e.getMessage());
        }
        try {
            Set<String> ssNameSet = new LinkedHashSet<>();
            try (FileIterator it = fs.list(Location.of(repoRootPath))) {
                while (it.hasNext()) {
                    FileEntry entry = it.next();
                    String uri = entry.location().uri();
                    if (entry.isDirectory()) {
                        // HDFS / Broker: real directory entry whose name is "__ss_<snapshotName>"
                        String entryName = uri.substring(uri.lastIndexOf('/') + 1);
                        if (entryName.startsWith(PREFIX_SNAPSHOT_DIR)) {
                            ssNameSet.add(disjoinPrefix(PREFIX_SNAPSHOT_DIR, entryName));
                        }
                    } else {
                        // S3 / OSS: flat object URI, e.g. ".../repo/__ss_snap1/__meta.xxx"
                        // Extract the snapshot name from the FIRST "/__ss_" after the repo root.
                        // Using lastIndexOf would incorrectly match nested "__ss_content" segments.
                        int ssIdx = uri.indexOf(PATH_DELIMITER + PREFIX_SNAPSHOT_DIR,
                                repoRootPath.length() - 1);
                        if (ssIdx < 0) {
                            continue;
                        }
                        String afterSs = uri.substring(ssIdx + 1 + PREFIX_SNAPSHOT_DIR.length());
                        String snapshotName = afterSs.contains(PATH_DELIMITER)
                                ? afterSs.substring(0, afterSs.indexOf(PATH_DELIMITER))
                                : afterSs;
                        if (!snapshotName.isEmpty()) {
                            ssNameSet.add(snapshotName);
                        }
                    }
                }
            }
            snapshotNames.addAll(ssNameSet);
            return Status.OK;
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to list snapshots: " + e.getMessage());
        } finally {
            releaseSpiFs(fs);
        }
    }

    //
    public boolean prepareSnapshotInfo() {
        return false;
    }

    // create remote tablet snapshot path
    // eg:
    // /location/__palo_repository_repo_name/__ss_my_ss1/__ss_content/
    // __db_10001/__tbl_10020/__part_10031/__idx_10032/__10023/__3481721
    public String assembleRemoteSnapshotPath(String label, SnapshotInfo info) {
        String path = Joiner.on(PATH_DELIMITER).join(location,
                joinPrefix(PREFIX_REPO, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                DIR_SNAPSHOT_CONTENT,
                joinPrefix(PREFIX_DB, info.getDbId()),
                joinPrefix(PREFIX_TBL, info.getTblId()),
                joinPrefix(PREFIX_PART, info.getPartitionId()),
                joinPrefix(PREFIX_IDX, info.getIndexId()),
                joinPrefix(PREFIX_COMMON, info.getTabletId()),
                joinPrefix(PREFIX_COMMON, info.getSchemaHash()));
        if (LOG.isDebugEnabled()) {
            LOG.debug("get remote tablet snapshot path: {}", path);
        }
        return path;
    }

    public Status getSnapshotInfoFile(String label, String backupTimestamp, List<BackupJobInfo> infos) {
        String remoteInfoFilePath = assembleJobInfoFilePath(label, -1) + backupTimestamp;
        File localInfoFile = new File(BackupHandler.BACKUP_ROOT_DIR + PATH_DELIMITER
                + "info_" + allocLocalFileSuffix());
        try {
            Status st = download(remoteInfoFilePath, localInfoFile.getPath());
            if (!st.ok()) {
                return st;
            }

            BackupJobInfo jobInfo = BackupJobInfo.fromFile(localInfoFile.getAbsolutePath());
            infos.add(jobInfo);
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to create job info from file: "
                    + "" + localInfoFile.getName() + ". msg: " + e.getMessage());
        } finally {
            localInfoFile.delete();
        }

        return Status.OK;
    }

    public Status getSnapshotMetaFile(String label, List<BackupMeta> backupMetas, int metaVersion) {
        String remoteMetaFilePath = assembleMetaInfoFilePath(label);
        File localMetaFile = new File(BackupHandler.BACKUP_ROOT_DIR + PATH_DELIMITER
                + "meta_" + allocLocalFileSuffix());

        try {
            Status st = download(remoteMetaFilePath, localMetaFile.getAbsolutePath());
            if (!st.ok()) {
                return st;
            }

            // read file to backupMeta
            BackupMeta backupMeta = BackupMeta.fromFile(localMetaFile.getAbsolutePath(), metaVersion);
            backupMetas.add(backupMeta);
        } catch (IOException e) {
            LOG.warn("failed to read backup meta from file", e);
            return new Status(ErrCode.COMMON_ERROR, "Failed create backup meta from file: "
                    + localMetaFile.getAbsolutePath() + ", msg: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            LOG.warn("failed to set meta version", e);
            return new Status(ErrCode.COMMON_ERROR, e.getMessage());
        } finally {
            localMetaFile.delete();
        }

        return Status.OK;
    }

    // upload the local file to specified remote file with checksum
    // remoteFilePath should be FULL path
    public Status upload(String localFilePath, String remoteFilePath) {
        // get md5sum of local file
        File file = new File(localFilePath);
        String md5sum;
        try (FileInputStream fis = new FileInputStream(file)) {
            md5sum = DigestUtils.md5Hex(fis);
        } catch (FileNotFoundException e) {
            return new Status(ErrCode.NOT_FOUND, "file " + localFilePath + " does not exist");
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get md5sum of file: " + localFilePath);
        }
        Preconditions.checkState(!Strings.isNullOrEmpty(md5sum));
        String finalRemotePath = assembleFileNameWithSuffix(remoteFilePath, md5sum);

        org.apache.doris.filesystem.FileSystem fs;
        try {
            fs = acquireSpiFs();
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to acquire filesystem: " + e.getMessage());
        }
        try {
            if (fileSystemDescriptor.getStorageType() == FsStorageType.BROKER) {
                // Broker doesn't support atomic overwrite; use temp-file dance
                String tmpRemotePath = assembleFileNameWithSuffix(remoteFilePath, SUFFIX_TMP_FILE);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("upload with temp file: local={}, tmp={}, final={}",
                            localFilePath, tmpRemotePath, finalRemotePath);
                }
                fs.delete(Location.of(tmpRemotePath), false);
                fs.delete(Location.of(finalRemotePath), false);
                spiUploadFile(fs, localFilePath, tmpRemotePath);
                fs.rename(Location.of(tmpRemotePath), Location.of(finalRemotePath));
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("upload: local={}, final={}", localFilePath, finalRemotePath);
                }
                fs.delete(Location.of(finalRemotePath), false);
                spiUploadFile(fs, localFilePath, finalRemotePath);
            }
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to upload " + localFilePath + ": " + e.getMessage());
        } finally {
            releaseSpiFs(fs);
        }

        LOG.info("finished to upload local file {} to remote file: {}", localFilePath, finalRemotePath);
        return Status.OK;
    }

    /**
     * Lists files whose paths start with {@code pathPrefix} (a glob suffix "*" is appended
     * internally).  Works correctly across all backends:
     * <ul>
     *   <li><b>S3 / OSS</b>: {@link org.apache.doris.filesystem.FileSystem#globListWithLimit}
     *       strips the trailing "*" to obtain an S3 list prefix, then filters by the full
     *       glob pattern — so "prefix*" matches any file starting with that prefix.</li>
     *   <li><b>HDFS</b>: {@code globListWithLimit} delegates to Hadoop's
     *       {@code FileSystem.globStatus}, which natively handles the "*" wildcard.</li>
     *   <li><b>Broker</b>: {@code globListWithLimit} is not implemented; falls back to
     *       {@link org.apache.doris.filesystem.FileSystem#listFiles}, which passes the path
     *       (including "*") to the broker's {@code listPath} RPC — the broker handles glob
     *       patterns via Hadoop's {@code globStatus}.</li>
     * </ul>
     */
    private static List<FileEntry> listFilesWithGlob(org.apache.doris.filesystem.FileSystem fs,
            String pathPrefix) throws IOException {
        try {
            GlobListing listing = fs.globListWithLimit(Location.of(pathPrefix + "*"), null, 0, 0);
            if (listing != null) {
                return listing.getFiles();
            }
        } catch (UnsupportedOperationException e) {
            // Broker: list() passes the path (with "*") directly to the broker's listPath RPC,
            // which calls Hadoop globStatus and handles the wildcard correctly.
        }
        return fs.listFiles(Location.of(pathPrefix + "*"));
    }

    // remoteFilePath must be a file(not dir) and does not contain checksum
    public Status download(String remoteFilePath, String localFilePath) {
        org.apache.doris.filesystem.FileSystem fs;
        try {
            fs = acquireSpiFs();
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to acquire filesystem: " + e.getMessage());
        }
        String md5sum;
        try {
            // 0. list to get to full name (with checksum)
            List<FileEntry> remoteFiles = listFilesWithGlob(fs, remoteFilePath);
            if (remoteFiles.size() != 1) {
                return new Status(ErrCode.COMMON_ERROR,
                        "Expected one file with path: " + remoteFilePath + ". get: " + remoteFiles.size());
            }
            if (remoteFiles.get(0).isDirectory()) {
                return new Status(ErrCode.COMMON_ERROR,
                        "Expected file with path: " + remoteFilePath + ". but get dir");
            }

            String remoteFileFullUri = remoteFiles.get(0).location().uri();
            String remoteFileName = remoteFileFullUri.substring(remoteFileFullUri.lastIndexOf('/') + 1);
            String remoteFilePathWithChecksum = replaceFileNameWithChecksumFileName(remoteFilePath, remoteFileName);
            if (LOG.isDebugEnabled()) {
                LOG.debug("get download filename with checksum: {}", remoteFilePathWithChecksum);
            }

            // 1. get checksum from remote file name
            Pair<String, String> pair = decodeFileNameWithChecksum(remoteFilePathWithChecksum);
            if (pair == null) {
                return new Status(ErrCode.COMMON_ERROR,
                        "file name should contain checksum: " + remoteFilePathWithChecksum);
            }
            if (!remoteFilePath.endsWith(pair.first)) {
                return new Status(ErrCode.COMMON_ERROR, "File does not exist: " + remoteFilePath);
            }
            md5sum = pair.second;

            // 2. download
            DorisInputFile inputFile = fs.newInputFile(Location.of(remoteFilePathWithChecksum));
            try (java.io.InputStream in = inputFile.newStream();
                    java.io.OutputStream out = Files.newOutputStream(Paths.get(localFilePath))) {
                copyStream(in, out);
            }

            // 3. verify checksum
            String localMd5sum;
            try (FileInputStream fis = new FileInputStream(localFilePath)) {
                localMd5sum = DigestUtils.md5Hex(fis);
            }
            if (!localMd5sum.equals(md5sum)) {
                return new Status(ErrCode.BAD_FILE,
                        "md5sum does not equal. local: " + localMd5sum + ", remote: " + md5sum);
            }
            return Status.OK;
        } catch (FileNotFoundException e) {
            return new Status(ErrCode.NOT_FOUND, "file " + localFilePath + " does not exist");
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "Failed to download file: " + e.getMessage());
        } finally {
            releaseSpiFs(fs);
        }
    }

    public Status getBrokerAddress(Long beId, Env env, List<FsBroker> brokerAddrs) {
        // get backend
        Backend be = Env.getCurrentSystemInfo().getBackend(beId);
        if (be == null) {
            return new Status(ErrCode.COMMON_ERROR, "backend " + beId + " is missing. "
                    + "failed to send upload snapshot task");
        }
        // only Broker storage backend need to get broker addr, other type return a fake one;
        if (fileSystemDescriptor.getStorageType() != FsStorageType.BROKER) {
            brokerAddrs.add(new FsBroker("127.0.0.1", 0));
            return Status.OK;
        }

        // get proper broker for this backend
        FsBroker brokerAddr = null;
        try {
            brokerAddr = env.getBrokerMgr().getBroker(fileSystemDescriptor.getName(), be.getHost());
        } catch (AnalysisException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get address of broker "
                    + fileSystemDescriptor.getName() + " when try to send upload snapshot task: "
                    + e.getMessage());
        }
        if (brokerAddr == null) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get address of broker "
                    + fileSystemDescriptor.getName() + " when try to send upload snapshot task");
        }
        brokerAddrs.add(brokerAddr);
        return Status.OK;
    }

    public List<String> getInfo() {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(id));
        info.add(name);
        info.add(TimeUtils.longToTimeString(createTime));
        info.add(String.valueOf(isReadOnly));
        info.add(location);
        info.add(fileSystemDescriptor.getStorageType() != FsStorageType.BROKER
                ? "-" : fileSystemDescriptor.getName());
        info.add(fileSystemDescriptor.getStorageType().name());
        info.add(errMsg == null ? FeConstants.null_string : errMsg);
        return info;
    }

    public List<List<String>> getSnapshotInfos(String snapshotName, String timestamp)
            throws AnalysisException {
        List<List<String>> snapshotInfos = Lists.newArrayList();
        if (Strings.isNullOrEmpty(snapshotName)) {
            // get all snapshot infos
            List<String> snapshotNames = Lists.newArrayList();
            Status status = listSnapshots(snapshotNames);
            if (!status.ok()) {
                throw new AnalysisException(
                        "Failed to list snapshot in repo: " + name + ", err: " + status.getErrMsg());
            }

            for (String ssName : snapshotNames) {
                List<String> info = getSnapshotInfo(ssName, null /* get all timestamp */);
                snapshotInfos.add(info);
            }
        } else {
            // get specified snapshot info
            List<String> info = getSnapshotInfo(snapshotName, timestamp);
            snapshotInfos.add(info);
        }

        return snapshotInfos;
    }

    public String getCreateStatement() {
        StringBuilder stmtBuilder = new StringBuilder();
        stmtBuilder.append("CREATE ");
        if (this.isReadOnly) {
            stmtBuilder.append("READ ONLY ");
        }
        stmtBuilder.append("REPOSITORY ");
        stmtBuilder.append(this.name);
        stmtBuilder.append(" \nWITH ");
        FsStorageType storageType = fileSystemDescriptor.getStorageType();
        if (storageType == FsStorageType.S3) {
            stmtBuilder.append(" S3 ");
        } else if (storageType == FsStorageType.HDFS) {
            stmtBuilder.append(" HDFS ");
        } else if (storageType == FsStorageType.BROKER) {
            stmtBuilder.append(" BROKER ");
            stmtBuilder.append(fileSystemDescriptor.getName());
        } else {
            // should never reach here
            throw new UnsupportedOperationException(storageType.toString() + " backend is not implemented");
        }
        stmtBuilder.append(" \nON LOCATION \"");
        stmtBuilder.append(this.location);
        stmtBuilder.append("\"");

        stmtBuilder.append("\nPROPERTIES\n(");
        Map<String, String> properties = new HashMap();
        properties.putAll(fileSystemDescriptor.getProperties());
        stmtBuilder.append(new DatasourcePrintableMap<>(properties, " = ", true, true, true));
        stmtBuilder.append("\n)");
        return stmtBuilder.toString();
    }

    private List<String> getSnapshotInfo(String snapshotName, String timestamp) {
        List<String> info = Lists.newArrayList();
        if (Strings.isNullOrEmpty(timestamp)) {
            // get all timestamps
            // path eg: /location/__palo_repository_repo_name/__ss_my_snap/__info_*
            String infoFilePath = assembleJobInfoFilePath(snapshotName, -1);
            if (LOG.isDebugEnabled()) {
                LOG.debug("assemble infoFilePath: {}, snapshot: {}", infoFilePath, snapshotName);
            }
            try {
                org.apache.doris.filesystem.FileSystem fs = acquireSpiFs();
                List<FileEntry> results;
                try {
                    results = listFilesWithGlob(fs, infoFilePath);
                } finally {
                    releaseSpiFs(fs);
                }
                List<String> tmp = Lists.newArrayList();
                for (FileEntry entry : results) {
                    // __info_2018-04-18-20-11-00.Jdwnd9312sfdn1294343
                    String uri = entry.location().uri();
                    String fileName = uri.substring(uri.lastIndexOf('/') + 1);
                    Pair<String, String> pureFileName = decodeFileNameWithChecksum(fileName);
                    if (pureFileName == null) {
                        // maybe: __info_2018-04-18-20-11-00.part
                        tmp.add("Invalid: " + fileName);
                        continue;
                    }
                    tmp.add(disjoinPrefix(PREFIX_JOB_INFO, pureFileName.first));
                }
                if (!tmp.isEmpty()) {
                    info.add(snapshotName);
                    info.add(Joiner.on("\n").join(tmp));
                    info.add("OK");
                } else {
                    info.add(snapshotName);
                    info.add(FeConstants.null_string);
                    info.add("ERROR: No info file found");
                }
            } catch (IOException e) {
                info.add(snapshotName);
                info.add(FeConstants.null_string);
                info.add("ERROR: Failed to get info: " + e.getMessage());
            }
        } else {
            // get specified timestamp, different repos might have snapshots with same timestamp.
            String localFilePath = BackupHandler.BACKUP_ROOT_DIR + "/"
                    + Repository.PREFIX_JOB_INFO + allocLocalFileSuffix();
            try {
                String remoteInfoFilePath = assembleJobInfoFilePath(snapshotName, -1) + timestamp;
                Status st = download(remoteInfoFilePath, localFilePath);
                if (!st.ok()) {
                    info.add(snapshotName);
                    info.add(timestamp);
                    info.add(FeConstants.null_string);
                    info.add(FeConstants.null_string);
                    info.add("Failed to get info: " + st.getErrMsg());
                } else {
                    try {
                        BackupJobInfo jobInfo = BackupJobInfo.fromFile(localFilePath);
                        info.add(snapshotName);
                        info.add(timestamp);
                        info.add(jobInfo.dbName);
                        info.add(jobInfo.getBrief());
                        info.add("OK");
                    } catch (IOException e) {
                        info.add(snapshotName);
                        info.add(timestamp);
                        info.add(FeConstants.null_string);
                        info.add(FeConstants.null_string);
                        info.add("Failed to read info from local file: " + e.getMessage());
                    }
                }
            } finally {
                // delete tmp local file
                File localFile = new File(localFilePath);
                if (localFile.exists()) {
                    localFile.delete();
                }
            }
        }

        return info;
    }

    // Allocate an unique suffix.
    private String allocLocalFileSuffix() {
        return System.currentTimeMillis() + UUID.randomUUID().toString().replace("-", "_");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
