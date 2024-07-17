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

import org.apache.doris.analysis.CreateRepositoryStmt;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.PersistentFileSystem;
import org.apache.doris.fs.remote.AzureFileSystem;
import org.apache.doris.fs.remote.BrokerFileSystem;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.fs.remote.S3FileSystem;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    @SerializedName("fs")
    private PersistentFileSystem fileSystem;

    private Repository() {
        // for persist
    }

    public Repository(long id, String name, boolean isReadOnly, String location, RemoteFileSystem fileSystem) {
        this.id = id;
        this.name = name;
        this.isReadOnly = isReadOnly;
        this.location = location;
        this.fileSystem = fileSystem;
        this.createTime = System.currentTimeMillis();
    }

    // join job info file name with timestamp
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
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_137) {
            Repository repo = new Repository();
            repo.readFields(in);
            return repo;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), Repository.class);
        }
    }

    @Override
    public void gsonPostProcess() {
        StorageBackend.StorageType type = StorageBackend.StorageType.BROKER;
        if (this.fileSystem.properties.containsKey(PersistentFileSystem.STORAGE_TYPE)) {
            type = StorageBackend.StorageType.valueOf(
                    this.fileSystem.properties.get(PersistentFileSystem.STORAGE_TYPE));
            this.fileSystem.properties.remove(PersistentFileSystem.STORAGE_TYPE);
        }
        this.fileSystem = FileSystemFactory.get(this.fileSystem.getName(),
                type,
                this.fileSystem.getProperties());
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
        return location;
    }

    public String getErrorMsg() {
        return errMsg;
    }

    public PersistentFileSystem getRemoteFileSystem() {
        return fileSystem;
    }

    public long getCreateTime() {
        return createTime;
    }

    // create repository dir and repo info file
    public Status initRepository() {
        if (FeConstants.runningUnitTest) {
            return Status.OK;
        }

        // A temporary solution is to delete all stale snapshots before creating an S3 repository
        // so that we can add regression tests about backup/restore.
        //
        // TODO: support hdfs/brokers
        if (fileSystem instanceof S3FileSystem || fileSystem instanceof AzureFileSystem) {
            String deleteStaledSnapshots = fileSystem.getProperties()
                    .getOrDefault(CreateRepositoryStmt.PROP_DELETE_IF_EXISTS, "false");
            if (deleteStaledSnapshots.equalsIgnoreCase("true")) {
                // delete with prefix:
                // eg. __palo_repository_repo_name/
                String snapshotPrefix = Joiner.on(PATH_DELIMITER).join(location, joinPrefix(PREFIX_REPO, name));
                LOG.info("property {} is set, delete snapshots with prefix: {}",
                        CreateRepositoryStmt.PROP_DELETE_IF_EXISTS, snapshotPrefix);
                Status st = fileSystem.deleteDirectory(snapshotPrefix);
                if (!st.ok()) {
                    return st;
                }
            }
        }

        String repoInfoFilePath = assembleRepoInfoFilePath();
        // check if the repo is already exist in remote
        List<RemoteFile> remoteFiles = Lists.newArrayList();
        Status st = fileSystem.globList(repoInfoFilePath, remoteFiles);
        if (!st.ok()) {
            return st;
        }
        if (remoteFiles.size() == 1) {
            RemoteFile remoteFile = remoteFiles.get(0);
            if (!remoteFile.isFile()) {
                return new Status(ErrCode.COMMON_ERROR, "the existing repo info is not a file");
            }

            // exist, download and parse the repo info file
            String localFilePath = BackupHandler.BACKUP_ROOT_DIR + "/tmp_info_" + allocLocalFileSuffix();
            try {
                st = fileSystem.downloadWithFileSize(repoInfoFilePath, localFilePath, remoteFile.getSize());
                if (!st.ok()) {
                    return st;
                }

                byte[] bytes = Files.readAllBytes(Paths.get(localFilePath));
                String json = new String(bytes, StandardCharsets.UTF_8);
                JSONObject root = (JSONObject) JSONValue.parse(json);
                if (name.compareTo((String) root.get("name")) != 0) {
                    return new Status(ErrCode.COMMON_ERROR,
                            "Invalid repository __repo_info, expected repo '" + name + "', but get name '"
                                + (String) root.get("name") + "' from " + repoInfoFilePath);
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
                File localFile = new File(localFilePath);
                localFile.delete();
            }

        } else if (remoteFiles.size() > 1) {
            return new Status(ErrCode.COMMON_ERROR,
                    "Invalid repository dir. expected one repo info file. get more: " + remoteFiles);
        } else {
            // repo is already exist, get repo info
            JSONObject root = new JSONObject();
            root.put("name", name);
            root.put("create_time", TimeUtils.longToTimeString(createTime));
            String repoInfoContent = root.toString();
            return fileSystem.directUpload(repoInfoContent, repoInfoFilePath);
        }
    }

    public Status alterRepositoryS3Properties(Map<String, String> properties) {
        if (fileSystem instanceof S3FileSystem) {
            Map<String, String> oldProperties = new HashMap<>(this.getRemoteFileSystem().getProperties());
            oldProperties.remove(S3Properties.ACCESS_KEY);
            oldProperties.remove(S3Properties.SECRET_KEY);
            oldProperties.remove(S3Properties.SESSION_TOKEN);
            oldProperties.remove(S3Properties.Env.ACCESS_KEY);
            oldProperties.remove(S3Properties.Env.SECRET_KEY);
            oldProperties.remove(S3Properties.Env.TOKEN);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (Objects.equals(entry.getKey(), S3Properties.ACCESS_KEY)
                        || Objects.equals(entry.getKey(), S3Properties.Env.ACCESS_KEY)) {
                    oldProperties.putIfAbsent(S3Properties.ACCESS_KEY, entry.getValue());
                }
                if (Objects.equals(entry.getKey(), S3Properties.SECRET_KEY)
                        || Objects.equals(entry.getKey(), S3Properties.Env.SECRET_KEY)) {
                    oldProperties.putIfAbsent(S3Properties.SECRET_KEY, entry.getValue());
                }
                if (Objects.equals(entry.getKey(), S3Properties.SESSION_TOKEN)
                        || Objects.equals(entry.getKey(), S3Properties.Env.TOKEN)) {
                    oldProperties.putIfAbsent(S3Properties.SESSION_TOKEN, entry.getValue());
                }
            }
            properties.clear();
            properties.putAll(oldProperties);
            return Status.OK;
        } else {
            return new Status(ErrCode.COMMON_ERROR, "Only support alter s3 repository");
        }
    }

    // eg: location/__palo_repository_repo_name/__repo_info
    public String assembleRepoInfoFilePath() {
        return Joiner.on(PATH_DELIMITER).join(location,
                joinPrefix(PREFIX_REPO, name),
                FILE_REPO_INFO);
    }

    // eg: location/__palo_repository_repo_name/__my_sp1/__meta
    public String assembleMetaInfoFilePath(String label) {
        return Joiner.on(PATH_DELIMITER).join(location, joinPrefix(PREFIX_REPO, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                FILE_META_INFO);
    }

    // eg: location/__palo_repository_repo_name/__my_sp1/__info_2018-01-01-08-00-00
    public String assembleJobInfoFilePath(String label, long createTime) {
        return Joiner.on(PATH_DELIMITER).join(location, joinPrefix(PREFIX_REPO, name),
                joinPrefix(PREFIX_SNAPSHOT_DIR, label),
                jobInfoFileNameWithTimestamp(createTime));
    }

    // eg:
    // __palo_repository_repo_name/__ss_my_ss1/__ss_content/__db_10001/__tbl_10020/__part_10031/__idx_10020/__10022/
    public String getRepoTabletPathBySnapshotInfo(String label, SnapshotInfo info) {
        String path = Joiner.on(PATH_DELIMITER).join(location, joinPrefix(PREFIX_REPO, name),
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
        String path = Joiner.on(PATH_DELIMITER).join(location, joinPrefix(PREFIX_REPO, name),
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
        // for s3 sdk, the headObject() method does not support list "dir",
        // so we check FILE_REPO_INFO instead.
        String path = location + "/" + joinPrefix(PREFIX_REPO, name) + "/" + FILE_REPO_INFO;
        try {
            URI checkUri = new URI(path);
            Status st = fileSystem.exists(checkUri.normalize().toString());
            if (!st.ok()) {
                errMsg = TimeUtils.longToTimeString(System.currentTimeMillis()) + ": " + st.getErrMsg();
                return false;
            }
            // clear err msg
            errMsg = null;

            return true;
        } catch (URISyntaxException e) {
            errMsg = TimeUtils.longToTimeString(System.currentTimeMillis())
                    + ": Invalid path. " + path + ", error: " + e.getMessage();
            return false;
        }
    }

    // Visit the repository, and list all existing snapshot names
    public Status listSnapshots(List<String> snapshotNames) {
        // list with prefix:
        // eg. __palo_repository_repo_name/__ss_*
        String listPath = Joiner.on(PATH_DELIMITER).join(location, joinPrefix(PREFIX_REPO, name), PREFIX_SNAPSHOT_DIR)
                + "*";
        List<RemoteFile> result = Lists.newArrayList();
        Status st = fileSystem.globList(listPath, result);
        if (!st.ok()) {
            return st;
        }

        for (RemoteFile remoteFile : result) {
            if (remoteFile.isFile()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("get snapshot path{} which is not a dir", remoteFile);
                }
                continue;
            }

            snapshotNames.add(disjoinPrefix(PREFIX_SNAPSHOT_DIR, remoteFile.getName()));
        }
        return Status.OK;
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
        // Preconditions.checkArgument(remoteFilePath.startsWith(location), remoteFilePath);
        // get md5usm of local file
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

        Status st = Status.OK;
        if (fileSystem instanceof BrokerFileSystem) {
            // this may be a retry, so we should first delete remote file
            String tmpRemotePath = assembleFileNameWithSuffix(remoteFilePath, SUFFIX_TMP_FILE);
            if (LOG.isDebugEnabled()) {
                LOG.debug("get md5sum of file: {}. tmp remote path: {}. final remote path: {}",
                        localFilePath, tmpRemotePath, finalRemotePath);
            }
            st = fileSystem.delete(tmpRemotePath);
            if (!st.ok()) {
                return st;
            }

            st = fileSystem.delete(finalRemotePath);
            if (!st.ok()) {
                return st;
            }

            // upload tmp file
            st = fileSystem.upload(localFilePath, tmpRemotePath);
            if (!st.ok()) {
                return st;
            }

            // rename tmp file with checksum named file
            st = fileSystem.rename(tmpRemotePath, finalRemotePath);
            if (!st.ok()) {
                return st;
            }
        } else if (fileSystem instanceof S3FileSystem || fileSystem instanceof AzureFileSystem) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("get md5sum of file: {}. final remote path: {}", localFilePath, finalRemotePath);
            }
            st = fileSystem.delete(finalRemotePath);
            if (!st.ok()) {
                return st;
            }

            // upload final file
            st = fileSystem.upload(localFilePath, finalRemotePath);
            if (!st.ok()) {
                return st;
            }
        } else if (fileSystem instanceof DFSFileSystem) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("hdfs get md5sum of file: {}. final remote path: {}", localFilePath, finalRemotePath);
            }
            st = fileSystem.delete(finalRemotePath);
            if (!st.ok()) {
                return st;
            }

            // upload final file
            st = fileSystem.upload(localFilePath, finalRemotePath);
            if (!st.ok()) {
                return st;
            }
        }

        LOG.info("finished to upload local file {} to remote file: {}", localFilePath, finalRemotePath);
        return st;
    }

    // remoteFilePath must be a file(not dir) and does not contain checksum
    public Status download(String remoteFilePath, String localFilePath) {
        // 0. list to get to full name(with checksum)
        List<RemoteFile> remoteFiles = Lists.newArrayList();
        Status status = fileSystem.globList(remoteFilePath + "*", remoteFiles);
        if (!status.ok()) {
            return status;
        }
        if (remoteFiles.size() != 1) {
            return new Status(ErrCode.COMMON_ERROR,
                    "Expected one file with path: " + remoteFilePath + ". get: " + remoteFiles.size());
        }
        if (!remoteFiles.get(0).isFile()) {
            return new Status(ErrCode.COMMON_ERROR, "Expected file with path: " + remoteFilePath + ". but get dir");
        }

        String remoteFilePathWithChecksum = replaceFileNameWithChecksumFileName(remoteFilePath,
                remoteFiles.get(0).getName());
        if (LOG.isDebugEnabled()) {
            LOG.debug("get download filename with checksum: " + remoteFilePathWithChecksum);
        }

        // 1. get checksum from remote file name
        Pair<String, String> pair = decodeFileNameWithChecksum(remoteFilePathWithChecksum);
        if (pair == null) {
            return new Status(ErrCode.COMMON_ERROR,
                    "file name should contains checksum: " + remoteFilePathWithChecksum);
        }
        if (!remoteFilePath.endsWith(pair.first)) {
            return new Status(ErrCode.COMMON_ERROR, "File does not exist: " + remoteFilePath);
        }
        String md5sum = pair.second;

        // 2. download
        status = fileSystem.downloadWithFileSize(remoteFilePathWithChecksum, localFilePath,
                    remoteFiles.get(0).getSize());
        if (!status.ok()) {
            return status;
        }

        // 3. verify checksum
        String localMd5sum = null;
        try (FileInputStream fis = new FileInputStream(localFilePath)) {
            localMd5sum = DigestUtils.md5Hex(fis);
        } catch (FileNotFoundException e) {
            return new Status(ErrCode.NOT_FOUND, "file " + localFilePath + " does not exist");
        } catch (IOException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get md5sum of file: " + localFilePath);
        }

        if (!localMd5sum.equals(md5sum)) {
            return new Status(ErrCode.BAD_FILE,
                    "md5sum does not equal. local: " + localMd5sum + ", remote: " + md5sum);
        }

        return Status.OK;
    }

    public Status getBrokerAddress(Long beId, Env env, List<FsBroker> brokerAddrs) {
        // get backend
        Backend be = Env.getCurrentSystemInfo().getBackend(beId);
        if (be == null) {
            return new Status(ErrCode.COMMON_ERROR, "backend " + beId + " is missing. "
                    + "failed to send upload snapshot task");
        }
        // only Broker storage backend need to get broker addr, other type return a fake one;
        if (fileSystem.getStorageType() != StorageBackend.StorageType.BROKER) {
            brokerAddrs.add(new FsBroker("127.0.0.1", 0));
            return Status.OK;
        }

        // get proper broker for this backend
        FsBroker brokerAddr = null;
        try {
            brokerAddr = env.getBrokerMgr().getBroker(fileSystem.getName(), be.getHost());
        } catch (AnalysisException e) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get address of broker "
                    + fileSystem.getName() + " when try to send upload snapshot task: "
                    + e.getMessage());
        }
        if (brokerAddr == null) {
            return new Status(ErrCode.COMMON_ERROR, "failed to get address of broker "
                    + fileSystem.getName() + " when try to send upload snapshot task");
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
        info.add(fileSystem.getStorageType() != StorageBackend.StorageType.BROKER ? "-" : fileSystem.getName());
        info.add(fileSystem.getStorageType().name());
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
        StorageBackend.StorageType storageType = this.fileSystem.getStorageType();
        if (storageType == StorageBackend.StorageType.S3) {
            stmtBuilder.append(" S3 ");
        } else if (storageType == StorageBackend.StorageType.HDFS) {
            stmtBuilder.append(" HDFS ");
        } else if (storageType == StorageBackend.StorageType.BROKER) {
            stmtBuilder.append(" BROKER ");
            stmtBuilder.append(this.fileSystem.getName());
        } else {
            // should never reach here
            throw new UnsupportedOperationException(storageType.toString() + " backend is not implemented");
        }
        stmtBuilder.append(" \nON LOCATION \"");
        stmtBuilder.append(this.location);
        stmtBuilder.append("\"");

        stmtBuilder.append("\nPROPERTIES\n(");
        Map<String, String> properties = new HashMap();
        properties.putAll(this.getRemoteFileSystem().getProperties());
        stmtBuilder.append(new PrintableMap<>(properties, " = ", true, true, true));
        stmtBuilder.append("\n)");
        return stmtBuilder.toString();
    }

    private List<String> getSnapshotInfo(String snapshotName, String timestamp) {
        List<String> info = Lists.newArrayList();
        if (Strings.isNullOrEmpty(timestamp)) {
            // get all timestamp
            // path eg: /location/__palo_repository_repo_name/__ss_my_snap/__info_*
            String infoFilePath = assembleJobInfoFilePath(snapshotName, -1);
            if (LOG.isDebugEnabled()) {
                LOG.debug("assemble infoFilePath: {}, snapshot: {}", infoFilePath, snapshotName);
            }
            List<RemoteFile> results = Lists.newArrayList();
            Status st = fileSystem.globList(infoFilePath + "*", results);
            if (!st.ok()) {
                info.add(snapshotName);
                info.add(FeConstants.null_string);
                info.add("ERROR: Failed to get info: " + st.getErrMsg());
            } else {
                List<String> tmp = Lists.newArrayList();
                for (RemoteFile file : results) {
                    // __info_2018-04-18-20-11-00.Jdwnd9312sfdn1294343
                    Pair<String, String> pureFileName = decodeFileNameWithChecksum(file.getName());
                    if (pureFileName == null) {
                        // maybe: __info_2018-04-18-20-11-00.part
                        tmp.add("Invalid: " + file.getName());
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

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        name = Text.readString(in);
        isReadOnly = in.readBoolean();
        location = Text.readString(in);
        fileSystem = PersistentFileSystem.read(in);
        createTime = in.readLong();
    }
}
