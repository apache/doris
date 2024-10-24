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

package org.apache.doris.cloud.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud.CopyJobPB;
import org.apache.doris.cloud.proto.Cloud.CopyJobPB.JobStatus;
import org.apache.doris.cloud.proto.Cloud.ObjectFilePB;
import org.apache.doris.cloud.stage.StageUtil;
import org.apache.doris.cloud.storage.ListObjectsResult;
import org.apache.doris.cloud.storage.ObjectFile;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.loadv2.BrokerLoadPendingTask;
import org.apache.doris.load.loadv2.BrokerPendingTaskAttachment;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class CopyLoadPendingTask extends BrokerLoadPendingTask {
    private static final Logger LOG = LogManager.getLogger(CopyLoadPendingTask.class);
    private static final String NO_FILES_ERROR_MSG = "No files can be copied";
    private Map<FileGroupAggKey, List<List<Pair<TBrokerFileStatus, ObjectFilePB>>>> fileStatusMap = Maps.newHashMap();

    private int matchedFileNum = 0;
    private int loadedFileNum = 0;
    private String reachLimitStr = "";
    // BeginCopy may have been executed if replay when FE is restarted
    private boolean isBeginCopyDone = false;

    public CopyLoadPendingTask(CopyJob loadTaskCallback,
            Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups, BrokerDesc brokerDesc) {
        super(loadTaskCallback, aggKeyToBrokerFileGroups, brokerDesc, Priority.NORMAL);
        retryTime = 0;
    }

    @Override
    public void executeTask() throws UserException {
        super.executeTask(); // get all files and begin txn
        if (!isBeginCopyDone) {
            beginCopy((BrokerPendingTaskAttachment) attachment);
        }
        ((CopyJob) callback).setSelectedFiles(((BrokerPendingTaskAttachment) attachment).getFileStatusMap());
    }

    @Override
    protected void getAllFileStatus() throws UserException {
        long start = System.currentTimeMillis();
        CopyJob copyJob = (CopyJob) callback;
        for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
            FileGroupAggKey aggKey = entry.getKey();
            List<BrokerFileGroup> fileGroups = entry.getValue();
            LOG.debug("aggKey:{}, fileGroups:{}", aggKey, fileGroups);

            List<List<Pair<TBrokerFileStatus, ObjectFilePB>>> fileStatusList = Lists.newArrayList();
            long tableTotalFileSize = 0;
            int tableTotalFileNum = 0;
            int groupNum = 0;
            for (BrokerFileGroup fileGroup : fileGroups) {
                long groupFileSize = 0;
                List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatuses = Lists.newArrayList();
                if (copyJob.isReplay()) {
                    fileStatuses = getCopyFilesWhenReplay(copyJob.getStageId(), fileGroup.getTableId(),
                            copyJob.getCopyId(), copyJob.getObjectInfo());
                    LOG.info("Get copy files when replay, stageId={}, tableId={}, copyId={}, files={}",
                            copyJob.getStageId(), fileGroup.getTableId(), copyJob.getCopyId(),
                            fileStatuses.stream().map(p -> p.second.getRelativePath()).collect(Collectors.toList()));
                }
                if (!isBeginCopyDone) {
                    for (String path : fileGroup.getFilePaths()) {
                        LOG.debug("input path = {}", path);
                        parseFileForCopyJob(copyJob.getStageId(), fileGroup.getTableId(), copyJob.getCopyId(),
                                copyJob.getPattern(), copyJob.getSizeLimit(), Config.max_file_num_per_copy_into_job,
                                Config.max_meta_size_per_copy_into_job, fileStatuses, copyJob.getObjectInfo(),
                                copyJob.isForceCopy());
                    }
                }
                boolean isBinaryFileFormat = fileGroup.isBinaryFileFormat();
                List<Pair<TBrokerFileStatus, ObjectFilePB>> filteredFileStatuses = Lists.newArrayList();
                for (Pair<TBrokerFileStatus, ObjectFilePB> pair : fileStatuses) {
                    TBrokerFileStatus fstatus = pair.first;
                    if (fstatus.getSize() == 0 && isBinaryFileFormat) {
                        // For parquet or orc file, if it is an empty file, ignore it.
                        // Because we can not read an empty parquet or orc file.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId()).add("empty file", fstatus)
                                            .build());
                        }
                    } else {
                        groupFileSize += fstatus.size;
                        filteredFileStatuses.add(pair);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId()).add("file_status",
                                    fstatus).build());
                        }
                    }
                }
                fileStatusList.add(filteredFileStatuses);
                tableTotalFileSize += groupFileSize;
                tableTotalFileNum += filteredFileStatuses.size();
                LOG.info("get {} files in file group {} for table {}. size: {}. job: {}, broker: {} ",
                        filteredFileStatuses.size(), groupNum, entry.getKey(), groupFileSize, callback.getCallbackId(),
                        brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER ? BrokerUtil.getAddress(
                                brokerDesc) : brokerDesc.getStorageType());
                groupNum++;
            }
            if (fileStatusList.stream().flatMap(List::stream).map(l -> l.second).count() == 0) {
                retryTime = 0;
                copyJob.retryTimes = 0;
                copyJob.setAbortedCopy(true);
                throw new UserException(String.format(NO_FILES_ERROR_MSG + ", matched %d files, "
                        + "filtered %d files because files may be loading or loaded"
                        + reachLimitStr, matchedFileNum, loadedFileNum));
            }
            fileStatusMap.put(aggKey, fileStatusList);
            LOG.info("get {} files to be loaded. total size: {}. cost: {} ms, job: {}, queryId: {}", tableTotalFileNum,
                    tableTotalFileSize, (System.currentTimeMillis() - start), callback.getCallbackId(),
                    copyJob.getCopyId());
        }
    }

    private void beginCopy(BrokerPendingTaskAttachment attachment) throws UserException {
        CopyJob copyJob = (CopyJob) callback;
        long startTime = System.currentTimeMillis();
        long timeoutTime = startTime + copyJob.getTimeout() * 1000 + 5000; // add a delta

        long totalFileSize = 0;
        int totalFileNum = 0;
        for (Entry<FileGroupAggKey, List<List<Pair<TBrokerFileStatus, ObjectFilePB>>>> entry :
                fileStatusMap.entrySet()) {
            FileGroupAggKey fileGroupAggKey = entry.getKey();
            List<List<Pair<TBrokerFileStatus, ObjectFilePB>>> value = entry.getValue();

            List<ObjectFilePB> objectFiles = value.stream().flatMap(List::stream).map(l -> l.second)
                    .collect(Collectors.toList());
            // groupId is 0 because the tableId is unique in FileGroupAggKey(copy into can't set partition now)
            List<ObjectFilePB> filteredObjectFiles = copyJob.isForceCopy() ? objectFiles
                    : ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                            .beginCopy(copyJob.getStageId(), copyJob.getStageType(), fileGroupAggKey.getTableId(),
                                    copyJob.getCopyId(), 0, startTime, timeoutTime, objectFiles, copyJob.getSizeLimit(),
                                    Config.max_file_num_per_copy_into_job, Config.max_meta_size_per_copy_into_job);
            if (filteredObjectFiles.isEmpty()) {
                retryTime = 0;
                copyJob.setAbortedCopy(true);
                throw new UserException(String.format(NO_FILES_ERROR_MSG + ", matched %d files, "
                        + "filtered %d files because files may be loading or loaded"
                        + reachLimitStr, matchedFileNum, matchedFileNum));
            }
            copyJob.setAbortedCopy(false);
            Set<String> filteredObjectSet = filteredObjectFiles.stream()
                    .map(f -> StageUtil.getFileInfoUniqueId(f)).collect(Collectors.toSet());
            LOG.debug("Begin copy for stage={}, table={}, queryId={}, before objectSize={}, filtered objectSize={}",
                    copyJob.getStageId(), fileGroupAggKey.getTableId(), copyJob.getCopyId(), objectFiles.size(),
                    filteredObjectSet.size());

            List<List<TBrokerFileStatus>> fileStatusList = new ArrayList<>();
            long tableTotalFileSize = 0;
            int tableTotalFileNum = 0;
            int groupNum = 0;

            boolean needFilter = filteredObjectFiles.size() != objectFiles.size();
            for (List<Pair<TBrokerFileStatus, ObjectFilePB>> pairs : value) {
                List<TBrokerFileStatus> fileStatuses = new ArrayList<>();
                for (Pair<TBrokerFileStatus, ObjectFilePB> pair : pairs) {
                    TBrokerFileStatus brokerFileStatus = pair.first;
                    ObjectFilePB objectFile = pair.second;
                    if (!needFilter || filteredObjectSet.contains(StageUtil.getFileInfoUniqueId(objectFile))) {
                        tableTotalFileSize += brokerFileStatus.getSize();
                        tableTotalFileNum++;
                        fileStatuses.add(brokerFileStatus);
                    }
                }
                fileStatusList.add(fileStatuses);
                LOG.info("get {} files in file group {} for table {}. size: {}. job: {}", fileStatuses.size(), groupNum,
                        entry.getKey(), tableTotalFileSize, callback.getCallbackId());
                groupNum++;
            }

            totalFileSize += tableTotalFileSize;
            totalFileNum += tableTotalFileNum;
            attachment.addFileStatus(fileGroupAggKey, fileStatusList);
            LOG.info("get {} files to be loaded. total size: {}. cost: {} ms, job: {}", tableTotalFileNum,
                    tableTotalFileSize, (System.currentTimeMillis() - startTime), callback.getCallbackId());
        }
        copyJob.setLoadFileInfo(totalFileNum, totalFileSize);
    }

    protected void parseFileForCopyJob(String stageId, long tableId, String copyId, String pattern, long sizeLimit,
            int fileNumLimit, int fileMetaSizeLimit, List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus,
            ObjectInfo objectInfo, boolean forceCopy) throws UserException {
        try {
            Triple<Integer, Integer, String> triple = StageUtil.listAndFilterFilesV2(objectInfo, pattern, copyId,
                    stageId, tableId, forceCopy, sizeLimit, fileNumLimit, fileMetaSizeLimit, fileStatus);
            matchedFileNum = triple.getLeft();
            loadedFileNum = triple.getMiddle();
            reachLimitStr = triple.getRight();
        } catch (Exception e) {
            LOG.warn("Failed to list copy files for queryId={}", copyId, e);
            throw new UserException("list copy files failed. msg=" + e.getMessage());
        }
    }

    private List<ObjectFilePB> getCopyFiles(String stageId, long tableId, boolean force) throws DdlException {
        return force ? new ArrayList<>() :
                ((CloudInternalCatalog) Env.getCurrentInternalCatalog()).getCopyFiles(stageId, tableId);
    }

    protected void listAndFilterFiles(ObjectInfo objectInfo, String pattern, String copyId, long sizeLimit, int fileNum,
            int metaSize, List<ObjectFilePB> copiedFiles, List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus)
            throws Exception {
        long startTimestamp = System.currentTimeMillis();
        long listFileNum = 0;
        matchedFileNum = 0;
        loadedFileNum = 0;
        reachLimitStr = "";
        RemoteBase remote = RemoteBase.newInstance(objectInfo);
        Set<String> loadedFileSet = copiedFiles.stream().map(f -> StageUtil.getFileInfoUniqueId(f))
                .collect(Collectors.toSet());
        try {
            long totalSize = 0;
            long totalMetaSize = 0;
            PathMatcher matcher = StageUtil.getPathMatcher(pattern);
            String continuationToken = null;
            boolean finish = false;
            while (!finish) {
                ListObjectsResult listObjectsResult = remote.listObjects(continuationToken);
                listFileNum += listObjectsResult.getObjectInfoList().size();
                long costSeconds = (System.currentTimeMillis() - startTimestamp) / 1000;
                if (costSeconds >= 3600 || listFileNum >= 1000000) {
                    throw new DdlException("Abort list object for queryId=" + ((CopyJob) callback).getCopyId()
                            + ". We don't collect enough files to load, after listing " + listFileNum + " objects for "
                            + costSeconds + " seconds, please check if your pattern " + pattern + " is correct.");
                }
                for (ObjectFile objectFile : listObjectsResult.getObjectInfoList()) {
                    // check:
                    // 1. match pattern if it's set
                    // 2. file is not copying or copied by other copy jobs
                    // 3. not reach any limit of fileNum/fileSize/fileMetaSize if select more than 1 file
                    if (!StageUtil.matchPattern(objectFile.getRelativePath(), matcher)) {
                        continue;
                    }
                    matchedFileNum++;
                    if (loadedFileSet.contains(StageUtil.getFileInfoUniqueId(objectFile))) {
                        loadedFileNum++;
                        continue;
                    }
                    ObjectFilePB objectFilePB = ObjectFilePB.newBuilder().setRelativePath(objectFile.getRelativePath())
                            .setEtag(objectFile.getEtag()).build();
                    if (fileStatus.size() > 0 && sizeLimit > 0 && totalSize + objectFile.getSize() >= sizeLimit) {
                        finish = true;
                        reachLimitStr = ", skip list because reach size limit: " + sizeLimit;
                        break;
                    }
                    if (fileStatus.size() > 0 && metaSize > 0
                            && totalMetaSize + objectFilePB.getSerializedSize() >= metaSize) {
                        finish = true;
                        reachLimitStr = ", skip list because reach meta size limit: " + metaSize;
                        break;
                    }
                    // add file
                    String objUrl = "s3://" + objectInfo.getBucket() + "/" + objectFile.getKey();
                    fileStatus.add(
                            Pair.of(new TBrokerFileStatus(objUrl, false, objectFile.getSize(), true), objectFilePB));

                    totalSize += objectFile.getSize();
                    totalMetaSize += objectFilePB.getSerializedSize();
                    if (fileNum > 0 && fileStatus.size() >= fileNum) {
                        finish = true;
                        reachLimitStr = ", skip list because reach file num limit: " + fileNum;
                        break;
                    }
                }
                if (!listObjectsResult.isTruncated()) {
                    break;
                }
                continuationToken = listObjectsResult.getContinuationToken();
            }
        } finally {
            remote.close();
        }
    }

    private List<Pair<TBrokerFileStatus, ObjectFilePB>> getCopyFilesWhenReplay(String stageId, long tableId,
            String copyId, ObjectInfo objectInfo) throws DdlException {
        CopyJobPB copyJobPB =
                ((CloudInternalCatalog) Env.getCurrentInternalCatalog()).getCopyJob(stageId, tableId, copyId, 0);
        // BeginCopy does not execute
        if (copyJobPB == null) {
            return new ArrayList<>();
        }
        if (copyJobPB.getJobStatus() != JobStatus.LOADING) {
            throw new DdlException("Copy job is not in loading status, status=" + copyJobPB.getJobStatus());
        }
        isBeginCopyDone = true;
        RemoteBase remote = null;
        try {
            remote = RemoteBase.newInstance(objectInfo);
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatuses = Lists.newArrayList();
            for (ObjectFilePB objectFile : copyJobPB.getObjectFilesList()) {
                List<ObjectFile> files = remote.headObject(objectFile.getRelativePath()).getObjectInfoList();
                TBrokerFileStatus brokerFileStatus = null;
                for (ObjectFile file : files) {
                    if (file.getRelativePath().equals(objectFile.getRelativePath()) && file.getEtag()
                            .equals(objectFile.getEtag())) {
                        String objUrl = "s3://" + objectInfo.getBucket() + "/" + file.getKey();
                        brokerFileStatus = new TBrokerFileStatus(objUrl, false, file.getSize(), true);
                        break;
                    }
                }
                if (brokerFileStatus == null) {
                    throw new Exception("Can not find object with relative path: " + objectFile.getRelativePath());
                }
                fileStatuses.add(Pair.of(brokerFileStatus, objectFile));
            }
            return fileStatuses;
        } catch (Exception e) {
            throw new DdlException(e.getMessage());
        } finally {
            remote.close();
        }
    }
}
