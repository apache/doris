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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.FailMsg;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class BrokerLoadPendingTask extends LoadTask {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadPendingTask.class);

    protected Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups;
    protected BrokerDesc brokerDesc;

    public BrokerLoadPendingTask(BrokerLoadJob loadTaskCallback,
                                 Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToBrokerFileGroups,
                                 BrokerDesc brokerDesc, Priority priority) {
        super(loadTaskCallback, TaskType.PENDING, priority);
        this.retryTime = 3;
        this.attachment = new BrokerPendingTaskAttachment(signature);
        this.aggKeyToBrokerFileGroups = aggKeyToBrokerFileGroups;
        this.brokerDesc = brokerDesc;
        this.failMsg = new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL);
    }

    @Override
    public void executeTask() throws UserException {
        LOG.info("begin to execute broker pending task. job: {}", callback.getCallbackId());
        getAllFileStatus();
        ((BrokerLoadJob) callback).beginTxn();
    }

    protected void getAllFileStatus() throws UserException {
        long start = System.currentTimeMillis();
        long totalFileSize = 0;
        int totalFileNum = 0;
        for (Map.Entry<FileGroupAggKey, List<BrokerFileGroup>> entry : aggKeyToBrokerFileGroups.entrySet()) {
            FileGroupAggKey aggKey = entry.getKey();
            List<BrokerFileGroup> fileGroups = entry.getValue();

            List<List<TBrokerFileStatus>> fileStatusList = Lists.newArrayList();
            long tableTotalFileSize = 0;
            int tableTotalFileNum = 0;
            int groupNum = 0;
            if (brokerDesc.isMultiLoadBroker()) {
                for (BrokerFileGroup fileGroup : fileGroups) {
                    if (fileGroup.getFilePaths().size() != fileGroup.getFileSize().size()) {
                        LOG.warn("Cannot get file size, file path count {}, file size count {}",
                                fileGroup.getFilePaths().size(), fileGroup.getFileSize().size());
                        throw new AnalysisException("Cannot get file size.");
                    }
                    List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                    tableTotalFileNum += fileGroup.getFilePaths().size();
                    for (int i = 0; i < fileGroup.getFilePaths().size(); i++) {
                        tableTotalFileSize += fileGroup.getFileSize().get(i);
                        TBrokerFileStatus fileStatus = new TBrokerFileStatus(fileGroup.getFilePaths().get(i),
                                false, fileGroup.getFileSize().get(i), false);
                        fileStatuses.add(fileStatus);
                    }
                    fileStatusList.add(fileStatuses);
                }
            } else {
                for (BrokerFileGroup fileGroup : fileGroups) {
                    long groupFileSize = 0;
                    List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                    for (String path : fileGroup.getFilePaths()) {
                        BrokerUtil.parseFile(path, brokerDesc, fileStatuses);
                    }
                    boolean isBinaryFileFormat = fileGroup.isBinaryFileFormat();
                    List<TBrokerFileStatus> filteredFileStatuses = Lists.newArrayList();
                    for (TBrokerFileStatus fstatus : fileStatuses) {
                        if (fstatus.getSize() == 0 && isBinaryFileFormat) {
                            // For parquet or orc file, if it is an empty file, ignore it.
                            // Because we can not read an empty parquet or orc file.
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                                        .add("empty file", fstatus).build());
                            }
                        } else {
                            groupFileSize += fstatus.size;
                            filteredFileStatuses.add(fstatus);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                                        .add("file_status", fstatus).build());
                            }
                        }
                    }
                    fileStatusList.add(filteredFileStatuses);
                    tableTotalFileSize += groupFileSize;
                    tableTotalFileNum += filteredFileStatuses.size();
                    LOG.info("get {} files in file group {} for table {}. size: {}. job: {}, broker: {} ",
                            filteredFileStatuses.size(), groupNum, entry.getKey(), groupFileSize,
                            callback.getCallbackId(),
                            brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER
                                    ? BrokerUtil.getAddress(brokerDesc) : brokerDesc.getStorageType());
                    groupNum++;
                }
            }

            totalFileSize += tableTotalFileSize;
            totalFileNum += tableTotalFileNum;
            ((BrokerPendingTaskAttachment) attachment).addFileStatus(aggKey, fileStatusList);
            LOG.info("get {} files to be loaded. total size: {}. cost: {} ms, job: {}",
                    tableTotalFileNum, tableTotalFileSize, (System.currentTimeMillis() - start),
                    callback.getCallbackId());
        }

        ((BrokerLoadJob) callback).setLoadFileInfo(totalFileNum, totalFileSize);
    }
}
