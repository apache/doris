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
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.FailMsg;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class BrokerLoadPendingTask extends LoadTask {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadPendingTask.class);

    private Map<Long, List<BrokerFileGroup>> tableToBrokerFileList;
    private BrokerDesc brokerDesc;

    public BrokerLoadPendingTask(BrokerLoadJob loadTaskCallback,
                                 Map<Long, List<BrokerFileGroup>> tableToBrokerFileList,
                                 BrokerDesc brokerDesc) {
        super(loadTaskCallback);
        this.attachment = new BrokerPendingTaskAttachment(signature);
        this.tableToBrokerFileList = tableToBrokerFileList;
        this.brokerDesc = brokerDesc;
        this.failMsg = new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL);
    }

    @Override
    void executeTask() throws UserException {
        getAllFileStatus();
    }

    private void getAllFileStatus()
            throws UserException {
        for (Map.Entry<Long, List<BrokerFileGroup>> entry : tableToBrokerFileList.entrySet()) {
            long tableId = entry.getKey();

            List<List<TBrokerFileStatus>> fileStatusList = Lists.newArrayList();
            List<BrokerFileGroup> fileGroups = entry.getValue();
            for (BrokerFileGroup fileGroup : fileGroups) {
                List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                for (String path : fileGroup.getFilePaths()) {
                    BrokerUtil.parseBrokerFile(path, brokerDesc, fileStatuses);
                }
                fileStatusList.add(fileStatuses);
                if (LOG.isDebugEnabled()) {
                    for (TBrokerFileStatus fstatus : fileStatuses) {
                        LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                                          .add("file_status", fstatus)
                                          .build());
                    }
                }
            }

            ((BrokerPendingTaskAttachment) attachment).addFileStatus(tableId, fileStatusList);
        }
    }
}
