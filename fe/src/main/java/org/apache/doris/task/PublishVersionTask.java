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

import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TPublishVersionRequest;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class PublishVersionTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PublishVersionTask.class);

    private long transactionId;
    private List<TPartitionVersionInfo> partitionVersionInfos;
    private List<Long> errorTablets;
    private boolean isFinished;

    public PublishVersionTask(long backendId, long transactionId, long dbId,
            List<TPartitionVersionInfo> partitionVersionInfos) {
        super(null, backendId, TTaskType.PUBLISH_VERSION, dbId, -1L, -1L, -1L, -1L, transactionId);
        this.transactionId = transactionId;
        this.partitionVersionInfos = partitionVersionInfos;
        this.errorTablets = new ArrayList<Long>();
        this.isFinished = false;
    }
    
    public TPublishVersionRequest toThrift() {
        TPublishVersionRequest publishVersionRequest = new TPublishVersionRequest(transactionId, 
                partitionVersionInfos);
        return publishVersionRequest;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public List<TPartitionVersionInfo> getPartitionVersionInfos() {
        return partitionVersionInfos;
    }

    public synchronized List<Long> getErrorTablets() {
        return errorTablets;
    }
    
    public synchronized void addErrorTablets(List<Long> errorTablets) {
        this.errorTablets.clear();
        if (errorTablets == null) {
            return;
        }
        this.errorTablets.addAll(errorTablets);
    }
    
    public void setIsFinished(boolean isFinished) {
        this.isFinished = isFinished;
    }
    
    public boolean isFinished() {
        return isFinished;
    }
}
