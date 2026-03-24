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

import org.apache.doris.thrift.TCalcDeleteBitmapAsyncPublishRequest;
import org.apache.doris.thrift.TCalcDeleteBitmapPartitionInfo;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Task for calculating delete bitmap during async publish phase.
 * This task stays in AgentTaskQueue on failure for heartbeat auto-retry.
 */
public class CalcDeleteBitmapAsyncPublishTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(CalcDeleteBitmapAsyncPublishTask.class);

    private long transactionId;
    private List<TCalcDeleteBitmapPartitionInfo> partitionInfos;
    private List<Long> errorTablets;

    public CalcDeleteBitmapAsyncPublishTask(long backendId, long transactionId, long dbId,
            List<TCalcDeleteBitmapPartitionInfo> partitionInfos, long signature) {
        super(null, backendId, TTaskType.CALC_DELETE_BITMAP_ASYNC_PUBLISH,
                dbId, -1L, -1L, -1L, -1L, signature);
        this.transactionId = transactionId;
        this.partitionInfos = partitionInfos;
        this.errorTablets = new ArrayList<Long>();
        this.isFinished = false;
    }

    public boolean isFinishRequestStale(List<TCalcDeleteBitmapPartitionInfo> respPartitionInfos) {
        return !respPartitionInfos.equals(partitionInfos);
    }

    public TCalcDeleteBitmapAsyncPublishRequest toThrift() {
        return new TCalcDeleteBitmapAsyncPublishRequest(transactionId, partitionInfos);
    }

    public long getTransactionId() {
        return transactionId;
    }

    public List<TCalcDeleteBitmapPartitionInfo> getPartitionInfos() {
        return partitionInfos;
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
