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

import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Status;
import org.apache.doris.thrift.TCalcDeleteBitmapPartitionInfo;
import org.apache.doris.thrift.TCalcDeleteBitmapRequest;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class CalcDeleteBitmapTask extends AgentTask  {
    private static final Logger LOG = LogManager.getLogger(CreateReplicaTask.class);
    // used for synchronous process
    private MarkedCountDownLatch<Long, Long> latch;
    private long transactionId;
    private List<TCalcDeleteBitmapPartitionInfo> partitionInfos;
    private List<Long> errorTablets;

    public CalcDeleteBitmapTask(long backendId, long transactionId, long dbId,
            List<TCalcDeleteBitmapPartitionInfo> partitionInfos,
            MarkedCountDownLatch<Long, Long> latch) {
        super(null, backendId, TTaskType.CALCULATE_DELETE_BITMAP, dbId, -1L, -1L, -1L, -1L, transactionId);
        this.transactionId = transactionId;
        this.partitionInfos = partitionInfos;
        this.errorTablets = new ArrayList<Long>();
        this.isFinished = false;
        this.latch = latch;
    }

    public void countDownLatch(long backendId, long transactionId) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, transactionId)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("CalcDeleteBitmapTask current latch count: {}, backend: {}, transactionId:{}",
                            latch.getCount(), backendId, transactionId);
                }
            }
        }
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            if (LOG.isDebugEnabled()) {
                LOG.debug("CalcDeleteBitmapTask download to zero. error msg: {}", errMsg);
            }
        }
    }

    public void countDownToZero(TStatusCode code, String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(code, errMsg));
            if (LOG.isDebugEnabled()) {
                LOG.debug("CalcDeleteBitmapTask download to zero. error msg: {}", errMsg);
            }
        }
    }

    public boolean isFinishRequestStale(List<TCalcDeleteBitmapPartitionInfo> respPartitionInfos) {
        return !respPartitionInfos.equals(partitionInfos);
    }

    public void setLatch(MarkedCountDownLatch<Long, Long> latch) {
        this.latch = latch;
    }

    public TCalcDeleteBitmapRequest toThrift() {
        TCalcDeleteBitmapRequest calcDeleteBitmapRequest = new TCalcDeleteBitmapRequest(transactionId,
                partitionInfos);
        return calcDeleteBitmapRequest;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public List<TCalcDeleteBitmapPartitionInfo> getCalcDeleteBimapPartitionInfos() {
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
