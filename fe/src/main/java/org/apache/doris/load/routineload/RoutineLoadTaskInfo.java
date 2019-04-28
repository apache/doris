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

package org.apache.doris.load.routineload;


import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TransactionState;

import java.util.UUID;

/**
 * Routine load task info is the task info include the only id (signature).
 * For the kafka type of task info, it also include partitions which will be obtained data in this task.
 * The routine load task info and routine load task are the same thing logically.
 * Differently, routine load task is a agent task include backendId which will execute this task.
 */
public abstract class RoutineLoadTaskInfo {
    
    private RoutineLoadManager routineLoadManager = Catalog.getCurrentCatalog().getRoutineLoadManager();
    
    protected UUID id;
    protected long txnId;
    protected long jobId;
    protected String clusterName;

    private long createTimeMs;
    private long loadStartTimeMs;
    // the be id of previous task
    protected long previousBeId = -1L;
    // the be id of this task
    protected long beId = -1L;

    public RoutineLoadTaskInfo(UUID id, long jobId, String clusterName) {
        this.id = id;
        this.jobId = jobId;
        this.clusterName = clusterName;
        this.createTimeMs = System.currentTimeMillis();
    }

    public RoutineLoadTaskInfo(UUID id, long jobId, String clusterName, long previousBeId) {
        this(id, jobId, clusterName);
        this.previousBeId = previousBeId;
    }
    
    public UUID getId() {
        return id;
    }
    
    public long getJobId() {
        return jobId;
    }
    
    public String getClusterName() {
        return clusterName;
    }

    public void setLoadStartTimeMs(long loadStartTimeMs) {
        this.loadStartTimeMs = loadStartTimeMs;
    }

    public long getPreviousBeId() {
        return previousBeId;
    }

    public void setBeId(long beId) {
        this.beId = beId;
    }

    public long getBeId() {
        return beId;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public long getLoadStartTimeMs() {
        return loadStartTimeMs;
    }
    
    public long getTxnId() {
        return txnId;
    }

    abstract TRoutineLoadTask createRoutineLoadTask() throws LoadException, UserException;

    public void beginTxn() throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException {
        // begin a txn for task
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(jobId);
        txnId = Catalog.getCurrentGlobalTransactionMgr().beginTransaction(
                routineLoadJob.getDbId(), DebugUtil.printId(id), -1, "streamLoad",
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, routineLoadJob.getId());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RoutineLoadTaskInfo) {
            RoutineLoadTaskInfo routineLoadTaskInfo = (RoutineLoadTaskInfo) obj;
            return this.id.toString().equals(routineLoadTaskInfo.getId().toString());
        } else {
            return false;
        }
    }
}
