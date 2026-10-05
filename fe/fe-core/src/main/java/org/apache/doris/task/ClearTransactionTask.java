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

import org.apache.doris.thrift.TClearTransactionTaskRequest;
import org.apache.doris.thrift.TTaskType;

import java.util.List;

public class ClearTransactionTask extends AgentTask {

    private long transactionId;
    private List<Long> partitionIds;

    public ClearTransactionTask(long backendId, long transactionId, List<Long> partitionIds) {
        super(null, backendId, TTaskType.CLEAR_TRANSACTION_TASK, -1L, -1L, -1L, -1L, -1L, transactionId);
        this.transactionId = transactionId;
        this.partitionIds = partitionIds;
        this.isFinished = false;
    }

    public TClearTransactionTaskRequest toThrift() {
        TClearTransactionTaskRequest clearTransactionTaskRequest = new TClearTransactionTaskRequest(
                transactionId, partitionIds);
        return clearTransactionTaskRequest;
    }
}
