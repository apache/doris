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

import org.apache.doris.thrift.TRLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TxnCommitAttachment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// {"progress": "", "backendId": "", "taskSignature": "", "numOfErrorData": "",
// "numOfTotalData": "", "taskId": "", "jobId": ""}
public class RLTaskTxnCommitAttachment extends TxnCommitAttachment {

    public enum LoadSourceType {
        KAFKA(1);

        private final int flag;

        private LoadSourceType(int flag) {
            this.flag = flag;
        }

        public int value() {
            return flag;
        }

        public static LoadSourceType valueOf(int flag) {
            switch (flag) {
                case 1:
                    return KAFKA;
                default:
                    return null;
            }
        }
    }

    private long jobId;
    private TUniqueId taskId;
    private long filteredRows;
    private long loadedRows;
    private RoutineLoadProgress progress;
    private LoadSourceType loadSourceType;

    public RLTaskTxnCommitAttachment() {
    }

    public RLTaskTxnCommitAttachment(TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment) {
        this.jobId = rlTaskTxnCommitAttachment.getJobId();
        this.taskId = rlTaskTxnCommitAttachment.getId();
        this.filteredRows = rlTaskTxnCommitAttachment.getFilteredRows();
        this.loadedRows = rlTaskTxnCommitAttachment.getLoadedRows();

        switch (rlTaskTxnCommitAttachment.getLoadSourceType()) {
            case KAFKA:
                this.loadSourceType = LoadSourceType.KAFKA;
                this.progress = new KafkaProgress(rlTaskTxnCommitAttachment.getKafkaRLTaskProgress());
            default:
                break;
        }
    }

    public long getJobId() {
        return jobId;
    }

    public TUniqueId getTaskId() {
        return taskId;
    }

    public long getFilteredRows() {
        return filteredRows;
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public RoutineLoadProgress getProgress() {
        return progress;
    }

    @Override
    public String toString() {
        return "RoutineLoadTaskTxnExtra [filteredRows=" + filteredRows
                + ", loadedRows=" + loadedRows
                + ", taskId=" + taskId
                + ", jobId=" + jobId
                + ", progress=" + progress.toString() + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO: think twice
    }

    @Override
    public void readFields(DataInput in) throws IOException {
     // TODO: think twice
    }
}
