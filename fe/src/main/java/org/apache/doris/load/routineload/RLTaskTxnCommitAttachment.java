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

import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TRLTaskTxnCommitAttachment;
import org.apache.doris.transaction.TxnCommitAttachment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// {"progress": "", "backendId": "", "taskSignature": "", "numOfErrorData": "",
// "numOfTotalData": "", "taskId": "", "jobId": ""}
public class RLTaskTxnCommitAttachment extends TxnCommitAttachment {

    public enum RoutineLoadType {
        KAFKA(1);

        private final int flag;

        private RoutineLoadType(int flag) {
            this.flag = flag;
        }

        public int value() {
            return flag;
        }

        public static RoutineLoadType valueOf(int flag) {
            switch (flag) {
                case 1:
                    return KAFKA;
                default:
                    return null;
            }
        }
    }

    private RoutineLoadProgress progress;
    private long backendId;
    private long taskSignature;
    private int numOfErrorData;
    private int numOfTotalData;
    private String taskId;
    private String jobId;
    private RoutineLoadType routineLoadType;

    public RLTaskTxnCommitAttachment() {
    }

    public RLTaskTxnCommitAttachment(TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment) {
        this.backendId = rlTaskTxnCommitAttachment.getBackendId();
        this.taskSignature = rlTaskTxnCommitAttachment.getTaskSignature();
        this.numOfErrorData = rlTaskTxnCommitAttachment.getNumOfErrorData();
        this.numOfTotalData = rlTaskTxnCommitAttachment.getNumOfTotalData();
        this.taskId = rlTaskTxnCommitAttachment.getTaskId();
        this.jobId = rlTaskTxnCommitAttachment.getJobId();
        switch (rlTaskTxnCommitAttachment.getRoutineLoadType()) {
            case KAFKA:
                this.progress = new KafkaProgress(rlTaskTxnCommitAttachment.getKafkaRLTaskProgress());
        }
    }

    public RoutineLoadProgress getProgress() {
        return progress;
    }

    public void setProgress(RoutineLoadProgress progress) {
        this.progress = progress;
    }

    public long getBackendId() {
        return backendId;
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public long getTaskSignature() {
        return taskSignature;
    }

    public void setTaskSignature(long taskSignature) {
        this.taskSignature = taskSignature;
    }

    public int getNumOfErrorData() {
        return numOfErrorData;
    }

    public void setNumOfErrorData(int numOfErrorData) {
        this.numOfErrorData = numOfErrorData;
    }

    public int getNumOfTotalData() {
        return numOfTotalData;
    }

    public void setNumOfTotalData(int numOfTotalData) {
        this.numOfTotalData = numOfTotalData;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString() {
        return "RoutineLoadTaskTxnExtra [backendId=" + backendId
                + ", taskSignature=" + taskSignature
                + ", numOfErrorData=" + numOfErrorData
                + ", numOfTotalData=" + numOfTotalData
                + ", taskId=" + taskId
                + ", jobId=" + jobId
                + ", progress=" + progress.toString() + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(backendId);
        out.writeLong(taskSignature);
        out.writeInt(numOfErrorData);
        out.writeInt(numOfTotalData);
        Text.writeString(out, taskId);
        Text.writeString(out, jobId);
        out.writeInt(routineLoadType.value());
        progress.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        backendId = in.readLong();
        taskSignature = in.readLong();
        numOfErrorData = in.readInt();
        numOfTotalData = in.readInt();
        taskId = Text.readString(in);
        jobId = Text.readString(in);
        routineLoadType = RoutineLoadType.valueOf(in.readInt());
        switch (routineLoadType) {
            case KAFKA:
                KafkaProgress kafkaProgress = new KafkaProgress();
                kafkaProgress.readFields(in);
                progress = kafkaProgress;
        }
    }
}
