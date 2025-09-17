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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.cloud.proto.Cloud.StreamingTaskCommitAttachmentPB;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;

public class StreamingTaskTxnCommitAttachment extends TxnCommitAttachment {

    public StreamingTaskTxnCommitAttachment(long jobId, long taskId,
                long scannedRows, long loadBytes, long fileNumber, long fileSize, Offset offset) {
        super(TransactionState.LoadJobSourceType.STREAMING_JOB);
        this.jobId = jobId;
        this.taskId = taskId;
        this.scannedRows = scannedRows;
        this.loadBytes = loadBytes;
        this.fileNumber = fileNumber;
        this.fileSize = fileSize;
        this.offset = offset;
    }

    public StreamingTaskTxnCommitAttachment(StreamingTaskCommitAttachmentPB pb) {
        super(TransactionState.LoadJobSourceType.STREAMING_JOB);
        this.scannedRows = pb.getScannedRows();
        this.loadBytes = pb.getLoadBytes();
        this.fileNumber = pb.getFileNumber();
        this.fileSize = pb.getFileSize();
        this.offset.setEndOffset(pb.getOffset());
    }

    @Getter
    private long jobId;
    @Getter
    private long taskId;
    @SerializedName(value = "sr")
    @Getter
    private long scannedRows;
    @SerializedName(value = "lb")
    @Getter
    private long loadBytes;
    @SerializedName(value = "fn")
    @Getter
    private long fileNumber;
    @SerializedName(value = "fs")
    @Getter
    private long fileSize;
    @SerializedName(value = "of")
    @Getter
    private Offset offset;

    @Override
    public String toString() {
        return "StreamingTaskTxnCommitAttachment: ["
                + "scannedRows=" + scannedRows
                + ", loadBytes=" + loadBytes
                + ", fileNumber=" + fileNumber
                + ", fileSize=" + fileSize
                + ", offset=" + offset.toString()
                + "]";
    }
}
