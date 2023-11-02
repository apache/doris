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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.IOException;

/**
 * This object will be created when job finished or cancelled.
 * It is used to edit the job final state.
 */
public class LoadJobFinalOperation extends TxnCommitAttachment implements Writable {
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "loadingStatus")
    private EtlStatus loadingStatus = new EtlStatus();
    @SerializedName(value = "progress")
    private int progress;
    @SerializedName(value = "loadStartTimestamp")
    private long loadStartTimestamp;
    @SerializedName(value = "finishTimestamp")
    private long finishTimestamp;
    @SerializedName(value = "jobState")
    private JobState jobState;
    @SerializedName(value = "failMsg")
    // optional
    private FailMsg failMsg;

    public LoadJobFinalOperation() {
        super(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB);
    }

    public LoadJobFinalOperation(long id, EtlStatus loadingStatus, int progress, long loadStartTimestamp,
                                 long finishTimestamp, JobState jobState, FailMsg failMsg) {
        super(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB);
        this.id = id;
        this.loadingStatus = loadingStatus;
        this.progress = progress;
        this.loadStartTimestamp = loadStartTimestamp;
        this.finishTimestamp = finishTimestamp;
        this.jobState = jobState;
        this.failMsg = failMsg;
    }

    public long getId() {
        return id;
    }

    public EtlStatus getLoadingStatus() {
        return loadingStatus;
    }

    public int getProgress() {
        return progress;
    }

    public long getLoadStartTimestamp() {
        return loadStartTimestamp;
    }

    public long getFinishTimestamp() {
        return finishTimestamp;
    }

    public JobState getJobState() {
        return jobState;
    }

    public FailMsg getFailMsg() {
        return failMsg;
    }

    @Deprecated
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        id = in.readLong();
        loadingStatus.readFields(in);
        progress = in.readInt();
        loadStartTimestamp = in.readLong();
        finishTimestamp = in.readLong();
        jobState = JobState.valueOf(Text.readString(in));
        if (in.readBoolean()) {
            failMsg = new FailMsg();
            failMsg.readFields(in);
        }
    }

    @Override
    public String toString() {
        return "LoadJobEndOperation{"
                + "id=" + id
                + ", loadingStatus=" + loadingStatus
                + ", progress=" + progress
                + ", loadStartTimestamp=" + loadStartTimestamp
                + ", finishTimestamp=" + finishTimestamp
                + ", jobState=" + jobState
                + ", failMsg=" + failMsg
                + '}';
    }
}
