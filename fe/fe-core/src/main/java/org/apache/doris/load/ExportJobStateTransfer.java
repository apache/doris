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

package org.apache.doris.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
public class ExportJobStateTransfer implements Writable {
    @SerializedName("jobId")
    long jobId;
    @SerializedName("state")
    private ExportJobState state;
    @SerializedName("startTimeMs")
    private long startTimeMs;
    @SerializedName("finishTimeMs")
    private long finishTimeMs;
    @SerializedName("failMsg")
    private ExportFailMsg failMsg;
    @SerializedName("outFileInfo")
    private String outFileInfo;

    // used for reading from one log
    public ExportJobStateTransfer() {
        this.jobId = -1;
        this.state = ExportJobState.CANCELLED;
        this.failMsg = new ExportFailMsg(ExportFailMsg.CancelType.UNKNOWN, "");
        this.outFileInfo = "";
    }

    // used for persisting one log
    public ExportJobStateTransfer(long jobId, ExportJobState state) {
        this.jobId = jobId;
        this.state = state;
        ExportJob job = Env.getCurrentEnv().getExportMgr().getJob(jobId);
        this.startTimeMs = job.getStartTimeMs();
        this.finishTimeMs = job.getFinishTimeMs();
        this.failMsg = job.getFailMsg();
        this.outFileInfo = job.getOutfileInfo();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ExportJobStateTransfer read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_120) {
            ExportJobStateTransfer transfer = new ExportJobStateTransfer();
            transfer.readFields(in);
            return transfer;
        }
        String json = Text.readString(in);
        ExportJobStateTransfer transfer = GsonUtils.GSON.fromJson(json, ExportJobStateTransfer.class);
        return transfer;
    }

    private void readFields(DataInput in) throws IOException {
        jobId = in.readLong();
        state = ExportJobState.valueOf(Text.readString(in));
    }
}
