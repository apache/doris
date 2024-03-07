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

package org.apache.doris.statistics;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisTaskInfo implements Writable {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskInfo.class);

    @SerializedName("jobId")
    public final long jobId;

    @SerializedName("taskId")
    public final long taskId;

    public AnalysisTaskInfo(long jobId, long taskId) {
        this.jobId = jobId;
        this.taskId = taskId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static AnalysisTaskInfo read(DataInput dataInput) throws IOException {
        String json = Text.readString(dataInput);
        AnalysisTaskInfo analysisTaskInfo = GsonUtils.GSON.fromJson(json, AnalysisTaskInfo.class);
        return analysisTaskInfo;
    }
}
