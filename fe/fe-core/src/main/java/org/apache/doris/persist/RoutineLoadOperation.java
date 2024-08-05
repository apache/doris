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

package org.apache.doris.persist;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.routineload.ErrorReason;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RoutineLoadOperation implements Writable {
    @SerializedName("id")
    private long id;
    @SerializedName("js")
    private JobState jobState;
    @SerializedName("rs")
    private ErrorReason reason;

    private RoutineLoadOperation() {
    }

    public RoutineLoadOperation(long id, JobState jobState) {
        this.id = id;
        this.jobState = jobState;
    }

    public RoutineLoadOperation(long id, JobState jobState, ErrorReason reason) {
        this.id = id;
        this.jobState = jobState;
        this.reason = reason;
    }

    public long getId() {
        return id;
    }

    public JobState getJobState() {
        return jobState;
    }

    public ErrorReason getErrorReason() {
        return reason;
    }

    public static RoutineLoadOperation read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_137) {
            RoutineLoadOperation operation = new RoutineLoadOperation();
            operation.readFields(in);
            return operation;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), RoutineLoadOperation.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        jobState = JobState.valueOf(Text.readString(in));
    }
}
