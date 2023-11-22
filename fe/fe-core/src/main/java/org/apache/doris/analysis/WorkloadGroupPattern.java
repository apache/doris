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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WorkloadGroupPattern implements Writable {

    @SerializedName(value = "workloadGroupName")
    private String workloadGroupName;

    private WorkloadGroupPattern() {
    }

    public WorkloadGroupPattern(String workloadGroupName) {
        this.workloadGroupName = workloadGroupName;
    }

    public String getworkloadGroupName() {
        return workloadGroupName;
    }

    public PrivLevel getPrivLevel() {
        return PrivLevel.WORKLOAD_GROUP;
    }

    public void analyze() throws AnalysisException {
        if (Strings.isNullOrEmpty(workloadGroupName)) {
            throw new AnalysisException("Workload group name is empty.");
        }
        if (workloadGroupName.equals("*")) {
            throw new AnalysisException("Global workload group priv is not supported.");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WorkloadGroupPattern)) {
            return false;
        }
        WorkloadGroupPattern other = (WorkloadGroupPattern) obj;
        return workloadGroupName.equals(other.getworkloadGroupName());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + workloadGroupName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return workloadGroupName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static WorkloadGroupPattern read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WorkloadGroupPattern.class);
    }
}
