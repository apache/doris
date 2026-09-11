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

package org.apache.doris.datasource.lance.job;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Batch removal record of the Lance index job retention GC: one journal entry
 * carries every resolved job id removed in one clean round. A plain id list
 * suffices (no watermark): the journal is ordered, so every upsert of a removed
 * job precedes this record, and no later stale record can resurrect it.
 * Serialization is the standard Gson stream, see {@link LanceIndexJob}.
 */
public class LanceIndexJobRemoveOperation implements Writable {
    @SerializedName(value = "jids")
    private List<Long> jobIds;

    public LanceIndexJobRemoveOperation(List<Long> jobIds) {
        this.jobIds = jobIds;
    }

    public List<Long> getJobIds() {
        return jobIds;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static LanceIndexJobRemoveOperation read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), LanceIndexJobRemoveOperation.class);
    }
}
