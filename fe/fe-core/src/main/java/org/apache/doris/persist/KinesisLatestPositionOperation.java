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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Initial LATEST positions persisted before any data task uses them. */
public class KinesisLatestPositionOperation implements Writable {
    @SerializedName("jobId")
    private long jobId;
    @SerializedName("shardPositions")
    private Map<String, String> shardPositions;

    public KinesisLatestPositionOperation(long jobId, Map<String, String> shardPositions) {
        this.jobId = jobId;
        this.shardPositions = new HashMap<>(shardPositions);
    }

    public long getJobId() {
        return jobId;
    }

    public Map<String, String> getShardPositions() {
        return shardPositions;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static KinesisLatestPositionOperation read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), KinesisLatestPositionOperation.class);
    }
}
