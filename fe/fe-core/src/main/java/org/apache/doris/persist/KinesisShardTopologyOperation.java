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
import org.apache.doris.proto.InternalService;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Discovery metadata only: replay never replaces committed progress or visibility barriers. */
public class KinesisShardTopologyOperation implements Writable {
    private static class ShardMetadata {
        @SerializedName("id")
        private String shardId;
        @SerializedName("parent")
        private String parentId;
        @SerializedName("adjacentParent")
        private String adjacentParentId;
        @SerializedName("closed")
        private boolean closed;
    }

    @SerializedName("jobId")
    private long jobId;
    @SerializedName("shards")
    private List<ShardMetadata> shards = new ArrayList<>();
    @SerializedName("defaultPosition")
    private String defaultPosition;
    @SerializedName("initialPositions")
    private Map<String, String> initialPositions;

    public KinesisShardTopologyOperation(long jobId, List<InternalService.PShardInfo> shardInfos,
            String defaultPosition, Map<String, String> initialPositions) {
        this.jobId = jobId;
        this.defaultPosition = defaultPosition;
        this.initialPositions = new HashMap<>(initialPositions);
        for (InternalService.PShardInfo info : shardInfos) {
            ShardMetadata metadata = new ShardMetadata();
            metadata.shardId = info.getShardId();
            metadata.parentId = info.hasParentShardId() ? info.getParentShardId() : null;
            metadata.adjacentParentId = info.hasAdjacentParentShardId() ? info.getAdjacentParentShardId() : null;
            metadata.closed = info.getClosed();
            shards.add(metadata);
        }
    }

    public long getJobId() {
        return jobId;
    }

    public List<InternalService.PShardInfo> getShardInfos() {
        List<InternalService.PShardInfo> infos = new ArrayList<>();
        for (ShardMetadata metadata : shards) {
            InternalService.PShardInfo.Builder builder = InternalService.PShardInfo.newBuilder()
                    .setShardId(metadata.shardId).setClosed(metadata.closed);
            if (metadata.parentId != null) {
                builder.setParentShardId(metadata.parentId);
            }
            if (metadata.adjacentParentId != null) {
                builder.setAdjacentParentShardId(metadata.adjacentParentId);
            }
            infos.add(builder.build());
        }
        return infos;
    }

    public String getDefaultPosition() {
        return defaultPosition;
    }

    public Map<String, String> getInitialPositions() {
        return initialPositions;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static KinesisShardTopologyOperation read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), KinesisShardTopologyOperation.class);
    }
}
