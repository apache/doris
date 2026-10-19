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

import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TableStreamCleanupInfo implements Writable {
    @SerializedName(value = "entries")
    private List<PartitionOffsetPruneEntry> entries;

    // db ids whose stream mapping should be removed from TableStreamManager.dbStreamMap
    @SerializedName(value = "staleDbIds")
    private List<Long> staleDbIds;

    // (dbId, streamId) pairs whose stream mapping should be removed from TableStreamManager.dbStreamMap
    @SerializedName(value = "staleStreamIds")
    private List<Pair<Long, Long>> staleStreamIds;

    public TableStreamCleanupInfo(List<PartitionOffsetPruneEntry> entries) {
        this(entries, new ArrayList<>(), new ArrayList<>());
    }

    public TableStreamCleanupInfo(List<PartitionOffsetPruneEntry> entries, List<Long> staleDbIds,
            List<Pair<Long, Long>> staleStreamIds) {
        this.entries = entries;
        this.staleDbIds = staleDbIds;
        this.staleStreamIds = staleStreamIds;
    }

    public List<PartitionOffsetPruneEntry> getEntries() {
        return entries == null ? Collections.emptyList() : entries;
    }

    public List<Long> getStaleDbIds() {
        return staleDbIds == null ? Collections.emptyList() : staleDbIds;
    }

    public List<Pair<Long, Long>> getStaleStreamIds() {
        return staleStreamIds == null ? Collections.emptyList() : staleStreamIds;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableStreamCleanupInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TableStreamCleanupInfo.class);
    }

    public static class PartitionOffsetPruneEntry {
        @SerializedName(value = "dbId")
        private long dbId;

        @SerializedName(value = "streamId")
        private long streamId;

        @SerializedName(value = "partitionIds")
        private Set<Long> partitionIds;

        public PartitionOffsetPruneEntry(long dbId, long streamId, Set<Long> partitionIds) {
            this.dbId = dbId;
            this.streamId = streamId;
            this.partitionIds = partitionIds;
        }

        public long getDbId() {
            return dbId;
        }

        public long getStreamId() {
            return streamId;
        }

        public Set<Long> getPartitionIds() {
            return partitionIds;
        }
    }
}
