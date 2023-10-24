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

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * PersistInfo for ColocateTableIndex.
 */
public class ColocatePersistInfo implements Writable {
    @SerializedName(value = "groupId")
    private GroupId groupId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "backendsPerBucketSeq")
    private Map<Tag, List<List<Long>>> backendsPerBucketSeq = Maps.newHashMap();

    private ColocatePersistInfo(GroupId groupId, long tableId, Map<Tag, List<List<Long>>> backendsPerBucketSeq) {
        this.groupId = groupId;
        this.tableId = tableId;
        this.backendsPerBucketSeq = backendsPerBucketSeq;
    }

    public static ColocatePersistInfo createForAddTable(GroupId groupId, long tableId,
            Map<Tag, List<List<Long>>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(groupId, tableId, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo createForBackendsPerBucketSeq(GroupId groupId,
            Map<Tag, List<List<Long>>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(groupId, -1L, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo createForMarkUnstable(GroupId groupId) {
        return new ColocatePersistInfo(groupId, -1L, Maps.newHashMap());
    }

    public static ColocatePersistInfo createForMarkStable(GroupId groupId) {
        return new ColocatePersistInfo(groupId, -1L, Maps.newHashMap());
    }

    public static ColocatePersistInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ColocatePersistInfo.class);
    }

    public long getTableId() {
        return tableId;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public Map<Tag, List<List<Long>>> getBackendsPerBucketSeq() {
        return backendsPerBucketSeq;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException {
        tableId = in.readLong();
        groupId = GroupId.read(in);

        int size = in.readInt();
        backendsPerBucketSeq = Maps.newHashMap();
        List<List<Long>> backendsPerBucketSeqList = Lists.newArrayList();
        backendsPerBucketSeq.put(Tag.DEFAULT_BACKEND_TAG, backendsPerBucketSeqList);
        for (int i = 0; i < size; i++) {
            int beListSize = in.readInt();
            List<Long> beLists = new ArrayList<>();
            for (int j = 0; j < beListSize; j++) {
                beLists.add(in.readLong());
            }
            backendsPerBucketSeqList.add(beLists);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, tableId, backendsPerBucketSeq);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ColocatePersistInfo)) {
            return false;
        }

        ColocatePersistInfo info = (ColocatePersistInfo) obj;

        return tableId == info.tableId && groupId.equals(info.groupId) && backendsPerBucketSeq.equals(
                info.backendsPerBucketSeq);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table id: ").append(tableId);
        sb.append(" group id: ").append(groupId);
        sb.append(" backendsPerBucketSeq: ").append(backendsPerBucketSeq);
        return sb.toString();
    }
}
