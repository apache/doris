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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.TagSet;
import org.apache.doris.system.Backend;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/*
 * This class represent the allocation of replicas.
 * This class is not thread safe
 */
public class ReplicaAllocation implements Writable {
    public enum AllocationType {
        LOCAL, // replica is allocated on local Backend
        REMOTE // replica is allocated on remote storage
    }

    @SerializedName(value = "typeToTag")
    // allocation type -> (tags -> replication num)
    private Table<AllocationType, TagSet, Short> typeToTag = HashBasedTable.create();

    // return the default allocation with specified replication num
    public static ReplicaAllocation createDefault(short replicaNum, String cluster) {
        ReplicaAllocation replicaAlloc = new ReplicaAllocation();
        TagSet tagSet = TagSet.copyFrom(Backend.DEFAULT_TAG_SET);
        tagSet.substituteMerge(TagSet.create(Tag.createNoThrow(Tag.TYPE_LOCATION, cluster)));
        replicaAlloc.setReplica(AllocationType.LOCAL, tagSet, replicaNum);
        return replicaAlloc;
    }

    public ReplicaAllocation() {

    }

    public ReplicaAllocation(ReplicaAllocation other) {
        typeToTag = HashBasedTable.create(other.typeToTag);
    }

    // set replication num by give allocation type and set of tags.
    // it will overwrite existing entry
    public void setReplica(AllocationType type, TagSet tagSet, short num) {
        typeToTag.put(type, tagSet, num);
    }

    // return the total replica num of given allocation type
    public short getReplicaNumByType(AllocationType type) {
        if (!typeToTag.containsRow(type)) {
            return 0;
        }
        return (short) typeToTag.row(type).values().stream().mapToInt(Short::valueOf).sum();
    }

    // return the total replica num
    public short getReplicaNum() {
        return (short) typeToTag.values().stream().mapToInt(Short::valueOf).sum();
    }

    // return a map of (tagset -> replication num).
    // return a empty map if there is no such replica allocation type
    public Map<TagSet, Short> getTagMapByType(AllocationType type) {
        if (!typeToTag.containsRow(type)) {
            return Maps.newHashMap();
        }
        return Maps.newHashMap(typeToTag.row(type));
    }

    // return true if 2 replica allocations are same
    public boolean isSameAlloc(ReplicaAllocation allocation) {
        return typeToTag.equals(allocation.typeToTag);
    }

    public static ReplicaAllocation read(DataInput in) throws IOException {
        String json = Text.readString(in);
        ReplicaAllocation allocation = GsonUtils.GSON.fromJson(json, ReplicaAllocation.class);
        return allocation;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // TODO
        return sb.toString();
    }
}
