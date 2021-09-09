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
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// ReplicaAllocation is used to describe the distribution of replicas of a tablet.
// By default, 3 replicas of a tablet are distributed on 3 BE nodes with Tag "default".
public class ReplicaAllocation implements Writable {

    public static final ReplicaAllocation DEFAULT_ALLOCATION;
    // represent that replica allocation is not set.
    public static final ReplicaAllocation NOT_SET;

    static {
        DEFAULT_ALLOCATION = new ReplicaAllocation((short) 3);
        NOT_SET = new ReplicaAllocation();
    }

    @SerializedName(value = "allocMap")
    private Map<Tag, Short> allocMap = Maps.newHashMap();

    public ReplicaAllocation() {

    }

    // For convert the old replica number to replica allocation
    public ReplicaAllocation(short replicaNum) {
        allocMap.put(Tag.DEFAULT_BACKEND_TAG, replicaNum);
    }

    public ReplicaAllocation(Map<Tag, Short> allocMap) {
        this.allocMap = allocMap;
    }

    public void put(Tag tag, Short num) {
        this.allocMap.put(tag, num);
    }

    public Map<Tag, Short> getAllocMap() {
        return allocMap;
    }

    public short getTotalReplicaNum() {
        short num = 0;
        for (Short s : allocMap.values()) {
            num += s;
        }
        return num;
    }

    public boolean isEmpty() {
        return allocMap.isEmpty();
    }

    public boolean isNotSet() {
        return this.equals(NOT_SET);
    }

    public Short getReplicaNumByTag(Tag tag) {
        return allocMap.getOrDefault(tag, (short) 0);
    }

    public static ReplicaAllocation read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ReplicaAllocation.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaAllocation that = (ReplicaAllocation) o;
        return that.allocMap.equals(this.allocMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allocMap);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public String toString() {
        return toCreateStmt();
    }

    // For show create table stmt. like:
    // "tag.location.zone1: 2, tag.location.zone2: 1"
    public String toCreateStmt() {
        List<String> tags = Lists.newArrayList();
        for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
            tags.add(PropertyAnalyzer.TAG_LOCATION + "." + entry.getKey().value + ": " + entry.getValue());
        }
        return Joiner.on(", ").join(tags);
    }
}
