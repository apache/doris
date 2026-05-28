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

import org.apache.doris.catalog.TenantLevelColocateGroupSchema;
import org.apache.doris.resource.Tag;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

public class TenantLevelColocateGroupInfo {
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "tag")
    private Tag tag;
    @SerializedName(value = "backendsPerBucketSeq")
    private List<List<Long>> backendsPerBucketSeq = new ArrayList<>();


    public static TenantLevelColocateGroupInfo create(String name, Tag tag, List<List<Long>> backendsPerBucketSeq) {
        TenantLevelColocateGroupInfo groupInfo = new TenantLevelColocateGroupInfo();
        groupInfo.setName(name);
        groupInfo.setTag(tag);
        groupInfo.setBackendsPerBucketSeq(backendsPerBucketSeq);
        return groupInfo;
    }

    public static TenantLevelColocateGroupInfo create(TenantLevelColocateGroupSchema groupSchema,
            List<List<Long>> backendsPerBucketSeq) {
        return create(groupSchema.getName(), groupSchema.getTag(), backendsPerBucketSeq);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Tag getTag() {
        return tag;
    }

    public void setTag(Tag tag) {
        this.tag = tag;
    }

    public List<List<Long>> getBackendsPerBucketSeq() {
        return backendsPerBucketSeq;
    }

    public void setBackendsPerBucketSeq(List<List<Long>> backendsPerBucketSeq) {
        this.backendsPerBucketSeq = backendsPerBucketSeq;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("name: ").append(name);
        sb.append(", tag: ").append(tag);
        sb.append(", backendsPerBucketSeq: ").append(backendsPerBucketSeq);
        sb.append("}");
        return sb.toString();
    }
}
