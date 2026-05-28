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
import java.util.Objects;
import java.util.Set;

/**
 * PersistInfo for ColocateTableIndex.
 */
public class TenantLevelColocateStableInfo implements Writable {
    @SerializedName(value = "groupSet")
    private Set<Long> groupSet;

    public TenantLevelColocateStableInfo() {
    }

    public TenantLevelColocateStableInfo(Set<Long> groupSet) {
        this.groupSet = groupSet;
    }

    public static TenantLevelColocateStableInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TenantLevelColocateStableInfo.class);
    }

    public Set<Long> getGroupSet() {
        return groupSet;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        TenantLevelColocateStableInfo that = (TenantLevelColocateStableInfo) object;
        return Objects.equals(groupSet, that.groupSet);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(groupSet);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" group: ").append(groupSet);
        return sb.toString();
    }
}
