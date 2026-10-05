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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * used for batch modify multi partitions in one atomic operation
 */

public class BatchModifyPartitionsInfo implements Writable {
    @SerializedName(value = "infos")
    private List<ModifyPartitionInfo> infos;

    public BatchModifyPartitionsInfo(List<ModifyPartitionInfo> infos) {
        if (infos == null || infos.isEmpty()) {
            throw new IllegalArgumentException("infos is null or empty");
        }

        long dbId = infos.get(0).getDbId();
        long tableId = infos.get(0).getTableId();
        for (ModifyPartitionInfo info : infos) {
            if (info.getDbId() != dbId || info.getTableId() != tableId) {
                throw new IllegalArgumentException("dbId or tableId is not equal");
            }
        }

        this.infos = infos;
    }

    public BatchModifyPartitionsInfo(ModifyPartitionInfo info) {
        if (info == null) {
            throw new IllegalArgumentException("info is null");
        }

        this.infos = Lists.newArrayList();
        this.infos.add(info);
    }

    public long getDbId() {
        return infos.get(0).getDbId();
    }

    public long getTableId() {
        return infos.get(0).getTableId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static BatchModifyPartitionsInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BatchModifyPartitionsInfo.class);
    }

    public List<ModifyPartitionInfo> getModifyPartitionInfos() {
        return infos;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BatchModifyPartitionsInfo)) {
            return false;
        }
        List<ModifyPartitionInfo> otherInfos = ((BatchModifyPartitionsInfo) other).getModifyPartitionInfos();
        for (ModifyPartitionInfo thisInfo : infos) {
            for (ModifyPartitionInfo otherInfo : otherInfos) {
                if (!thisInfo.equals(otherInfo)) {
                    return false;
                }
            }
        }
        return true;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }
}
