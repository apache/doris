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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConsistencyCheckInfo implements Writable {

    @SerializedName("db")
    private long dbId;
    @SerializedName("tb")
    private long tableId;
    @SerializedName("p")
    private long partitionId;
    @SerializedName("ind")
    private long indexId;
    @SerializedName("tab")
    private long tabletId;

    @SerializedName("t")
    private long lastCheckTime;

    @SerializedName("v")
    private long checkedVersion;

    @Deprecated
    @Expose(serialize = false, deserialize = false)
    private long checkedVersionHash;

    @SerializedName("isC")
    private boolean isConsistent;

    public ConsistencyCheckInfo() {
        // for persist
    }

    public ConsistencyCheckInfo(long dbId, long tableId, long partitionId, long indexId, long tabletId,
                                long lastCheckTime, long checkedVersion, boolean isConsistent) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
        this.tabletId = tabletId;

        this.lastCheckTime = lastCheckTime;
        this.checkedVersion = checkedVersion;

        this.isConsistent = isConsistent;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getLastCheckTime() {
        return lastCheckTime;
    }

    public long getCheckedVersion() {
        return checkedVersion;
    }

    public boolean isConsistent() {
        return isConsistent;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ConsistencyCheckInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_134) {
            ConsistencyCheckInfo info = new ConsistencyCheckInfo();
            info.readFields(in);
            return info;
        }
        return GsonUtils.GSON.fromJson(Text.readString(in), ConsistencyCheckInfo.class);
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();
        indexId = in.readLong();
        tabletId = in.readLong();

        lastCheckTime = in.readLong();
        checkedVersion = in.readLong();
        checkedVersionHash = in.readLong();

        isConsistent = in.readBoolean();
    }
}
