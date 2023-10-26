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
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AlterMTMV implements Writable {
    @SerializedName("mn")
    private TableNameInfo mvName;
    @SerializedName("ri")
    private MTMVRefreshInfo refreshInfo;
    @SerializedName("s")
    private MTMVStatus status;
    @SerializedName("nrj")
    private boolean needRebuildJob = false;

    public AlterMTMV(TableNameInfo mvName, MTMVRefreshInfo refreshInfo, boolean needRebuildJob) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.refreshInfo = Objects.requireNonNull(refreshInfo, "require refreshInfo object");
        this.needRebuildJob = needRebuildJob;
    }

    public AlterMTMV(TableNameInfo mvName, MTMVStatus status) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.status = Objects.requireNonNull(status, "require status object");
    }

    public TableNameInfo getMvName() {
        return mvName;
    }

    public MTMVStatus getStatus() {
        return status;
    }

    public boolean isNeedRebuildJob() {
        return needRebuildJob;
    }

    public MTMVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterMTMV read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AlterMTMV.class);
    }

}
