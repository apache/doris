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

package org.apache.doris.common;

import org.apache.doris.common.util.TimeUtils;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * Common persisted audit metadata for user-driven DDL objects.
 * Time is stored as epoch millis and formatted lazily with Doris/session timezone when needed.
 */
public abstract class UserAuditMetadata {
    @SerializedName(value = "cu")
    private String createUser;
    @SerializedName(value = "ct")
    private long createTime;
    @SerializedName(value = "au")
    private String alterUser;
    @SerializedName(value = "mt")
    private long modifyTime;

    protected UserAuditMetadata() {
    }

    protected UserAuditMetadata(String createUser, long createTime, String alterUser, long modifyTime) {
        this.createUser = Objects.requireNonNull(createUser, "createUser can not be null");
        this.alterUser = Objects.requireNonNull(alterUser, "alterUser can not be null");
        Preconditions.checkArgument(createTime > 0, "createTime must be positive");
        Preconditions.checkArgument(modifyTime > 0, "modifyTime must be positive");
        Preconditions.checkArgument(modifyTime >= createTime, "modifyTime can not be earlier than createTime");
        this.createTime = createTime;
        this.modifyTime = modifyTime;
    }

    public String getCreateUser() {
        return createUser;
    }

    public long getCreateTime() {
        return createTime;
    }

    public String getAlterUser() {
        return alterUser;
    }

    public long getModifyTime() {
        return modifyTime;
    }

    public String getCreateTimeString() {
        return TimeUtils.longToTimeString(createTime);
    }

    public String getModifyTimeString() {
        return TimeUtils.longToTimeString(modifyTime);
    }
}
