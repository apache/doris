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

package org.apache.doris.mtmv;

import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshEnum.MTMVState;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class MTMVStatus {
    @SerializedName("s")
    private MTMVState state;
    @SerializedName("scd")
    private String schemaChangeDetail;
    @SerializedName("rs")
    private MTMVRefreshState refreshState;

    public MTMVStatus() {
        this.state = MTMVState.INIT;
        this.refreshState = MTMVRefreshState.INIT;
    }

    public MTMVStatus(MTMVState state, String schemaChangeDetail) {
        this.state = state;
        this.schemaChangeDetail = schemaChangeDetail;
    }

    public MTMVState getState() {
        return state;
    }

    public String getSchemaChangeDetail() {
        return schemaChangeDetail;
    }

    public MTMVRefreshState getRefreshState() {
        return refreshState;
    }

    public MTMVStatus updateNotNull(MTMVStatus status) {
        Objects.requireNonNull(status);
        if (status.getRefreshState() != null) {
            this.state = status.getState();
        }
        if (!StringUtils.isEmpty(status.getSchemaChangeDetail())) {
            this.schemaChangeDetail = status.getSchemaChangeDetail();
        }
        if (status.getRefreshState() != null) {
            this.refreshState = status.getRefreshState();
        }
        return this;
    }
}
