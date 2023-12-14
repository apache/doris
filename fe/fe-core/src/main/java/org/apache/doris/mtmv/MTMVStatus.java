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

import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class MTMVStatus {
    @SerializedName("state")
    private MTMVState state;
    @SerializedName("schemaChangeDetail")
    private String schemaChangeDetail;
    @SerializedName("refreshState")
    private MTMVRefreshState refreshState;

    public MTMVStatus() {
        this.state = MTMVState.INIT;
        this.refreshState = MTMVRefreshState.INIT;
    }

    public MTMVStatus(MTMVState state, String schemaChangeDetail) {
        this.state = state;
        this.schemaChangeDetail = schemaChangeDetail;
    }

    public MTMVStatus(MTMVRefreshState refreshState) {
        this.refreshState = refreshState;
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

    public void setState(MTMVState state) {
        this.state = state;
    }

    public void setSchemaChangeDetail(String schemaChangeDetail) {
        this.schemaChangeDetail = schemaChangeDetail;
    }

    public void setRefreshState(MTMVRefreshState refreshState) {
        this.refreshState = refreshState;
    }

    public MTMVStatus updateNotNull(MTMVStatus status) {
        Objects.requireNonNull(status);
        if (status.getState() != null) {
            this.state = status.getState();
            if (this.state == MTMVState.SCHEMA_CHANGE) {
                this.schemaChangeDetail = status.getSchemaChangeDetail();
            } else {
                this.schemaChangeDetail = null;
            }
        }
        if (status.getRefreshState() != null) {
            this.refreshState = status.getRefreshState();
        }
        return this;
    }

    @Override
    public String toString() {
        return "MTMVStatus{"
                + "state=" + state
                + ", schemaChangeDetail='" + schemaChangeDetail + '\''
                + ", refreshState=" + refreshState
                + '}';
    }
}
