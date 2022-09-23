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

import org.apache.doris.analysis.MVRefreshInfo;
import org.apache.doris.analysis.MVRefreshInfo.BuildMode;
import org.apache.doris.catalog.OlapTableFactory.MaterializedViewParams;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaterializedView extends OlapTable {
    @SerializedName("buildMode")
    private BuildMode buildMode;
    @SerializedName("refreshInfo")
    private MVRefreshInfo refreshInfo;
    @SerializedName("query")
    private String query;

    // For deserialization
    public MaterializedView() {
        type = TableType.MATERIALIZED_VIEW;
    }

    MaterializedView(MaterializedViewParams params) {
        super(
                params.tableId,
                params.tableName,
                params.schema,
                params.keysType,
                params.partitionInfo,
                params.distributionInfo
        );
        type = TableType.MATERIALIZED_VIEW;
        buildMode = params.buildMode;
        refreshInfo = params.mvRefreshInfo;
        query = params.queryStmt.toSql();
    }

    public BuildMode getBuildMode() {
        return buildMode;
    }

    public MVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        MaterializedView materializedView = GsonUtils.GSON.fromJson(Text.readString(in), this.getClass());
        refreshInfo = materializedView.refreshInfo;
        query = materializedView.query;
    }
}
