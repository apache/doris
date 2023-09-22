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

import org.apache.doris.catalog.OlapTableFactory.MaterializedViewParams;
import org.apache.doris.common.io.Text;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.nereids.trees.plans.commands.info.MVRefreshInfo.BuildMode;
import org.apache.doris.nereids.trees.plans.commands.info.MVRefreshInfo.RefreshMethod;
import org.apache.doris.nereids.trees.plans.commands.info.MVRefreshTriggerInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;


public class MaterializedView extends OlapTable {
    @SerializedName("bm")
    private BuildMode buildMode;
    @SerializedName("rm")
    private RefreshMethod refreshMethod;
    @SerializedName("rti")
    private MVRefreshTriggerInfo refreshTriggerInfo;
    @SerializedName("qs")
    private String querySql;
    @SerializedName("bt")
    private List<BaseTableInfo> baseTables = Lists.newArrayList();
    @SerializedName("s")
    private MTMVStatus status;
    @SerializedName("a")
    private boolean active;

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
        refreshMethod = params.refreshMethod;
        querySql = params.querySql;
        refreshTriggerInfo = params.refreshTriggerInfo;
        for (TableIf tableIf : params.baseTables) {
            baseTables.add(transferTableIfToBaseTableInfo(tableIf));
        }
    }

    private BaseTableInfo transferTableIfToBaseTableInfo(TableIf tableIf) {
        DatabaseIf db = tableIf.getDatabase();
        return new BaseTableInfo(tableIf.getId(), db.getId(), db.getCatalog().getId());
    }

    public BuildMode getBuildMode() {
        return buildMode;
    }

    public RefreshMethod getRefreshMethod() {
        return refreshMethod;
    }

    public MVRefreshTriggerInfo getRefreshTriggerInfo() {
        return refreshTriggerInfo;
    }

    public String getQuerySql() {
        return querySql;
    }

    public List<BaseTableInfo> getBaseTables() {
        return baseTables;
    }

    public void setBuildMode(BuildMode buildMode) {
        this.buildMode = buildMode;
    }

    public void setRefreshMethod(RefreshMethod refreshMethod) {
        this.refreshMethod = refreshMethod;
    }

    public void setRefreshTriggerInfo(MVRefreshTriggerInfo refreshTriggerInfo) {
        this.refreshTriggerInfo = refreshTriggerInfo;
    }

    public MTMVStatus getStatus() {
        return status;
    }

    public void setStatus(MTMVStatus status) {
        this.status = status;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String toSql() {
        // TODO: 2023/9/21 more info
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE MATERIALIZED VIEW ");
        builder.append(name);
        builder.append(" ");
        builder.append(buildMode);
        builder.append(" REFRESH");
        builder.append(refreshMethod);
        builder.append(" ");
        builder.append(refreshTriggerInfo);
        builder.append(" AS ");
        builder.append(querySql);
        return builder.toString();
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
        buildMode = materializedView.buildMode;
        refreshMethod = materializedView.refreshMethod;
        refreshTriggerInfo = materializedView.refreshTriggerInfo;
        querySql = materializedView.querySql;
        baseTables = materializedView.baseTables;
        status = materializedView.status;
        active = materializedView.active;
    }

}
