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
    @SerializedName("buildMode")
    private BuildMode buildMode;
    @SerializedName("refreshMethod")
    private RefreshMethod refreshMethod;
    @SerializedName("refreshTriggerInfo")
    private MVRefreshTriggerInfo refreshTriggerInfo;
    @SerializedName("querySql")
    private String querySql;
    @SerializedName("originSql")
    private String originSql;
    @SerializedName("baseTables")
    private List<BaseTableInfo> baseTables = Lists.newArrayList();

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
        originSql = params.originSql;
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

    public String getOriginSql() {
        return originSql;
    }

    public List<BaseTableInfo> getBaseTables() {
        return baseTables;
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
        originSql = materializedView.originSql;
        baseTables = materializedView.baseTables;
    }

}
