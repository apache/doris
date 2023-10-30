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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mtmv.MTMVJobInfo;
import org.apache.doris.mtmv.MTMVJobManager;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.nereids.trees.plans.commands.info.EnvInfo;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVRefreshInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;


public class MTMV extends OlapTable {
    @SerializedName("ri")
    private MTMVRefreshInfo refreshInfo;
    @SerializedName("qs")
    private String querySql;
    @SerializedName("s")
    private MTMVStatus status;
    @SerializedName("ei")
    private EnvInfo envInfo;
    @SerializedName("ji")
    private MTMVJobInfo jobInfo;
    @SerializedName("mp")
    private Map<String, String> mvProperties;

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
        this.type = TableType.MATERIALIZED_VIEW;
        this.querySql = params.querySql;
        this.refreshInfo = params.refreshInfo;
        this.envInfo = params.envInfo;
        this.status = new MTMVStatus();
        this.jobInfo = new MTMVJobInfo(MTMVJobManager.MTMV_JOB_PREFIX + params.tableId);
        this.mvProperties = params.mvProperties;
    }

    public MTMVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }

    public String getQuerySql() {
        return querySql;
    }

    public MTMVStatus getStatus() {
        return status;
    }

    public EnvInfo getEnvInfo() {
        return envInfo;
    }

    public MTMVJobInfo getJobInfo() {
        return jobInfo;
    }

    public MTMVRefreshInfo alterRefreshInfo(MTMVRefreshInfo newRefreshInfo) {
        return refreshInfo.updateNotNull(newRefreshInfo);
    }

    public MTMVStatus alterStatus(MTMVStatus status) {
        return status.updateNotNull(status);
    }

    public Map<String, String> alterMvProperties(Map<String, String> mvProperties) {
        this.mvProperties.putAll(mvProperties);
        return this.mvProperties;
    }

    public long getGracePeriod() {
        if (mvProperties.containsKey(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD)) {
            return Long.parseLong(mvProperties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD));
        } else {
            return 0L;
        }
    }

    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE MATERIALIZED VIEW ");
        builder.append(name);
        builder.append(" ");
        builder.append(refreshInfo);
        builder.append(" PROPERTIES ");
        builder.append(mvProperties.toString().replace("{", "(").replace("}", ")"));
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
        refreshInfo = materializedView.refreshInfo;
        querySql = materializedView.querySql;
        status = materializedView.status;
        envInfo = materializedView.envInfo;
        jobInfo = materializedView.jobInfo;
        mvProperties = materializedView.mvProperties;
        Env.getCurrentEnv().getMtmvService().registerMTMV(this);
    }

}
