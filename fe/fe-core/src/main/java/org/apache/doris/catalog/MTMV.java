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

import org.apache.doris.catalog.OlapTableFactory.MTMVParams;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.EnvInfo;
import org.apache.doris.mtmv.MTMVJobInfo;
import org.apache.doris.mtmv.MTMVJobManager;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class MTMV extends OlapTable {
    private static final Logger LOG = LogManager.getLogger(MTMV.class);
    private ReentrantReadWriteLock mvRwLock;

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
    @SerializedName("r")
    private MTMVRelation relation;

    // For deserialization
    public MTMV() {
        type = TableType.MATERIALIZED_VIEW;
        mvRwLock = new ReentrantReadWriteLock(true);
    }

    MTMV(MTMVParams params) {
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
        mvRwLock = new ReentrantReadWriteLock(true);
    }

    public MTMVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }

    public String getQuerySql() {
        return querySql;
    }

    public MTMVStatus getStatus() {
        try {
            readMvLock();
            return status;
        } finally {
            readMvUnlock();
        }
    }

    public EnvInfo getEnvInfo() {
        return envInfo;
    }

    public MTMVJobInfo getJobInfo() {
        return jobInfo;
    }

    public MTMVRelation getRelation() {
        return relation;
    }

    public MTMVRefreshInfo alterRefreshInfo(MTMVRefreshInfo newRefreshInfo) {
        return refreshInfo.updateNotNull(newRefreshInfo);
    }

    public MTMVStatus alterStatus(MTMVStatus newStatus) {
        try {
            writeMvLock();
            return this.status.updateNotNull(newStatus);
        } finally {
            writeMvUnlock();
        }
    }

    public void addTaskResult(MTMVTask task, MTMVRelation relation) {
        try {
            writeMvLock();
            if (task.getStatus() == TaskStatus.SUCCESS) {
                this.status.setState(MTMVState.NORMAL);
                this.status.setSchemaChangeDetail(null);
                this.status.setRefreshState(MTMVRefreshState.SUCCESS);
                this.relation = relation;
            } else {
                this.status.setRefreshState(MTMVRefreshState.FAIL);
            }
            this.jobInfo.addHistoryTask(task);
        } finally {
            writeMvUnlock();
        }
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

    public Map<String, String> getMvProperties() {
        return mvProperties;
    }

    public void readMvLock() {
        this.mvRwLock.readLock().lock();
    }

    public void readMvUnlock() {
        this.mvRwLock.readLock().unlock();
    }

    public void writeMvLock() {
        this.mvRwLock.writeLock().lock();
    }

    public void writeMvUnlock() {
        this.mvRwLock.writeLock().unlock();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        MTMV materializedView = GsonUtils.GSON.fromJson(Text.readString(in), this.getClass());
        refreshInfo = materializedView.refreshInfo;
        querySql = materializedView.querySql;
        status = materializedView.status;
        envInfo = materializedView.envInfo;
        jobInfo = materializedView.jobInfo;
        mvProperties = materializedView.mvProperties;
        relation = materializedView.relation;
    }

}
