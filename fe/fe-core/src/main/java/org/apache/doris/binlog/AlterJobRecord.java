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

package org.apache.doris.binlog;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.SchemaChangeJobV2;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlterJobRecord {
    @SerializedName(value = "type")
    private AlterJobV2.JobType type;
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "jobState")
    private AlterJobV2.JobState jobState;
    @SerializedName(value = "rawSql")
    private String rawSql;
    @SerializedName(value = "iim")
    private Map<Long, Long> indexIdMap;

    public AlterJobRecord(AlterJobV2 job) {
        this.type = job.getType();
        this.dbId = job.getDbId();
        this.tableId = job.getTableId();
        this.tableName = job.getTableName();
        this.jobId = job.getJobId();
        this.jobState = job.getJobState();
        this.rawSql = job.getRawSql();
        if (type == AlterJobV2.JobType.SCHEMA_CHANGE && job instanceof SchemaChangeJobV2) {
            this.indexIdMap = ((SchemaChangeJobV2) job).getIndexIdMap();
        }
    }

    public boolean isJobFinished() {
        return jobState == AlterJobV2.JobState.FINISHED;
    }

    public boolean isSchemaChangeJob() {
        return type == AlterJobV2.JobType.SCHEMA_CHANGE;
    }

    public List<Long> getOriginIndexIdList() {
        if (indexIdMap == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(indexIdMap.values());
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    public static AlterJobRecord fromJson(String json) {
        return GsonUtils.GSON.fromJson(json, AlterJobRecord.class);
    }
}
