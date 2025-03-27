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

package org.apache.doris.backup;

import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RestoreBinlogInfo {
    // currently we are sending only DB and table info.
    // partitions level restore not possible since, there can be
    // race condition when two partition recover and ccr-syncer try to sync it.
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableInfo")
    // map of tableId and TableName.
    private Map<Long, String> tableInfo = Maps.newHashMap();

    /*
     * constuctor
     */
    public RestoreBinlogInfo(long dbId, String dbName) {
        this.dbId = dbId;
        this.dbName = dbName;
    }

    public void addTableInfo(long tableId, String tableName) {
        tableInfo.put(tableId, tableName);
    }

    public long getDbId() {
        return dbId;
    }

    public List<Long> getTableIdList() {
        return  tableInfo.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    public static RestoreBinlogInfo fromJson(String json) {
        return GsonUtils.GSON.fromJson(json, RestoreBinlogInfo.class);
    }
}
