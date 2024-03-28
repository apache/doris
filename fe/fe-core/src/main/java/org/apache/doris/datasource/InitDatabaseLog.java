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

package org.apache.doris.datasource;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

@Data
public class InitDatabaseLog implements Writable {
    public enum Type {
        HMS,
        ICEBERG,
        ES,
        JDBC,
        MAX_COMPUTE,
        HUDI,
        PAIMON,
        LAKESOUL,
        TEST,
        INFO_SCHEMA_DB,
        TRINO_CONNECTOR,
        UNKNOWN;
    }

    @SerializedName(value = "catalogId")
    private long catalogId;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "refreshCount")
    private int refreshCount;

    @SerializedName(value = "createCount")
    private int createCount;

    @SerializedName(value = "refreshTableIds")
    private List<Long> refreshTableIds;

    @SerializedName(value = "createTableIds")
    private List<Long> createTableIds;

    @SerializedName(value = "createTableNames")
    private List<String> createTableNames;

    @SerializedName(value = "type")
    private Type type;

    @SerializedName(value = "lastUpdateTime")
    protected long lastUpdateTime;

    public InitDatabaseLog() {
        refreshCount = 0;
        createCount = 0;
        catalogId = 0;
        dbId = 0;
        refreshTableIds = Lists.newArrayList();
        createTableIds = Lists.newArrayList();
        createTableNames = Lists.newArrayList();
        type = Type.UNKNOWN;
    }

    public void addRefreshTable(long id) {
        refreshCount += 1;
        refreshTableIds.add(id);
    }

    public void addCreateTable(long id, String name) {
        createCount += 1;
        createTableIds.add(id);
        createTableNames.add(name);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static InitDatabaseLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, InitDatabaseLog.class);
    }
}
