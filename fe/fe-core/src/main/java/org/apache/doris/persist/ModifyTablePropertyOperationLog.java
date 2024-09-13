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

package org.apache.doris.persist;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ModifyTablePropertyOperationLog implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "ctlName")
    private String ctlName;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "properties")
    private Map<String, String> properties = new HashMap<>();
    @SerializedName(value = "sql")
    private String sql;

    public ModifyTablePropertyOperationLog(long dbId, long tableId, String tableName, Map<String, String> properties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.properties = properties;

        StringBuilder sb = new StringBuilder();
        sb.append("SET (");
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
        }
        sb.deleteCharAt(sb.length() - 1); // remove last ','
        sb.append(")");
        this.sql = sb.toString();
    }

    public ModifyTablePropertyOperationLog(String ctlName, String dbName, String tableName,
                                           Map<String, String> properties) {
        this.ctlName = ctlName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.properties = properties;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getCtlName() {
        return ctlName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ModifyTablePropertyOperationLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), ModifyTablePropertyOperationLog.class);
    }

    public String toJson()  {
        return GsonUtils.GSON.toJson(this);
    }
}
