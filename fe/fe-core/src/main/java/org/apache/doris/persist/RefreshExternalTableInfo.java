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

import org.apache.doris.catalog.Column;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class RefreshExternalTableInfo implements Writable {
    public static final Logger LOG = LoggerFactory.getLogger(RefreshExternalTableInfo.class);

    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "newSchema")
    private List<Column> newSchema;

    public RefreshExternalTableInfo() {
        // for persist
    }

    public RefreshExternalTableInfo(String dbName, String tableName, List<Column> newSchema) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.newSchema = newSchema;

    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getNewSchema() {
        return newSchema;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static RefreshExternalTableInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, RefreshExternalTableInfo.class);
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RefreshExternalTableInfo)) {
            return false;
        }

        RefreshExternalTableInfo info = (RefreshExternalTableInfo) obj;

        return (dbName.equals(info.dbName))
                && (tableName.equals(info.tableName)) && (newSchema.equals(newSchema));
    }
}
