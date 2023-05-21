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

import org.apache.doris.analysis.CleanQueryStatsStmt.Scope;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CleanQueryStatsInfo implements Writable {
    @SerializedName(value = "catalog")
    private String catalog;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "scope")
    private Scope scope;

    public CleanQueryStatsInfo(Scope scope, String catalog, String dbName, String tableName) {
        this.catalog = catalog;
        this.dbName = dbName;
        this.tableName = tableName;
        this.scope = scope;
    }

    public static CleanQueryStatsInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), CleanQueryStatsInfo.class);
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public Scope getScope() {
        return scope;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
