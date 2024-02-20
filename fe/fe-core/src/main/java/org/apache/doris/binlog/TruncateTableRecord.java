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

import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

public class TruncateTableRecord {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "db")
    private String db;
    @SerializedName(value = "tblId")
    private long tblId;
    @SerializedName(value = "table")
    private String table;
    @SerializedName(value = "isEntireTable")
    private boolean isEntireTable = false;
    @SerializedName(value = "rawSql")
    private String rawSql = "";

    public TruncateTableRecord(TruncateTableInfo info) {
        this.dbId = info.getDbId();
        this.db = info.getDb();
        this.tblId = info.getTblId();
        this.table = info.getTable();
        this.isEntireTable = info.isEntireTable();
        this.rawSql = info.getRawSql();
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }
}
