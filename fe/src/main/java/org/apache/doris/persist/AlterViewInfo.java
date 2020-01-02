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

import com.google.gson.annotations.SerializedName;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class AlterViewInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "inlineViewDef")
    private String inlineViewDef;
    @SerializedName(value = "newFullSchema")
    private List<Column> newFullSchema;
    @SerializedName(value = "sqlMode")
    private long sqlMode;

    public AlterViewInfo() {
        // for persist
    }

    public AlterViewInfo(long dbId, long tableId, String inlineViewDef, List<Column> newFullSchema, long sqlMode) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.inlineViewDef = inlineViewDef;
        this.newFullSchema = newFullSchema;
        this.sqlMode = sqlMode;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public List<Column> getNewFullSchema() {
        return newFullSchema;
    }

    public long getSqlMode() {
        return sqlMode;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static AlterViewInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, AlterViewInfo.class);
    }
}
