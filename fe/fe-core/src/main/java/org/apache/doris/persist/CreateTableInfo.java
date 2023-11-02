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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CreateTableInfo implements Writable {
    public static final Logger LOG = LoggerFactory.getLogger(CreateTableInfo.class);

    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "table")
    private Table table;

    public CreateTableInfo() {
        // for persist
    }

    public CreateTableInfo(String dbName, Table table) {
        this.dbName = dbName;
        this.table = table;
    }

    public String getDbName() {
        return dbName;
    }

    public Table getTable() {
        return table;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        dbName = Text.readString(in);
        table = Table.read(in);
    }

    public static CreateTableInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_127) {
            CreateTableInfo createTableInfo = new CreateTableInfo();
            createTableInfo.readFields(in);
            return createTableInfo;
        }
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CreateTableInfo.class);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, table);
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CreateTableInfo)) {
            return false;
        }

        CreateTableInfo info = (CreateTableInfo) obj;

        return (dbName.equals(info.dbName))
                && (table.equals(info.table));
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
