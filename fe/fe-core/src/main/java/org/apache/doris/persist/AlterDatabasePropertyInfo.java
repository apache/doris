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

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class AlterDatabasePropertyInfo implements Writable, GsonPostProcessable {
    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "dbName")
    private String dbName;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public AlterDatabasePropertyInfo(long dbId, String dbName, Map<String, String> properties) {
        this.dbId = dbId;
        this.dbName = dbName;
        this.properties = properties;
    }

    public long getDbId() {
        return dbId;
    }

    public String getDbName() {
        return dbName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterDatabasePropertyInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AlterDatabasePropertyInfo.class);
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        dbName = ClusterNamespace.getNameFromFullName(dbName);
    }
}
