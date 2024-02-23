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

public class DropDbInfo implements Writable, GsonPostProcessable {
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "forceDrop")
    private boolean forceDrop = false;
    @SerializedName(value = "recycleTime")
    private long recycleTime = 0;

    public DropDbInfo() {
        this("", false, 0);
    }

    public DropDbInfo(String dbName, boolean forceDrop, long recycleTime) {
        this.dbName = dbName;
        this.forceDrop = forceDrop;
        this.recycleTime = recycleTime;
    }

    public String getDbName() {
        return dbName;
    }

    public boolean isForceDrop() {
        return  forceDrop;
    }

    public Long getRecycleTime() {
        return  recycleTime;
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException {
        dbName = Text.readString(in);
    }

    public static DropDbInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DropDbInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DropDbInfo)) {
            return false;
        }

        DropDbInfo info = (DropDbInfo) obj;

        return (dbName.equals(info.getDbName()))
            && (forceDrop == info.isForceDrop())
            && (recycleTime == info.getRecycleTime());
    }

    @Override
    public void gsonPostProcess() throws IOException {
        dbName = ClusterNamespace.getNameFromFullName(dbName);
    }
}
