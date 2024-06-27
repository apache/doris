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

import org.apache.doris.analysis.AlterDatabaseQuotaStmt.QuotaType;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Database.DbState;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DatabaseInfo implements Writable, GsonPostProcessable {

    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "newDbName")
    private String newDbName;
    @SerializedName(value = "quota")
    private long quota;
    @SerializedName(value = "clusterName")
    private String clusterName;
    @SerializedName(value = "dbState")
    private DbState dbState;
    @SerializedName(value = "quotaType")
    private QuotaType quotaType;
    @SerializedName(value = "binlogConfig")
    private BinlogConfig binlogConfig;

    public DatabaseInfo() {
        // for persist
        this.dbName = "";
        this.newDbName = "";
        this.quota = 0;
        this.clusterName = "";
        this.dbState = DbState.NORMAL;
        this.quotaType = QuotaType.DATA;
        binlogConfig = null;
    }

    public DatabaseInfo(String dbName, String newDbName, long quota, QuotaType quotaType) {
        this.dbName = dbName;
        this.newDbName = newDbName;
        this.quota = quota;
        this.clusterName = "";
        this.dbState = DbState.NORMAL;
        this.quotaType = quotaType;
        this.binlogConfig = null;
    }

    public String getDbName() {
        return dbName;
    }

    public String getNewDbName() {
        return newDbName;
    }

    public long getQuota() {
        return quota;
    }

    public BinlogConfig getBinlogConfig() {
        return binlogConfig;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DatabaseInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_134) {
            DatabaseInfo dbInfo = new DatabaseInfo();
            dbInfo.readFields(in);
            return dbInfo;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), DatabaseInfo.class);
        }
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        this.dbName = ClusterNamespace.getNameFromFullName(Text.readString(in));
        newDbName = ClusterNamespace.getNameFromFullName(Text.readString(in));
        this.quota = in.readLong();
        this.clusterName = Text.readString(in);
        this.dbState = DbState.valueOf(Text.readString(in));
        this.quotaType = QuotaType.valueOf(Text.readString(in));
    }

    public QuotaType getQuotaType() {
        return quotaType;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        dbName = ClusterNamespace.getNameFromFullName(dbName);
        newDbName = ClusterNamespace.getNameFromFullName(newDbName);
    }
}
