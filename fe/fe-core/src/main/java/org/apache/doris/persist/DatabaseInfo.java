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
import org.apache.doris.catalog.Database.DbState;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DatabaseInfo implements Writable {

    private String dbName;
    private String newDbName;
    private long quota;
    private String clusterName;
    private DbState dbState;
    private QuotaType quotaType;

    public DatabaseInfo() {
        // for persist
        this.dbName = "";
        this.newDbName = "";
        this.quota = 0;
        this.clusterName = "";
        this.dbState = DbState.NORMAL;
        this.quotaType = QuotaType.DATA;
    }

    public DatabaseInfo(String dbName, String newDbName, long quota, QuotaType quotaType) {
        this.dbName = dbName;
        this.newDbName = newDbName;
        this.quota = quota;
        this.clusterName = "";
        this.dbState = DbState.NORMAL;
        this.quotaType = quotaType;
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

    public static DatabaseInfo read(DataInput in) throws IOException {
        DatabaseInfo dbInfo = new DatabaseInfo();
        dbInfo.readFields(in);
        return dbInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, dbName);
        Text.writeString(out, newDbName);
        out.writeLong(quota);
        Text.writeString(out, this.clusterName);
        Text.writeString(out, this.dbState.name());
        Text.writeString(out, this.quotaType.name());
    }

    public void readFields(DataInput in) throws IOException {
        this.dbName = Text.readString(in);
        newDbName = Text.readString(in);
        this.quota = in.readLong();
        this.clusterName = Text.readString(in);
        this.dbState = DbState.valueOf(Text.readString(in));
        this.quotaType = QuotaType.valueOf(Text.readString(in));
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public DbState getDbState() {
        return dbState;
    }

    public QuotaType getQuotaType() {
        return quotaType;
    }

}
