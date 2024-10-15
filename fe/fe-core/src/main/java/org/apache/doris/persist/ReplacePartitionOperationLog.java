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
import java.util.List;

/*
 * For serialize "replace temp partition" operation log
 */
public class ReplacePartitionOperationLog implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tblId")
    private long tblId;
    @SerializedName(value = "tblName")
    private String tblName;
    @SerializedName(value = "partitions")
    private List<String> partitions;
    @SerializedName(value = "tempPartitions")
    private List<String> tempPartitions;
    @SerializedName(value = "strictRange")
    private boolean strictRange;
    @SerializedName(value = "useTempPartitionName")
    private boolean useTempPartitionName;
    @SerializedName(value = "version")
    private long version = 0L;
    @SerializedName(value = "versionTime")
    private long versionTime = 0L;
    @SerializedName(value = "force")
    private boolean force = false;

    public ReplacePartitionOperationLog(long dbId, String dbName, long tblId, String tblName,
                                        List<String> partitionNames,
                                        List<String> tempPartitonNames, boolean strictRange,
                                        boolean useTempPartitionName, long version, long versionTime, boolean force) {
        this.dbId = dbId;
        this.dbName = dbName;
        this.tblId = tblId;
        this.tblName = tblName;
        this.partitions = partitionNames;
        this.tempPartitions = tempPartitonNames;
        this.strictRange = strictRange;
        this.useTempPartitionName = useTempPartitionName;
        this.version = version;
        this.versionTime = versionTime;
        this.force = force;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTblId() {
        return tblId;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public List<String> getTempPartitions() {
        return tempPartitions;
    }

    public boolean isStrictRange() {
        return strictRange;
    }

    public boolean useTempPartitionName() {
        return useTempPartitionName;
    }

    public long getVersion() {
        return version;
    }

    public long getVersionTime() {
        return versionTime;
    }

    public boolean isForce() {
        return force;
    }

    public static ReplacePartitionOperationLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ReplacePartitionOperationLog.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
