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

import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ModifyTableDefaultDistributionBucketNumOperationLog implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "type")
    private DistributionInfoType type;
    @SerializedName(value = "autoBucket")
    protected boolean autoBucket;
    @SerializedName(value = "bucketNum")
    private int bucketNum;
    @SerializedName(value = "columnsName")
    private String columnsName;

    public ModifyTableDefaultDistributionBucketNumOperationLog(long dbId, long tableId, DistributionInfoType type,
            boolean autoBucket, int bucketNum, String columnsName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.type = type;
        this.autoBucket = autoBucket;
        this.bucketNum = bucketNum;
        this.columnsName = columnsName;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public DistributionInfoType getType() {
        return type;
    }

    public boolean getAutoBucket() {
        return autoBucket;
    }

    public String getColumnsName() {
        return columnsName == null ? "" : columnsName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ModifyTableDefaultDistributionBucketNumOperationLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), ModifyTableDefaultDistributionBucketNumOperationLog.class);
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }
}
