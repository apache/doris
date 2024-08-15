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
import java.util.Objects;
import java.util.Set;

/*
 * used for batch persist drop info in one atomic operation
 */

public class BatchDropInfo implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName; // not used in equals and hashCode
    @SerializedName(value = "indexIdSet")
    private Set<Long> indexIdSet;

    // used for delete decommission tablet
    @SerializedName(value = "deleteTabletWatermarkTxnId")
    private long deleteTabletWatermarkTxnId = -1;

    public BatchDropInfo(long dbId, long tableId, String tableName, Set<Long> indexIdSet,
            long deleteTabletWatermarkTxnId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.indexIdSet = indexIdSet;
        this.deleteTabletWatermarkTxnId = deleteTabletWatermarkTxnId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, indexIdSet);
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BatchDropInfo)) {
            return false;
        }
        BatchDropInfo otherBatchDropInfo = (BatchDropInfo) other;
        return this.dbId == otherBatchDropInfo.dbId && this.tableId == otherBatchDropInfo.tableId
                && this.indexIdSet.equals(otherBatchDropInfo.indexIdSet)
                && this.deleteTabletWatermarkTxnId == otherBatchDropInfo.deleteTabletWatermarkTxnId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static BatchDropInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BatchDropInfo.class);
    }

    public Set<Long> getIndexIdSet() {
        return indexIdSet;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public long getDeleteTabletWatermarkTxnId() {
        return deleteTabletWatermarkTxnId;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }
}
