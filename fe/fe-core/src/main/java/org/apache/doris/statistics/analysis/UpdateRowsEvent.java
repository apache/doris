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

package org.apache.doris.statistics.analysis;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class UpdateRowsEvent implements Writable {

    @SerializedName("records")
    private final Map<Long, Long> records;

    @SerializedName("tr")
    private final Map<Long, Map<Long, Long>> tabletRecords;

    @SerializedName("dbId")
    private final long dbId;

    @SerializedName("pur")
    private final Map<Long, Long> partitionToUpdateRows;

    @SerializedName("tableId")
    private final long tableId;

    public UpdateRowsEvent(Map<Long, Long> records) {
        this(-1, records);
    }

    /**
     * For a load which reports the rows of a table by table id instead of by tablet. The database id comes
     * first because the tablet keyed constructor above already takes a map followed by a long, and both
     * would have the same erasure otherwise.
     */
    public UpdateRowsEvent(long dbId, Map<Long, Long> records) {
        this.records = records;
        this.tabletRecords = null;
        this.dbId = dbId;
        this.partitionToUpdateRows = null;
        this.tableId = -1;
    }

    public UpdateRowsEvent(Map<Long, Map<Long, Long>> tabletRecords, long dbId) {
        this.records = null;
        this.tabletRecords = tabletRecords;
        this.dbId = dbId;
        this.partitionToUpdateRows = null;
        this.tableId = -1;
    }

    public UpdateRowsEvent(Map<Long, Long> partitionToUpdateRows, long dbId, long tableId) {
        this.records = null;
        this.tabletRecords = null;
        this.dbId = dbId;
        this.partitionToUpdateRows = partitionToUpdateRows;
        this.tableId = tableId;
    }

    // TableId -> table update rows
    public Map<Long, Long> getRecords() {
        return records;
    }

    // TableId -> (TabletId -> tablet update rows)
    public Map<Long, Map<Long, Long>> getTabletRecords() {
        return tabletRecords;
    }

    public long getDbId() {
        return dbId;
    }

    public Map<Long, Long> getPartitionToUpdateRows() {
        return partitionToUpdateRows;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static UpdateRowsEvent read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, UpdateRowsEvent.class);
    }
}
