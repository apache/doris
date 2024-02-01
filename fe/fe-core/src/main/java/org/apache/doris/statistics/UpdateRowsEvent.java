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

package org.apache.doris.statistics;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UpdateRowsEvent implements Writable {

    @SerializedName("tableIdToUpdateRows")
    public final Map<Long, Long> tableIdToUpdateRows = new HashMap<>();

    @VisibleForTesting
    public UpdateRowsEvent() {}

    // No need to be thread safe, only publish thread will call this.
    public void addUpdateRows(long tableId, long rows) {
        if (tableIdToUpdateRows.containsKey(tableId)) {
            tableIdToUpdateRows.put(tableId, tableIdToUpdateRows.get(tableId) + rows);
        } else {
            tableIdToUpdateRows.put(tableId, rows);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static UpdateRowsEvent read(DataInput dataInput) throws IOException {
        String json = Text.readString(dataInput);
        UpdateRowsEvent updateRowsEvent = GsonUtils.GSON.fromJson(json, UpdateRowsEvent.class);
        return updateRowsEvent;
    }
}
