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
import java.util.List;

public class NewPartitionLoadedEvent implements Writable {

    @SerializedName("tableIds")
    private List<Long> tableIds;

    @VisibleForTesting
    public NewPartitionLoadedEvent(List<Long> tableIds) {
        this.tableIds = tableIds;
    }

    // No need to be thread safe, only publish thread will call this.
    public void addTableId(long tableId) {
        tableIds.add(tableId);
    }

    public List<Long> getTableIds() {
        return tableIds;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static NewPartitionLoadedEvent read(DataInput dataInput) throws IOException {
        String json = Text.readString(dataInput);
        return GsonUtils.GSON.fromJson(json, NewPartitionLoadedEvent.class);
    }
}
