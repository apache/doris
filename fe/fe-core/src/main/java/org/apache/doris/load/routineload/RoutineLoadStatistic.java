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

package org.apache.doris.load.routineload;

import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class RoutineLoadStatistic {
    /*
     * The following variables are for statistics
     * currentErrorRows/currentTotalRows: the row statistics of current sampling period
     * errorRowsAfterResumed: currentErrorRows that is showed to users in "show routine load;".
     * errorRows/totalRows/receivedBytes: cumulative measurement
     * totalTaskExcutorTimeMs: cumulative execution time of tasks
     */
    /*
     * Rows will be updated after txn state changed when txn state has been successfully changed.
     */

    @SerializedName(value = "currentErrorRows")
    public long currentErrorRows = 0;
    @SerializedName(value = "currentTotalRows")
    public long currentTotalRows = 0;
    @SerializedName(value = "errorRows")
    public long errorRows = 0;
    @SerializedName(value = "totalRows")
    public long totalRows = 0;
    @SerializedName(value = "errorRowsAfterResumed")
    public long errorRowsAfterResumed = 0;
    @SerializedName(value = "unselectedRows")
    public long unselectedRows = 0;
    @SerializedName(value = "receivedBytes")
    public long receivedBytes = 0;
    @SerializedName(value = "totalTaskExcutionTimeMs")
    public long totalTaskExcutionTimeMs = 1; // init as 1 to avoid division by zero
    @SerializedName(value = "committedTaskNum")
    public long committedTaskNum = 0;
    @SerializedName(value = "abortedTaskNum")
    public long abortedTaskNum = 0;

    // Save all transactions current running. Including PREPARE, COMMITTED.
    // No need to persist, only for tracing txn of routine load job.
    public Set<Long> runningTxnIds = Sets.newHashSet();

    public static RoutineLoadStatistic read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, RoutineLoadStatistic.class);
    }

    public Map<String, Object> summary() {
        Map<String, Object> summary = Maps.newHashMap();
        summary.put("totalRows", Long.valueOf(totalRows));
        summary.put("loadedRows", Long.valueOf(totalRows - this.errorRows - this.unselectedRows));
        summary.put("errorRows", Long.valueOf(this.errorRows));
        summary.put("errorRowsAfterResumed", Long.valueOf(this.errorRowsAfterResumed));
        summary.put("unselectedRows", Long.valueOf(this.unselectedRows));
        summary.put("receivedBytes", Long.valueOf(this.receivedBytes));
        summary.put("taskExecuteTimeMs", Long.valueOf(this.totalTaskExcutionTimeMs));
        summary.put("receivedBytesRate", Long.valueOf(this.receivedBytes * 1000 / this.totalTaskExcutionTimeMs));
        summary.put("loadRowsRate", Long.valueOf((this.totalRows - this.errorRows - this.unselectedRows) * 1000
                / this.totalTaskExcutionTimeMs));
        summary.put("committedTaskNum", Long.valueOf(this.committedTaskNum));
        summary.put("abortedTaskNum", Long.valueOf(this.abortedTaskNum));
        summary.put("runningTxns", runningTxnIds);
        return summary;
    }
}
