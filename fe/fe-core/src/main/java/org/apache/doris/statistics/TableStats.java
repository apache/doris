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
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class TableStats implements Writable {

    @SerializedName("tblId")
    public final long tblId;
    @SerializedName("idxId")
    public final long idxId;
    @SerializedName("updatedRows")
    public final AtomicLong updatedRows = new AtomicLong();

    // We would like to analyze tables which queried frequently with higher priority in the future.
    @SerializedName("queriedTimes")
    public final AtomicLong queriedTimes = new AtomicLong();

    // Used for external table.
    @SerializedName("rowCount")
    public final long rowCount;

    @SerializedName("method")
    public final AnalysisMethod analysisMethod;

    @SerializedName("type")
    public final AnalysisType analysisType;

    @SerializedName("updateTime")
    public final long updatedTime;

    @SerializedName("columns")
    public String columns;

    @SerializedName("trigger")
    public JobType jobType;

    // It's necessary to store these fields separately from AnalysisInfo, since the lifecycle between AnalysisInfo
    // and TableStats is quite different.
    public TableStats(long tblId, long rowCount, AnalysisInfo analyzedJob) {
        this.tblId = tblId;
        this.idxId = -1;
        this.rowCount = rowCount;
        analysisMethod = analyzedJob.analysisMethod;
        analysisType = analyzedJob.analysisType;
        updatedTime = System.currentTimeMillis();
        columns = analyzedJob.colName;
        jobType = analyzedJob.jobType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static TableStats read(DataInput dataInput) throws IOException {
        String json = Text.readString(dataInput);
        return GsonUtils.GSON.fromJson(json, TableStats.class);
    }
}
