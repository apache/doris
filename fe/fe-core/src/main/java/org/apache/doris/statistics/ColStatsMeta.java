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

import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;

import com.google.gson.annotations.SerializedName;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class ColStatsMeta {

    @SerializedName("updateTime")
    public long updatedTime;

    @SerializedName("method")
    public AnalysisMethod analysisMethod;

    @SerializedName("type")
    public AnalysisType analysisType;

    @SerializedName("queriedTimes")
    public final AtomicLong queriedTimes = new AtomicLong();

    // TODO: For column that manually analyzed, we should use same analyze method as user specified.
    @SerializedName("trigger")
    public JobType jobType;

    @SerializedName("updatedRows")
    public long updatedRows;

    @SerializedName("rowCount")
    public long rowCount;

    @SerializedName("pur")
    public ConcurrentMap<Long, Long> partitionUpdateRows = new ConcurrentHashMap<>();

    public ColStatsMeta(long updatedTime, AnalysisMethod analysisMethod, AnalysisType analysisType, JobType jobType,
            long queriedTimes, long rowCount, long updatedRows, Map<Long, Long> partitionUpdateRows) {
        this.updatedTime = updatedTime;
        this.analysisMethod = analysisMethod;
        this.analysisType = analysisType;
        this.jobType = jobType;
        this.queriedTimes.addAndGet(queriedTimes);
        this.updatedRows = updatedRows;
        this.rowCount = rowCount;
        if (partitionUpdateRows != null) {
            this.partitionUpdateRows.putAll(partitionUpdateRows);
        }
    }
}
