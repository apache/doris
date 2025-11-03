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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.Pair;
import org.apache.doris.load.EtlJobType;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

@Deprecated
public class SparkLoadJob extends BulkLoadJob {
    // create from resourceDesc when job created
    @SerializedName(value = "sr")
    private SparkResource sparkResource;
    // members below updated when job state changed to etl
    @SerializedName(value = "est")
    private long etlStartTimestamp = -1;
    // for spark yarn
    @SerializedName(value = "appid")
    private String appId = "";
    // spark job outputPath
    @SerializedName(value = "etlop")
    private String etlOutputPath = "";
    // members below updated when job state changed to loading
    // { tableId.partitionId.indexId.bucket.schemaHash -> (etlFilePath, etlFileSize) }
    @SerializedName(value = "tm2fi")
    private Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();
    // for spark standalone
    @SerializedName(value = "slah")
    private SparkLoadAppHandle sparkLoadAppHandle = new SparkLoadAppHandle();

    // only for log replay
    public SparkLoadJob() {
        super(EtlJobType.SPARK);
    }

    /**
     * Used for spark load job journal log when job state changed to ETL or LOADING
     */
    public static class SparkLoadJobStateUpdateInfo extends LoadJobStateUpdateInfo {
        @SerializedName(value = "sparkLoadAppHandle")
        private SparkLoadAppHandle sparkLoadAppHandle;
        @SerializedName(value = "etlStartTimestamp")
        private long etlStartTimestamp;
        @SerializedName(value = "appId")
        private String appId;
        @SerializedName(value = "etlOutputPath")
        private String etlOutputPath;
        @SerializedName(value = "tabletMetaToFileInfo")
        private Map<String, Pair<String, Long>> tabletMetaToFileInfo;

        public SparkLoadJobStateUpdateInfo(long jobId, JobState state, long transactionId, long loadStartTimestamp) {
            super(jobId, state, transactionId, loadStartTimestamp);
        }
    }
}
