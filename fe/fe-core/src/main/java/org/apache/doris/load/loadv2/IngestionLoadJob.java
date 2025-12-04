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

import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Ingestion Load
 * </p>
 * Load data file which has been pre-processed
 * </p>
 * There are 4 steps in IngestionLoadJob:
 * Step1: Outside system execute ingestion etl job.
 * Step2: LoadEtlChecker will check ingestion etl job status periodically
 * and send push tasks to be when ingestion etl job is finished.
 * Step3: LoadLoadingChecker will check loading status periodically and commit transaction when push tasks are finished.
 * Step4: PublishVersionDaemon will send publish version tasks to be and finish transaction.
 */
@Deprecated
public class IngestionLoadJob extends LoadJob {

    public static final Logger LOG = LogManager.getLogger(IngestionLoadJob.class);

    @Setter
    @SerializedName("ests")
    private EtlStatus etlStatus;

    // members below updated when job state changed to loading
    // { tableId.partitionId.indexId.bucket.schemaHash -> (etlFilePath, etlFileSize) }
    @SerializedName(value = "tm2fi")
    private final Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();

    @SerializedName(value = "hp")
    private final Map<String, String> hadoopProperties = new HashMap<>();

    @SerializedName(value = "i2sv")
    private final Map<Long, Integer> indexToSchemaVersion = new HashMap<>();

    public IngestionLoadJob() {
        super(EtlJobType.INGESTION);
    }

    @Override
    Set<String> getTableNamesForShow() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        return Collections.emptySet();
    }

    public static class IngestionLoadJobStateUpdateInfo extends LoadJobStateUpdateInfo {

        @SerializedName(value = "etlStartTimestamp")
        private long etlStartTimestamp;
        @SerializedName(value = "etlStatus")
        private EtlStatus etlStatus;
        @SerializedName(value = "tabletMetaToFileInfo")
        private Map<String, Pair<String, Long>> tabletMetaToFileInfo;
        @SerializedName(value = "hadoopProperties")
        private Map<String, String> hadoopProperties;
        @SerializedName(value = "indexToSchemaVersion")
        private Map<Long, Integer> indexToSchemaVersion;

        public IngestionLoadJobStateUpdateInfo(long jobId, JobState state, long transactionId,
                                               long etlStartTimestamp, long loadStartTimestamp, EtlStatus etlStatus,
                                               Map<String, Pair<String, Long>> tabletMetaToFileInfo,
                                               Map<String, String> hadoopProperties,
                                               Map<Long, Integer> indexToSchemaVersion) {
            super(jobId, state, transactionId, loadStartTimestamp);
            this.etlStartTimestamp = etlStartTimestamp;
            this.etlStatus = etlStatus;
            this.tabletMetaToFileInfo = tabletMetaToFileInfo;
            this.hadoopProperties = hadoopProperties;
            this.indexToSchemaVersion = indexToSchemaVersion;
        }
    }
}
