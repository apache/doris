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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.Util;
import org.apache.doris.job.base.JobProperties;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.SessionVariable;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class StreamingJobProperties implements JobProperties {
    public static final String MAX_INTERVAL_SECOND_PROPERTY = "max_interval";
    public static final String S3_BATCH_FILES_PROPERTY = "s3.batch_files";
    public static final String S3_BATCH_SIZE_PROPERTY = "s3.batch_size";
    public static final String SESSION_VAR_PREFIX = "session.";

    public static final long DEFAULT_MAX_INTERVAL_SECOND = 10;
    public static final long DEFAULT_S3_BATCH_FILES = 256;
    public static final long DEFAULT_S3_BATCH_SIZE = 10 * 1024 * 1024 * 1024L; // 10GB
    public static final int DEFAULT_INSERT_TIMEOUT = 30 * 60; // 30min

    private final Map<String, String> properties;
    private long maxIntervalSecond;
    private long s3BatchFiles;
    private long s3BatchSize;

    public StreamingJobProperties(Map<String, String> jobProperties) {
        this.properties = jobProperties;
        if (properties.isEmpty()) {
            this.maxIntervalSecond = DEFAULT_MAX_INTERVAL_SECOND;
            this.s3BatchFiles = DEFAULT_S3_BATCH_FILES;
            this.s3BatchSize = DEFAULT_S3_BATCH_SIZE;
        }
    }

    public void validate() throws AnalysisException {
        this.maxIntervalSecond = Util.getLongPropertyOrDefault(
                properties.get(StreamingJobProperties.MAX_INTERVAL_SECOND_PROPERTY),
                StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND, (v) -> v >= 1,
                StreamingJobProperties.MAX_INTERVAL_SECOND_PROPERTY + " should > 1");

        this.s3BatchFiles = Util.getLongPropertyOrDefault(
                properties.get(StreamingJobProperties.S3_BATCH_FILES_PROPERTY),
                StreamingJobProperties.DEFAULT_S3_BATCH_FILES, (v) -> v >= 1,
                StreamingJobProperties.S3_BATCH_FILES_PROPERTY + " should >=1 ");

        this.s3BatchSize = Util.getLongPropertyOrDefault(properties.get(StreamingJobProperties.S3_BATCH_SIZE_PROPERTY),
                StreamingJobProperties.DEFAULT_S3_BATCH_SIZE, (v) -> v >= 100 * 1024 * 1024
                        && v <= (long) (1024 * 1024 * 1024) * 10,
                StreamingJobProperties.S3_BATCH_SIZE_PROPERTY + " should between 100MB and 10GB");
    }

    public SessionVariable getSessionVariable() throws JobException {
        final Map<String, String> sessionVarMap = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(SESSION_VAR_PREFIX)) {
                String subKey = entry.getKey().substring(SESSION_VAR_PREFIX.length());
                sessionVarMap.put(subKey, entry.getValue());
            }
        }

        SessionVariable sessionVariable = new SessionVariable();
        try {
            sessionVariable.setInsertTimeoutS(DEFAULT_INSERT_TIMEOUT);
            sessionVariable.readFromJson(GsonUtils.GSON.toJson(sessionVarMap));
        } catch (Exception e) {
            throw new JobException("Invalid session variable, " + e.getMessage());
        }
        return sessionVariable;
    }
}
