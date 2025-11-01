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
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import lombok.Data;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class StreamingJobProperties implements JobProperties {
    public static final String MAX_INTERVAL_SECOND_PROPERTY = "max_interval";
    public static final String S3_MAX_BATCH_FILES_PROPERTY = "s3.max_batch_files";
    public static final String S3_MAX_BATCH_BYTES_PROPERTY = "s3.max_batch_bytes";
    public static final String SESSION_VAR_PREFIX = "session.";
    public static final String OFFSET_PROPERTY = "offset";
    public static final List<String> SUPPORT_STREAM_JOB_PROPS = Arrays.asList(MAX_INTERVAL_SECOND_PROPERTY,
            S3_MAX_BATCH_FILES_PROPERTY, S3_MAX_BATCH_BYTES_PROPERTY, OFFSET_PROPERTY);

    public static final long DEFAULT_MAX_INTERVAL_SECOND = 10;
    public static final long DEFAULT_MAX_S3_BATCH_FILES = 256;
    public static final long DEFAULT_MAX_S3_BATCH_BYTES = 10 * 1024 * 1024 * 1024L; // 10GB
    public static final int DEFAULT_JOB_INSERT_TIMEOUT = 30 * 60; // 30min
    public static final int DEFAULT_JOB_QUERY_TIMEOUT = 30 * 60; // 30min


    private final Map<String, String> properties;
    private long maxIntervalSecond;
    private long s3BatchFiles;
    private long s3BatchBytes;

    public StreamingJobProperties(Map<String, String> jobProperties) {
        this.properties = jobProperties;
        if (properties.isEmpty()) {
            this.maxIntervalSecond = DEFAULT_MAX_INTERVAL_SECOND;
            this.s3BatchFiles = DEFAULT_MAX_S3_BATCH_FILES;
            this.s3BatchBytes = DEFAULT_MAX_S3_BATCH_BYTES;
        }
    }

    public void validate() throws AnalysisException {
        List<String> invalidProps = new ArrayList<>();
        for (String key : properties.keySet()) {
            if (!SUPPORT_STREAM_JOB_PROPS.contains(key) && !key.startsWith(SESSION_VAR_PREFIX)) {
                invalidProps.add(key);
            }
        }
        if (!invalidProps.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid properties: " + invalidProps
                            + ". Supported keys are " + SUPPORT_STREAM_JOB_PROPS
                            + " or any key starting with '" + SESSION_VAR_PREFIX + "'."
            );
        }

        this.maxIntervalSecond = Util.getLongPropertyOrDefault(
                        properties.get(StreamingJobProperties.MAX_INTERVAL_SECOND_PROPERTY),
                        StreamingJobProperties.DEFAULT_MAX_INTERVAL_SECOND, (v) -> v >= 1,
                StreamingJobProperties.MAX_INTERVAL_SECOND_PROPERTY + " should > 1");

        this.s3BatchFiles = Util.getLongPropertyOrDefault(
                        properties.get(StreamingJobProperties.S3_MAX_BATCH_FILES_PROPERTY),
                        StreamingJobProperties.DEFAULT_MAX_S3_BATCH_FILES, (v) -> v >= 1,
                StreamingJobProperties.S3_MAX_BATCH_FILES_PROPERTY + " should >=1 ");

        this.s3BatchBytes = Util.getLongPropertyOrDefault(
                        properties.get(StreamingJobProperties.S3_MAX_BATCH_BYTES_PROPERTY),
                        StreamingJobProperties.DEFAULT_MAX_S3_BATCH_BYTES, (v) -> v >= 100 * 1024 * 1024
                        && v <= (long) (1024 * 1024 * 1024) * 10,
                StreamingJobProperties.S3_MAX_BATCH_BYTES_PROPERTY + " should between 100MB and 10GB");

        // validate session variables
        try {
            Map<String, String> sessionVarMap = parseSessionVarMap();
            if (!sessionVarMap.isEmpty()) {
                SessionVariable sessionVar = new SessionVariable();
                sessionVar.readFromMap(sessionVarMap);
                JSONObject sessionVarJson = sessionVar.toJson();

                // check if there are invalid keys
                Set<String> inputKeys = sessionVarMap.keySet();
                Set<String> validKeys = sessionVarJson.keySet();
                Set<String> invalidKeys = new HashSet<>(inputKeys);
                invalidKeys.removeAll(validKeys);
                if (!invalidKeys.isEmpty()) {
                    throw new IllegalArgumentException(invalidKeys.toString());
                }
            }
        } catch (Exception e) {
            throw new AnalysisException("Invalid session variable, " + e.getMessage());
        }
    }

    public SessionVariable getSessionVariable(SessionVariable sessionVariable) throws JobException {
        int defaultInsert = parseIntOrDefault(
                VariableMgr.getDefaultValue(SessionVariable.INSERT_TIMEOUT),
                DEFAULT_JOB_INSERT_TIMEOUT
        );
        int defaultQuery = parseIntOrDefault(
                VariableMgr.getDefaultValue(SessionVariable.QUERY_TIMEOUT),
                DEFAULT_JOB_QUERY_TIMEOUT
        );
        // override with job default session var
        if (sessionVariable.getInsertTimeoutS() == defaultInsert) {
            sessionVariable.setInsertTimeoutS(DEFAULT_JOB_INSERT_TIMEOUT);
        }

        if (sessionVariable.getQueryTimeoutS() == defaultQuery) {
            sessionVariable.setQueryTimeoutS(DEFAULT_JOB_QUERY_TIMEOUT);
        }

        Map<String, String> sessionVarMap = parseSessionVarMap();
        if (!sessionVarMap.isEmpty()) {
            try {
                // override session var for sessionVarMap
                sessionVariable.readFromMap(sessionVarMap);
            } catch (Exception e) {
                throw new JobException("Invalid session variable, " + e.getMessage());
            }
        }
        return sessionVariable;
    }

    private int parseIntOrDefault(String val, int defaultValue) {
        if (val == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private Map<String, String> parseSessionVarMap() {
        final Map<String, String> sessionVarMap = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(SESSION_VAR_PREFIX)) {
                String subKey = entry.getKey().substring(SESSION_VAR_PREFIX.length());
                sessionVarMap.put(subKey, entry.getValue());
            }
        }
        return sessionVarMap;
    }

    public String getOffsetProperty() {
        return properties.get(OFFSET_PROPERTY);
    }
}
