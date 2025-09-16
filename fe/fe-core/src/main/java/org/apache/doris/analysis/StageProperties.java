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

package org.apache.doris.analysis;

import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageAccessType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.FileFormatConstants;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class StageProperties extends CopyProperties {
    private static final Logger LOG = LogManager.getLogger(StageProperties.class);
    private static final String KEY_PREFIX = "default.";

    // properties for object storage
    public static final String ENDPOINT = "endpoint";
    public static final String REGION = "region";
    public static final String BUCKET = "bucket";
    public static final String PREFIX = "prefix";
    public static final String AK = "ak";
    public static final String SK = "sk";
    public static final String ROLE_NAME = "role_name";
    public static final String ARN = "arn";
    public static final String PROVIDER = "provider";
    public static final String COMMENT = "comment";
    public static final String CREATE_TIME = "create_time";
    public static final String ACCESS_TYPE = "access_type";
    private static final String ACCESS_BY_AKSK = "aksk";
    private static final String ACCESS_BY_IAM = "iam";
    private static final String ACCESS_BY_BUCKET_ACL = "bucket_acl";
    private static final String DRY_RUN = "dry_run";

    private static final ImmutableSet<String> STORAGE_REQUIRED_PROPERTIES = new ImmutableSet.Builder<String>().add(
            ENDPOINT).add(REGION).add(BUCKET).add(PROVIDER).add(ACCESS_TYPE).build();
    private static final ImmutableSet<String> STORAGE_PROPERTIES = new ImmutableSet.Builder<String>().add(PREFIX)
            .addAll(STORAGE_REQUIRED_PROPERTIES).add(COMMENT).add(AK).add(SK).add(ROLE_NAME).add(ARN).build();

    private static final ImmutableSet<String> STAGE_PROPERTIES = new ImmutableSet.Builder<String>()
            .addAll(STORAGE_PROPERTIES)
            .add(KEY_PREFIX + TYPE).add(KEY_PREFIX + COMPRESSION).add(KEY_PREFIX + COLUMN_SEPARATOR)
            .add(KEY_PREFIX + LINE_DELIMITER).add(KEY_PREFIX + PARAM_STRIP_OUTER_ARRAY)
            .add(KEY_PREFIX + PARAM_FUZZY_PARSE).add(KEY_PREFIX + PARAM_NUM_AS_STRING)
            .add(KEY_PREFIX + PARAM_JSONPATHS).add(KEY_PREFIX + PARAM_JSONROOT)
            .add(KEY_PREFIX + SIZE_LIMIT).add(KEY_PREFIX + ON_ERROR).add(KEY_PREFIX + ASYNC)
            .add(KEY_PREFIX + STRICT_MODE).add(KEY_PREFIX + LOAD_PARALLELISM).add(DRY_RUN).build();

    public StageProperties(Map<String, String> properties) {
        super(properties, KEY_PREFIX);
    }

    public void analyze() throws AnalysisException {
        analyzeStorageProperties();
        analyzeTypeAndCompression();
        analyzeSizeLimit();
        analyzeOnError();
        analyzeAsync();
        analyzeStrictMode();
        analyzeLoadParallelism();
        analyzeAccessType();
        analyzeDryRun();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (!STAGE_PROPERTIES.contains(entry.getKey())) {
                throw new AnalysisException("Property '" + entry.getKey() + "' is invalid");
            }
        }
    }

    private void analyzeAccessType() throws AnalysisException {
        String accessType = properties.get(ACCESS_TYPE);
        if (accessType.equalsIgnoreCase(ACCESS_BY_AKSK)) {
            if (StringUtils.isEmpty(properties.get(AK))) {
                throw new AnalysisException("Property " + AK + " is required for ExternalStage");
            }
            if (StringUtils.isEmpty(properties.get(SK))) {
                throw new AnalysisException("Property " + SK + " is required for ExternalStage");
            }
        } else if (accessType.equalsIgnoreCase(ACCESS_BY_IAM)) {
            if (StringUtils.isEmpty(ROLE_NAME)) {
                throw new AnalysisException("Property " + ROLE_NAME + " is required for ExternalStage");
            }
            if (StringUtils.isEmpty(ARN)) {
                throw new AnalysisException("Property " + ARN + " is required for ExternalStage");
            }
        } else if (accessType.equalsIgnoreCase(ACCESS_BY_BUCKET_ACL)) {
            return;
        } else {
            throw new AnalysisException("Invalid value for property " + ACCESS_TYPE);
        }
    }

    private void analyzeStorageProperties() throws AnalysisException {
        for (String prop : STORAGE_REQUIRED_PROPERTIES) {
            if (!properties.containsKey(prop)) {
                throw new AnalysisException("Property " + prop + " is required for ExternalStage");
            }
        }
        // analyze prefix
        String prefix = properties.get(PREFIX);
        if (prefix == null) {
            prefix = "";
        }
        if (prefix.startsWith("/")) {
            prefix = prefix.substring(1);
        }
        if (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        if (prefix.startsWith("/") || prefix.endsWith("/")) {
            throw new AnalysisException("Property " + PREFIX + " with invalid value " + properties.get(PREFIX));
        }
        properties.put(PREFIX, prefix);
        // analyze provider
        // S3 Provider properties should be case insensitive.
        String provider = properties.get(PROVIDER).toUpperCase();
        if (!EnumUtils.isValidEnumIgnoreCase(ObjectStoreInfoPB.Provider.class, provider)) {
            throw new AnalysisException("Property " + PROVIDER + " with invalid value " + provider);
        }
    }

    protected void analyzeDryRun() throws AnalysisException {
        analyzeBooleanProperty(DRY_RUN);
    }

    public boolean isDryRun() {
        return properties.containsKey(DRY_RUN) ? Boolean.parseBoolean(properties.get(DRY_RUN)) : false;
    }

    public ObjectStoreInfoPB getObjectStoreInfoPB() {
        ObjectStoreInfoPB.Builder builder = ObjectStoreInfoPB.newBuilder().setEndpoint(properties.get(ENDPOINT))
                .setRegion(properties.get(REGION)).setBucket(properties.get(BUCKET)).setPrefix(properties.get(PREFIX))
                .setProvider(Provider.valueOf(properties.get(PROVIDER).toUpperCase()));
        if (getAccessType() == StageAccessType.AKSK) {
            builder.setAk(properties.get(AK)).setSk(properties.get(SK));
        }
        return builder.build();
    }

    public String getEndpoint() {
        return properties.get(ENDPOINT);
    }

    public String getComment() {
        return properties.containsKey(COMMENT) ? properties.get(COMMENT) : "";
    }

    public String getRoleName() {
        return properties.get(ROLE_NAME);
    }

    public String getArn() {
        return properties.get(ARN);
    }

    public StageAccessType getAccessType() {
        String accessType = properties.get(ACCESS_TYPE);
        if (accessType.equalsIgnoreCase(ACCESS_BY_AKSK)) {
            return StageAccessType.AKSK;
        } else if (accessType.equalsIgnoreCase(ACCESS_BY_IAM)) {
            return StageAccessType.IAM;
        } else if (accessType.equalsIgnoreCase(ACCESS_BY_BUCKET_ACL)) {
            return StageAccessType.BUCKET_ACL;
        }
        return StageAccessType.UNKNOWN;
    }

    public Map<String, String> getDefaultProperties() {
        Map<String, String> otherProperties = new HashMap<>();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (!STORAGE_PROPERTIES.contains(entry.getKey())) {
                otherProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return otherProperties;
    }

    public Map<String, String> getDefaultPropertiesWithoutPrefix() {
        Map<String, String> otherProperties = new HashMap<>();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (!STORAGE_PROPERTIES.contains(entry.getKey())) {
                otherProperties.put(removeKeyPrefix(entry.getKey()), entry.getValue());
            }
        }
        return otherProperties;
    }

    public Map<String, String> getStageTvfProperties() {
        Map<String, String> otherProperties = new HashMap<>();
        for (Entry<String, String> entry : getDefaultPropertiesWithoutPrefix().entrySet()) {
            if (entry.getKey().startsWith(COPY_PREFIX)) {
                continue;
            }
            if (entry.getKey().equals(TYPE)) {
                otherProperties.put(FileFormatConstants.PROP_FORMAT, entry.getValue());
            } else if (entry.getKey().equals(COMPRESSION)) {
                otherProperties.put(FileFormatConstants.PROP_COMPRESS, entry.getValue());
            } else {
                otherProperties.put(removeFilePrefix(entry.getKey()), entry.getValue());
            }
        }
        return otherProperties;
    }

    private String removeKeyPrefix(String key) {
        if (key.startsWith(prefix)) {
            return key.substring(prefix.length());
        }
        return key;
    }
}
