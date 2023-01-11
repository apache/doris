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

package org.apache.doris.catalog;

import org.apache.doris.backup.S3Storage;
import org.apache.doris.backup.Status;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * S3 resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "remote_s3"
 * PROPERTIES
 * (
 * "type" = "s3",
 * "AWS_ENDPOINT" = "bj",
 * "AWS_REGION" = "bj",
 * "AWS_ROOT_PATH" = "/path/to/root",
 * "AWS_ACCESS_KEY" = "bbb",
 * "AWS_SECRET_KEY" = "aaaa",
 * "AWS_MAX_CONNECTION" = "50",
 * "AWS_REQUEST_TIMEOUT_MS" = "3000",
 * "AWS_CONNECTION_TIMEOUT_MS" = "1000"
 * );
 */
public class S3Resource extends Resource {
    private static final Logger LOG = LogManager.getLogger(S3Resource.class);
    public static final String S3_PROPERTIES_PREFIX = "AWS";
    public static final String S3_FS_PREFIX = "fs.s3";
    // required
    public static final String S3_ENDPOINT = "AWS_ENDPOINT";
    public static final String S3_REGION = "AWS_REGION";
    public static final String S3_ACCESS_KEY = "AWS_ACCESS_KEY";
    public static final String S3_SECRET_KEY = "AWS_SECRET_KEY";
    public static final List<String> REQUIRED_FIELDS =
            Arrays.asList(S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY);
    // required by storage policy
    public static final String S3_ROOT_PATH = "AWS_ROOT_PATH";
    public static final String S3_BUCKET = "AWS_BUCKET";

    private static final String S3_VALIDITY_CHECK = "s3_validity_check";

    // optional
    public static final String S3_TOKEN = "AWS_TOKEN";
    public static final String USE_PATH_STYLE = "use_path_style";
    public static final String S3_MAX_CONNECTIONS = "AWS_MAX_CONNECTIONS";
    public static final String S3_REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
    public static final String S3_CONNECTION_TIMEOUT_MS = "AWS_CONNECTION_TIMEOUT_MS";
    public static final String DEFAULT_S3_MAX_CONNECTIONS = "50";
    public static final String DEFAULT_S3_REQUEST_TIMEOUT_MS = "3000";
    public static final String DEFAULT_S3_CONNECTION_TIMEOUT_MS = "1000";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public S3Resource(String name) {
        this(name, Maps.newHashMap());
    }

    public S3Resource(String name, Map<String, String> properties) {
        super(name, ResourceType.S3);
        this.properties = properties;
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        this.properties = properties;
        // check properties
        // required
        checkRequiredProperty(S3_ENDPOINT);
        checkRequiredProperty(S3_REGION);
        checkRequiredProperty(S3_ACCESS_KEY);
        checkRequiredProperty(S3_SECRET_KEY);
        checkRequiredProperty(S3_BUCKET);

        // default need check resource conf valid, so need fix ut and regression case
        boolean needCheck = !properties.containsKey(S3_VALIDITY_CHECK)
                || Boolean.parseBoolean(properties.get(S3_VALIDITY_CHECK));
        LOG.debug("s3 info need check validity : {}", needCheck);
        if (needCheck) {
            boolean available = pingS3();
            if (!available) {
                throw new DdlException("S3 can't use, please check your properties");
            }
        }

        // optional
        checkOptionalProperty(S3_MAX_CONNECTIONS, DEFAULT_S3_MAX_CONNECTIONS);
        checkOptionalProperty(S3_REQUEST_TIMEOUT_MS, DEFAULT_S3_REQUEST_TIMEOUT_MS);
        checkOptionalProperty(S3_CONNECTION_TIMEOUT_MS, DEFAULT_S3_CONNECTION_TIMEOUT_MS);
    }

    private boolean pingS3() {
        String bucket = "s3://" + properties.getOrDefault(S3_BUCKET, "") + "/";
        Map<String, String> propertiesPing = new HashMap<>();
        propertiesPing.put("AWS_ACCESS_KEY", properties.getOrDefault(S3_ACCESS_KEY, ""));
        propertiesPing.put("AWS_SECRET_KEY", properties.getOrDefault(S3_SECRET_KEY, ""));
        propertiesPing.put("AWS_ENDPOINT", "http://" + properties.getOrDefault(S3_ENDPOINT, ""));
        propertiesPing.put("AWS_REGION", properties.getOrDefault(S3_REGION, ""));
        propertiesPing.put(S3Resource.USE_PATH_STYLE, "false");
        S3Storage storage = new S3Storage(propertiesPing);

        String testFile = bucket + properties.getOrDefault(S3_ROOT_PATH, "") + "/test-object-valid.txt";
        String content = "doris will be better";
        try {
            Status status = storage.directUpload(content, testFile);
            if (status != Status.OK) {
                LOG.warn("ping update file status: {}, properties: {}", status, propertiesPing);
                return false;
            }
        } finally {
            Status delete = storage.delete(testFile);
            if (delete != Status.OK) {
                LOG.warn("ping delete file status: {}, properties: {}", delete, propertiesPing);
                return false;
            }
        }

        LOG.info("success to ping s3");
        return true;
    }

    private void checkRequiredProperty(String propertyKey) throws DdlException {
        String value = properties.get(propertyKey);

        if (Strings.isNullOrEmpty(value)) {
            throw new DdlException("Missing [" + propertyKey + "] in properties.");
        }
    }

    private void checkOptionalProperty(String propertyKey, String defaultValue) {
        this.properties.putIfAbsent(propertyKey, defaultValue);
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        if (references.containsValue(ReferenceType.POLICY)) {
            // can't change, because remote fs use it info to find data.
            List<String> cantChangeProperties = Arrays.asList(S3_ENDPOINT, S3_REGION, S3_ROOT_PATH, S3_BUCKET);
            Optional<String> any = cantChangeProperties.stream().filter(properties::containsKey).findAny();
            if (any.isPresent()) {
                throw new DdlException("current not support modify property : " + any.get());
            }
        }
        // modify properties
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        super.modifyProperties(properties);
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            // it's dangerous to show password in show odbc resource,
            // so we use empty string to replace the real password
            if (entry.getKey().equals(S3_SECRET_KEY)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }

    public Map<String, String> getS3HadoopProperties() {
        return getS3HadoopProperties(properties);
    }

    public static Map<String, String> getS3HadoopProperties(Map<String, String> properties) {
        Map<String, String> s3Properties = Maps.newHashMap();
        if (properties.containsKey(S3_ACCESS_KEY)) {
            s3Properties.put("fs.s3a.access.key", properties.get(S3_ACCESS_KEY));
        }
        if (properties.containsKey(S3Resource.S3_SECRET_KEY)) {
            s3Properties.put("fs.s3a.secret.key", properties.get(S3_SECRET_KEY));
        }
        if (properties.containsKey(S3Resource.S3_ENDPOINT)) {
            s3Properties.put("fs.s3a.endpoint", properties.get(S3_ENDPOINT));
        }
        if (properties.containsKey(S3Resource.S3_REGION)) {
            s3Properties.put("fs.s3a.endpoint.region", properties.get(S3_REGION));
        }
        if (properties.containsKey(S3Resource.S3_MAX_CONNECTIONS)) {
            s3Properties.put("fs.s3a.connection.maximum", properties.get(S3_MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Resource.S3_REQUEST_TIMEOUT_MS)) {
            s3Properties.put("fs.s3a.connection.request.timeout", properties.get(S3_REQUEST_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Resource.S3_CONNECTION_TIMEOUT_MS)) {
            s3Properties.put("fs.s3a.connection.timeout", properties.get(S3_CONNECTION_TIMEOUT_MS));
        }
        s3Properties.put("fs.s3.impl.disable.cache", "true");
        s3Properties.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        s3Properties.put("fs.s3a.attempts.maximum", "2");

        if (Boolean.valueOf(properties.getOrDefault(S3Resource.USE_PATH_STYLE, "false")).booleanValue()) {
            s3Properties.put("fs.s3a.path.style.access", "true");
        } else {
            s3Properties.put("fs.s3a.path.style.access", "false");
        }
        if (properties.containsKey(S3Resource.S3_TOKEN)) {
            s3Properties.put("fs.s3a.session.token", properties.get(S3_TOKEN));
            s3Properties.put("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
            s3Properties.put("fs.s3a.impl.disable.cache", "true");
            s3Properties.put("fs.s3.impl.disable.cache", "true");
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(S3Resource.S3_FS_PREFIX)) {
                s3Properties.put(entry.getKey(), entry.getValue());
            }
        }
        return s3Properties;
    }
}
