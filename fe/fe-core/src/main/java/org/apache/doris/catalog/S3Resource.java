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
import org.apache.doris.datasource.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.S3Properties;

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
 * <p>
 * For AWS S3, BE need following properties:
 * 1. AWS_ACCESS_KEY: ak
 * 2. AWS_SECRET_KEY: sk
 * 3. AWS_ENDPOINT: s3.us-east-1.amazonaws.com
 * 4. AWS_REGION: us-east-1
 * And file path: s3://bucket_name/csv/taxi.csv
 */
public class S3Resource extends Resource {
    private static final Logger LOG = LogManager.getLogger(S3Resource.class);
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    // for Gson fromJson
    // TODO(plat1ko): other Resource subclass also MUST define default ctor, otherwise when reloading object from json
    //  some not serialized field (i.e. `lock`) will be `null`.
    public S3Resource() {}

    public S3Resource(String name) {
        super(name, ResourceType.S3);
        properties = Maps.newHashMap();
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        this.properties = properties;
        // check properties
        if (properties.containsKey(S3Properties.ENDPOINT)) {
            checkRequiredS3Properties();
        } else {
            checkRequiredS3EnvProperties();
        }
        // default need check resource conf valid, so need fix ut and regression case
        boolean needCheck = !properties.containsKey(S3Properties.VALIDITY_CHECK)
                || Boolean.parseBoolean(properties.get(S3Properties.VALIDITY_CHECK));
        LOG.debug("s3 info need check validity : {}", needCheck);
        if (needCheck) {
            boolean available = pingS3(this.properties);
            if (!available) {
                throw new DdlException("S3 can't use, please check your properties");
            }
        }
        // optional
        checkOptionalS3Property();
    }

    private boolean pingS3(Map<String, String> properties) {
        String bucket = "s3://" + properties.getOrDefault(S3Properties.Environment.BUCKET, "") + "/";
        Map<String, String> propertiesPing = new HashMap<>();
        CloudCredentialWithEndpoint credential = S3Properties.getEnvironmentCredentialWithEndpoint(properties);
        propertiesPing.put("AWS_ACCESS_KEY", credential.getAccessKey());
        propertiesPing.put("AWS_SECRET_KEY", credential.getSecretKey());
        propertiesPing.put("AWS_ENDPOINT", "http://" + credential.getEndpoint());
        propertiesPing.put("AWS_REGION", credential.getRegion());
        propertiesPing.put(PropertyConverter.USE_PATH_STYLE, "false");
        S3Storage storage = new S3Storage(propertiesPing);

        String testFile = bucket + properties.getOrDefault(S3Properties.ROOT_PATH, "")
                + "/test-object-valid.txt";
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

    private void checkRequiredS3EnvProperties() throws DdlException {
        checkRequiredProperty(S3Properties.Environment.ENDPOINT);
        checkRequiredProperty(S3Properties.Environment.REGION);
        checkRequiredProperty(S3Properties.Environment.ACCESS_KEY);
        checkRequiredProperty(S3Properties.Environment.SECRET_KEY);
        checkRequiredProperty(S3Properties.Environment.BUCKET);
    }

    private void checkRequiredS3Properties() throws DdlException {
        checkRequiredProperty(S3Properties.ENDPOINT);
        checkRequiredProperty(S3Properties.REGION);
        checkRequiredProperty(S3Properties.ACCESS_KEY);
        checkRequiredProperty(S3Properties.SECRET_KEY);
        checkRequiredProperty(S3Properties.BUCKET);
    }

    private void checkRequiredProperty(String propertyKey) throws DdlException {
        String value = properties.get(propertyKey);

        if (Strings.isNullOrEmpty(value)) {
            throw new DdlException("Missing [" + propertyKey + "] in properties.");
        }
    }

    private void checkOptionalS3Property() {
        if (properties.containsKey(S3Properties.ENDPOINT)) {
            checkOptionalProperty(S3Properties.MAX_CONNECTIONS,
                    S3Properties.Environment.DEFAULT_MAX_CONNECTIONS);
            checkOptionalProperty(S3Properties.REQUEST_TIMEOUT_MS,
                    S3Properties.Environment.DEFAULT_REQUEST_TIMEOUT_MS);
            checkOptionalProperty(S3Properties.CONNECTION_TIMEOUT_MS,
                    S3Properties.Environment.DEFAULT_CONNECTION_TIMEOUT_MS);
        } else {
            checkOptionalProperty(S3Properties.Environment.MAX_CONNECTIONS,
                    S3Properties.Environment.DEFAULT_MAX_CONNECTIONS);
            checkOptionalProperty(S3Properties.Environment.REQUEST_TIMEOUT_MS,
                    S3Properties.Environment.DEFAULT_REQUEST_TIMEOUT_MS);
            checkOptionalProperty(S3Properties.Environment.CONNECTION_TIMEOUT_MS,
                    S3Properties.Environment.DEFAULT_CONNECTION_TIMEOUT_MS);
        }
    }

    private void checkOptionalProperty(String propertyKey, String defaultValue) {
        this.properties.putIfAbsent(propertyKey, defaultValue);
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        if (references.containsValue(ReferenceType.POLICY)) {
            // can't change, because remote fs use it info to find data.
            List<String> cantChangeProperties = Arrays.asList(S3Properties.ENDPOINT, S3Properties.REGION,
                    S3Properties.ROOT_PATH, S3Properties.BUCKET);
            Optional<String> any = cantChangeProperties.stream().filter(properties::containsKey).findAny();
            if (any.isPresent()) {
                throw new DdlException("current not support modify property : " + any.get());
            }
        }

        boolean needCheck = !this.properties.containsKey(S3Properties.VALIDITY_CHECK)
                || Boolean.parseBoolean(this.properties.get(S3Properties.VALIDITY_CHECK));
        if (properties.containsKey(S3Properties.VALIDITY_CHECK)) {
            needCheck = Boolean.parseBoolean(properties.get(S3Properties.VALIDITY_CHECK));
        }
        LOG.debug("s3 info need check validity : {}", needCheck);
        if (needCheck) {
            Map<String, String> s3Properties = new HashMap<>();
            s3Properties.put(S3Properties.Environment.BUCKET, properties.containsKey(S3Properties.BUCKET)
                    ? properties.get(S3Properties.BUCKET)
                    : this.properties.getOrDefault(S3Properties.BUCKET, ""));

            s3Properties.put(S3Properties.Environment.ACCESS_KEY, properties.containsKey(S3Properties.ACCESS_KEY)
                    ? properties.get(S3Properties.ACCESS_KEY)
                    : this.properties.getOrDefault(S3Properties.ACCESS_KEY, ""));

            s3Properties.put(S3Properties.Environment.SECRET_KEY, properties.containsKey(S3Properties.SECRET_KEY)
                    ? properties.get(S3Properties.SECRET_KEY)
                    : this.properties.getOrDefault(S3Properties.SECRET_KEY, ""));

            s3Properties.put(S3Properties.Environment.ENDPOINT, properties.containsKey(S3Properties.ENDPOINT)
                    ? properties.get(S3Properties.ENDPOINT)
                    : this.properties.getOrDefault(S3Properties.ENDPOINT, ""));

            s3Properties.put(S3Properties.Environment.REGION, properties.containsKey(S3Properties.REGION)
                    ? properties.get(S3Properties.REGION)
                    : this.properties.getOrDefault(S3Properties.REGION, ""));

            s3Properties.put(S3Properties.Environment.ROOT_PATH, properties.containsKey(S3Properties.ROOT_PATH)
                    ? properties.get(S3Properties.ROOT_PATH)
                    : this.properties.getOrDefault(S3Properties.ROOT_PATH, ""));
            boolean available = pingS3(s3Properties);
            if (!available) {
                throw new DdlException("S3 can't use, please check your properties");
            }
        }

        // modify properties
        writeLock();
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        ++version;
        writeUnlock();
        super.modifyProperties(properties);
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        result.addRow(Lists.newArrayList(name, lowerCaseType, "id", String.valueOf(id)));
        readLock();
        result.addRow(Lists.newArrayList(name, lowerCaseType, "version", String.valueOf(version)));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            // it's dangerous to show password in show odbc resource,
            // so we use empty string to replace the real password
            if (entry.getKey().equals(S3Properties.Environment.SECRET_KEY)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
        readUnlock();
    }
}
