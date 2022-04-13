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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * S3 resource for olap table
 *
 * Syntax:
 * CREATE RESOURCE "remote_s3"
 * PROPERTIES
 * (
 *    "type" = "s3",
 *    "s3_endpoint" = "bj",
 *    "s3_region" = "bj",
 *    "s3_root_path" = "/path/to/root",
 *    "s3_access_key" = "bbb",
 *    "s3_secret_key" = "aaaa",
 *    "s3_max_connections" = "50",
 *    "s3_request_timeout_ms" = "3000",
 *    "s3_connection_timeout_ms" = "1000"
 * );
 */
public class S3Resource extends Resource {
    // required
    private static final String S3_ENDPOINT = "s3_endpoint";
    private static final String S3_REGION = "s3_region";
    private static final String S3_ROOT_PATH = "s3_root_path";
    private static final String S3_ACCESS_KEY = "s3_access_key";
    private static final String S3_SECRET_KEY = "s3_secret_key";

    // optional
    private static final String S3_MAX_CONNECTIONS = "s3_max_connections";
    private static final String S3_REQUEST_TIMEOUT_MS = "s3_request_timeout_ms";
    private static final String S3_CONNECTION_TIMEOUT_MS = "s3_connection_timeout_ms";
    private static final String DEFAULT_S3_MAX_CONNECTIONS = "50";
    private static final String DEFAULT_S3_REQUEST_TIMEOUT_MS = "3000";
    private static final String DEFAULT_S3_CONNECTION_TIMEOUT_MS = "1000";

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
        checkRequiredProperty(S3_ROOT_PATH);
        checkRequiredProperty(S3_ACCESS_KEY);
        checkRequiredProperty(S3_SECRET_KEY);
        // optional
        checkOptionalProperty(S3_MAX_CONNECTIONS, DEFAULT_S3_MAX_CONNECTIONS);
        checkOptionalProperty(S3_REQUEST_TIMEOUT_MS, DEFAULT_S3_REQUEST_TIMEOUT_MS);
        checkOptionalProperty(S3_CONNECTION_TIMEOUT_MS, DEFAULT_S3_CONNECTION_TIMEOUT_MS);
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
        // modify properties
        replaceIfEffectiveValue(this.properties, S3_ENDPOINT, properties.get(S3_ENDPOINT));
        replaceIfEffectiveValue(this.properties, S3_REGION, properties.get(S3_REGION));
        replaceIfEffectiveValue(this.properties, S3_ROOT_PATH, properties.get(S3_ROOT_PATH));
        replaceIfEffectiveValue(this.properties, S3_ACCESS_KEY, properties.get(S3_ACCESS_KEY));
        replaceIfEffectiveValue(this.properties, S3_SECRET_KEY, properties.get(S3_SECRET_KEY));
        replaceIfEffectiveValue(this.properties, S3_MAX_CONNECTIONS, properties.get(S3_MAX_CONNECTIONS));
        replaceIfEffectiveValue(this.properties, S3_REQUEST_TIMEOUT_MS, properties.get(S3_REQUEST_TIMEOUT_MS));
        replaceIfEffectiveValue(this.properties, S3_CONNECTION_TIMEOUT_MS, properties.get(S3_CONNECTION_TIMEOUT_MS));
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        // check properties
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        copiedProperties.remove(S3_ENDPOINT);
        copiedProperties.remove(S3_REGION);
        copiedProperties.remove(S3_ROOT_PATH);
        copiedProperties.remove(S3_ACCESS_KEY);
        copiedProperties.remove(S3_SECRET_KEY);
        copiedProperties.remove(S3_MAX_CONNECTIONS);
        copiedProperties.remove(S3_REQUEST_TIMEOUT_MS);
        copiedProperties.remove(S3_CONNECTION_TIMEOUT_MS);

        if (!copiedProperties.isEmpty()) {
            throw new AnalysisException("Unknown S3 resource properties: " + copiedProperties);
        }
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            // it's dangerous to show password in show odbc resource,
            // so we use empty string to replace the real password
            if (entry.getKey().equals(S3_ACCESS_KEY)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), ""));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }
}
