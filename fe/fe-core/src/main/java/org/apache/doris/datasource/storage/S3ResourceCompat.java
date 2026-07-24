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

package org.apache.doris.datasource.storage;

import org.apache.doris.common.DdlException;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Compatibility home for the image/DDL-bearing S3 user-key constants and the pure static map
 * utilities that {@code S3Resource}/{@code AzureResource} (frozen "move, don't rewrite" classes)
 * depend on. Everything here is a VERBATIM move from the legacy typed S3 properties class —
 * the literals are wire/image contracts and
 * the method bodies are frozen; guarded by {@code S3ResourcePersistParityTest}. Do not "improve"
 * anything in this file.
 */
public final class S3ResourceCompat {

    public static final String ENDPOINT = S3CompatibleFileSystemProperties.PROP_ENDPOINT;
    public static final String REGION = S3CompatibleFileSystemProperties.PROP_REGION;
    public static final String ACCESS_KEY = S3CompatibleFileSystemProperties.PROP_ACCESS_KEY;
    public static final String SECRET_KEY = S3CompatibleFileSystemProperties.PROP_SECRET_KEY;
    public static final String SESSION_TOKEN = S3CompatibleFileSystemProperties.PROP_SESSION_TOKEN;
    public static final String MAX_CONNECTIONS = S3CompatibleFileSystemProperties.PROP_MAX_CONNECTIONS;
    public static final String REQUEST_TIMEOUT_MS = S3CompatibleFileSystemProperties.PROP_REQUEST_TIMEOUT_MS;
    public static final String CONNECTION_TIMEOUT_MS = S3CompatibleFileSystemProperties.PROP_CONNECTION_TIMEOUT_MS;
    public static final String ROOT_PATH = S3CompatibleFileSystemProperties.PROP_ROOT_PATH;
    public static final String BUCKET = S3CompatibleFileSystemProperties.PROP_BUCKET;
    public static final String ROLE_ARN = S3CompatibleFileSystemProperties.PROP_ROLE_ARN;
    public static final String EXTERNAL_ID = S3CompatibleFileSystemProperties.PROP_EXTERNAL_ID;
    public static final String CREDENTIALS_PROVIDER_TYPE =
            S3CompatibleFileSystemProperties.PROP_CREDENTIALS_PROVIDER_TYPE;
    public static final String USE_PATH_STYLE = S3CompatibleFileSystemProperties.PROP_USE_PATH_STYLE;
    public static final String VALIDITY_CHECK = "s3_validity_check";
    public static final String FS_PROVIDER_KEY = S3CompatibleFileSystemProperties.PROP_PROVIDER;

    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT);
    public static final List<String> PROVIDERS =
            Arrays.asList("COS", "OSS", "S3", "OBS", "BOS", "AZURE", "GCP", "TOS");

    private static final Pattern IPV4_PORT_PATTERN =
            Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3}:\\d{1,5})");

    /** Legacy env-style (AWS_*) key namespace; values are frozen wire/image literals. */
    public static class Env {
        public static final String ENDPOINT = "AWS_ENDPOINT";
        public static final String REGION = "AWS_REGION";
        public static final String ACCESS_KEY = "AWS_ACCESS_KEY";
        public static final String SECRET_KEY = "AWS_SECRET_KEY";
        public static final String TOKEN = "AWS_TOKEN";
        public static final String ROOT_PATH = "AWS_ROOT_PATH";
        public static final String BUCKET = "AWS_BUCKET";
        public static final String MAX_CONNECTIONS = "AWS_MAX_CONNECTIONS";
        public static final String REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
        public static final String CONNECTION_TIMEOUT_MS = "AWS_CONNECTION_TIMEOUT_MS";
        public static final String DEFAULT_MAX_CONNECTIONS =
                S3CompatibleFileSystemProperties.DEFAULT_MAX_CONNECTIONS_VALUE;
        public static final String DEFAULT_REQUEST_TIMEOUT_MS =
                S3CompatibleFileSystemProperties.DEFAULT_REQUEST_TIMEOUT_MS_VALUE;
        public static final String DEFAULT_CONNECTION_TIMEOUT_MS =
                S3CompatibleFileSystemProperties.DEFAULT_CONNECTION_TIMEOUT_MS_VALUE;
        public static final String ROLE_ARN = "AWS_ROLE_ARN";
        public static final String EXTERNAL_ID = "AWS_EXTERNAL_ID";
        public static final String CREDENTIALS_PROVIDER_TYPE = "AWS_CREDENTIALS_PROVIDER_TYPE";
        public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT);
    }

    private S3ResourceCompat() {
    }

    public static void requiredS3PingProperties(Map<String, String> properties) throws DdlException {
        requiredS3Properties(properties);
        checkRequiredProperty(properties, BUCKET);
    }

    public static void requiredS3Properties(Map<String, String> properties) throws DdlException {
        // Try to convert env properties to uniform properties
        // compatible with old version
        convertToStdProperties(properties);
        if (properties.containsKey(Env.ENDPOINT)
                && !properties.containsKey(ENDPOINT)) {
            for (String field : Env.REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        } else {
            for (String field : REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        }
        if (StringUtils.isNotBlank(properties.get(FS_PROVIDER_KEY))) {
            // S3 Provider properties should be case insensitive.
            if (!PROVIDERS.stream().anyMatch(s -> s.equals(properties.get(FS_PROVIDER_KEY).toUpperCase()))) {
                throw new DdlException("Provider must be one of OSS, OBS, AZURE, BOS, COS, S3, GCP");
            }
        }

    }

    public static void checkRequiredProperty(Map<String, String> properties, String propertyKey)
            throws DdlException {
        String value = properties.get(propertyKey);
        if (StringUtils.isBlank(value)) {
            throw new DdlException("Missing [" + propertyKey + "] in properties.");
        }
    }

    public static void optionalS3Property(Map<String, String> properties) {
        properties.putIfAbsent(MAX_CONNECTIONS, Env.DEFAULT_MAX_CONNECTIONS);
        properties.putIfAbsent(REQUEST_TIMEOUT_MS, Env.DEFAULT_REQUEST_TIMEOUT_MS);
        properties.putIfAbsent(CONNECTION_TIMEOUT_MS, Env.DEFAULT_CONNECTION_TIMEOUT_MS);
        // compatible with old version
        properties.putIfAbsent(Env.MAX_CONNECTIONS, Env.DEFAULT_MAX_CONNECTIONS);
        properties.putIfAbsent(Env.REQUEST_TIMEOUT_MS, Env.DEFAULT_REQUEST_TIMEOUT_MS);
        properties.putIfAbsent(Env.CONNECTION_TIMEOUT_MS, Env.DEFAULT_CONNECTION_TIMEOUT_MS);
    }

    public static void convertToStdProperties(Map<String, String> properties) {
        if (properties.containsKey(Env.ENDPOINT)) {
            properties.putIfAbsent(ENDPOINT, properties.get(Env.ENDPOINT));
        }
        if (properties.containsKey(Env.REGION)) {
            properties.putIfAbsent(REGION, properties.get(Env.REGION));
        }
        if (properties.containsKey(Env.ACCESS_KEY)) {
            properties.putIfAbsent(ACCESS_KEY, properties.get(Env.ACCESS_KEY));
        }
        if (properties.containsKey(Env.SECRET_KEY)) {
            properties.putIfAbsent(SECRET_KEY, properties.get(Env.SECRET_KEY));
        }
        if (properties.containsKey(Env.TOKEN)) {
            properties.putIfAbsent(SESSION_TOKEN, properties.get(Env.TOKEN));
        }
        if (properties.containsKey(Env.MAX_CONNECTIONS)) {
            properties.putIfAbsent(MAX_CONNECTIONS, properties.get(Env.MAX_CONNECTIONS));
        }
        if (properties.containsKey(Env.REQUEST_TIMEOUT_MS)) {
            properties.putIfAbsent(REQUEST_TIMEOUT_MS,
                    properties.get(Env.REQUEST_TIMEOUT_MS));

        }
        if (properties.containsKey(Env.CONNECTION_TIMEOUT_MS)) {
            properties.putIfAbsent(CONNECTION_TIMEOUT_MS,
                    properties.get(Env.CONNECTION_TIMEOUT_MS));
        }
        if (properties.containsKey(Env.ROOT_PATH)) {
            properties.putIfAbsent(ROOT_PATH, properties.get(Env.ROOT_PATH));
        }
        if (properties.containsKey(Env.BUCKET)) {
            properties.putIfAbsent(BUCKET, properties.get(Env.BUCKET));
        }
        if (properties.containsKey(USE_PATH_STYLE)) {
            properties.putIfAbsent(USE_PATH_STYLE, properties.get(USE_PATH_STYLE));
        }

        if (properties.containsKey(Env.ROLE_ARN)) {
            properties.putIfAbsent(ROLE_ARN, properties.get(Env.ROLE_ARN));
        }

        if (properties.containsKey(Env.EXTERNAL_ID)) {
            properties.putIfAbsent(EXTERNAL_ID, properties.get(Env.EXTERNAL_ID));
        }

        if (properties.containsKey(Env.CREDENTIALS_PROVIDER_TYPE)) {
            properties.putIfAbsent(CREDENTIALS_PROVIDER_TYPE, properties.get(Env.CREDENTIALS_PROVIDER_TYPE));
        }
    }

    public static String getRegionOfEndpoint(String endpoint) {
        if (IPV4_PORT_PATTERN.matcher(endpoint).find()) {
            // if endpoint contains '192.168.0.1:8999', return null region
            return null;
        }
        String[] endpointSplit = endpoint.replace("http://", "")
                .replace("https://", "")
                .split("\\.");
        if (endpointSplit.length < 2) {
            return null;
        }
        if (endpointSplit[0].contains("oss-")) {
            // compatible with the endpoint: oss-cn-bejing.aliyuncs.com
            return endpointSplit[0];
        }
        return endpointSplit[1];
    }
}
