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

package org.apache.doris.connector.delta;

import org.apache.hadoop.conf.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

/** Normalizes Delta catalog storage properties for FE metadata IO and BE native file scans. */
final class DeltaStorageProperties {
    static final String S3_ENDPOINT = "AWS_ENDPOINT";
    static final String S3_REGION = "AWS_REGION";
    static final String S3_ACCESS_KEY = "AWS_ACCESS_KEY";
    static final String S3_SECRET_KEY = "AWS_SECRET_KEY";
    static final String S3_TOKEN = "AWS_TOKEN";
    static final String S3_TOKEN_EXPIRATION_TIME_MS = "AWS_TOKEN_EXPIRATION_TIME_MS";
    static final String USE_PATH_STYLE = "use_path_style";
    static final String GCS_PROVIDER = "GCP";

    private DeltaStorageProperties() {
    }

    static void configureHadoop(Configuration configuration, Map<String, String> properties) {
        String endpoint = firstNonBlank(properties,
                S3_ENDPOINT, "s3.endpoint", "endpoint", "ENDPOINT", "aws.endpoint",
                "fs.s3a.endpoint");
        String region = firstNonBlank(properties,
                S3_REGION, "s3.region", "region", "REGION", "client.region", "aws.region",
                "fs.s3a.endpoint.region", "fs.s3a.region");
        if (endpoint == null && region != null) {
            endpoint = defaultS3Endpoint(region);
        }
        String accessKey = firstNonBlank(properties,
                S3_ACCESS_KEY, "s3.access_key", "access_key", "ACCESS_KEY",
                "s3.access-key-id", "fs.s3a.access.key");
        String secretKey = firstNonBlank(properties,
                S3_SECRET_KEY, "s3.secret_key", "secret_key", "SECRET_KEY",
                "s3.secret-access-key", "fs.s3a.secret.key");
        String sessionToken = firstNonBlank(properties,
                S3_TOKEN, "s3.session_token", "session_token", "s3.session-token",
                "fs.s3a.session.token");
        validateCredentials(properties, accessKey, secretKey, sessionToken);

        if (endpoint != null || region != null || accessKey != null || secretKey != null) {
            configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            configuration.set("fs.s3.impl.disable.cache", "true");
            configuration.set("fs.s3a.impl.disable.cache", "true");
        }
        setIfPresent(configuration, "fs.s3a.endpoint", endpoint);
        setIfPresent(configuration, "fs.s3a.endpoint.region", region);
        setIfPresent(configuration, "fs.s3a.access.key", accessKey);
        setIfPresent(configuration, "fs.s3a.secret.key", secretKey);
        setIfPresent(configuration, "fs.s3a.session.token", sessionToken);
        if (accessKey != null && secretKey != null) {
            configuration.set("fs.s3a.aws.credentials.provider",
                    sessionToken == null
                            ? "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                            : "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
        }
        setIfPresent(configuration, "fs.s3a.path.style.access",
                firstNonBlank(properties, USE_PATH_STYLE, "s3.path-style-access",
                        "fs.s3a.path.style.access"));
        setIfPresent(configuration, "fs.s3a.connection.maximum",
                firstNonBlank(properties, "AWS_MAX_CONNECTIONS", "s3.connection.maximum",
                        "fs.s3a.connection.maximum"));
        setIfPresent(configuration, "fs.s3a.connection.request.timeout",
                firstNonBlank(properties, "AWS_REQUEST_TIMEOUT_MS", "s3.connection.request.timeout",
                        "fs.s3a.connection.request.timeout"));
        setIfPresent(configuration, "fs.s3a.connection.timeout",
                firstNonBlank(properties, "AWS_CONNECTION_TIMEOUT_MS", "s3.connection.timeout",
                        "fs.s3a.connection.timeout"));
    }

    static Map<String, String> toBackendProperties(Map<String, String> properties) {
        Map<String, String> backend = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (isBackendProperty(entry.getKey())) {
                backend.put(entry.getKey(), entry.getValue());
            }
        }
        String region = firstNonBlank(properties,
                S3_REGION, "s3.region", "region", "REGION", "client.region", "aws.region",
                "fs.s3a.endpoint.region", "fs.s3a.region");
        String endpoint = firstNonBlank(properties,
                S3_ENDPOINT, "s3.endpoint", "endpoint", "ENDPOINT", "aws.endpoint",
                "fs.s3a.endpoint");
        if (endpoint == null && region != null) {
            endpoint = defaultS3Endpoint(region);
        }
        String accessKey = firstNonBlank(properties,
                S3_ACCESS_KEY, "s3.access_key", "access_key", "ACCESS_KEY",
                "s3.access-key-id", "fs.s3a.access.key");
        String secretKey = firstNonBlank(properties,
                S3_SECRET_KEY, "s3.secret_key", "secret_key", "SECRET_KEY",
                "s3.secret-access-key", "fs.s3a.secret.key");
        String sessionToken = firstNonBlank(properties,
                S3_TOKEN, "s3.session_token", "session_token", "s3.session-token",
                "fs.s3a.session.token");
        validateCredentials(properties, accessKey, secretKey, sessionToken);
        putIfPresent(backend, S3_ENDPOINT, endpoint);
        putIfPresent(backend, S3_REGION, region);
        putIfPresent(backend, S3_ACCESS_KEY, accessKey);
        putIfPresent(backend, S3_SECRET_KEY, secretKey);
        putIfPresent(backend, S3_TOKEN, sessionToken);
        putIfPresent(backend, USE_PATH_STYLE, firstNonBlank(properties,
                USE_PATH_STYLE, "s3.path-style-access", "fs.s3a.path.style.access"));
        return backend;
    }

    static boolean isBackendProperty(String key) {
        return key.startsWith("AWS_") || key.equals("provider") || key.equals(USE_PATH_STYLE)
                || key.startsWith("fs.") || key.startsWith("dfs.") || key.startsWith("hadoop.")
                || key.startsWith("hive.") || key.startsWith("s3.") || key.startsWith("s3a.")
                || key.startsWith("cos.") || key.startsWith("oss.") || key.startsWith("obs.")
                || key.startsWith("azure.") || key.startsWith("adls.")
                || key.startsWith("gcs.") || key.startsWith("google.") || key.equals("uri")
                || key.startsWith("http.")
                || key.equals(S3_TOKEN_EXPIRATION_TIME_MS);
    }

    static String firstNonBlank(Map<String, String> properties, String... keys) {
        for (String key : keys) {
            String value = properties.get(key);
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    private static void setIfPresent(Configuration configuration, String key, String value) {
        if (value != null) {
            configuration.set(key, value);
        }
    }

    private static void putIfPresent(Map<String, String> properties, String key, String value) {
        if (value != null) {
            properties.put(key, value);
        }
    }

    private static void validateCredentials(Map<String, String> properties,
            String accessKey, String secretKey, String sessionToken) {
        String provider = firstNonBlank(properties, "provider");
        if (provider != null && provider.equalsIgnoreCase("AZURE")) {
            return;
        }
        if ((accessKey == null) != (secretKey == null)) {
            throw new IllegalArgumentException(
                    "Delta S3 storage credentials require both access key and secret key");
        }
        if (sessionToken != null && accessKey == null) {
            throw new IllegalArgumentException(
                    "Delta S3 session credentials require access key and secret key");
        }
    }

    private static String defaultS3Endpoint(String region) {
        return "s3." + region + ".amazonaws.com";
    }
}
