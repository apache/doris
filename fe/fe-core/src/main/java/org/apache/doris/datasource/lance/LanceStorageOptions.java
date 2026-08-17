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

package org.apache.doris.datasource.lance;

import java.util.HashMap;
import java.util.Map;

/** Converts normalized Doris storage properties to Lance object-store options. */
public final class LanceStorageOptions {
    private static final Map<String, String> S3_KEYS = new HashMap<>();

    static {
        S3_KEYS.put("AWS_ACCESS_KEY", "aws_access_key_id");
        S3_KEYS.put("AWS_SECRET_KEY", "aws_secret_access_key");
        S3_KEYS.put("AWS_TOKEN", "aws_session_token");
        S3_KEYS.put("AWS_ENDPOINT", "aws_endpoint");
        S3_KEYS.put("AWS_REGION", "aws_region");
    }

    private LanceStorageOptions() {
    }

    public static Map<String, String> forJavaSdk(Map<String, String> backendProperties) {
        Map<String, String> result = new HashMap<>();
        S3_KEYS.forEach((dorisKey, lanceKey) -> putIfNotEmpty(result, lanceKey,
                backendProperties.get(dorisKey)));

        String endpoint = backendProperties.get("AWS_ENDPOINT");
        if (endpoint != null && endpoint.startsWith("http://")) {
            result.put("allow_http", "true");
        }
        String usePathStyle = backendProperties.get("use_path_style");
        if (usePathStyle != null && !usePathStyle.isEmpty()) {
            result.put("aws_virtual_hosted_style_request",
                    String.valueOf(!Boolean.parseBoolean(usePathStyle)));
        }
        return result;
    }

    /** Merge Lance storage options returned by a namespace into properties understood by Doris BE. */
    public static Map<String, String> forBackend(Map<String, String> staticBackendProperties,
            Map<String, String> lanceStorageOptions) {
        Map<String, String> result = new HashMap<>(staticBackendProperties);
        if (lanceStorageOptions == null || lanceStorageOptions.isEmpty()) {
            return result;
        }
        S3_KEYS.forEach((dorisKey, lanceKey) -> putIfNotEmpty(result, dorisKey,
                lanceStorageOptions.get(lanceKey)));

        String virtualHostedStyle = lanceStorageOptions.get("aws_virtual_hosted_style_request");
        if (virtualHostedStyle != null && !virtualHostedStyle.isEmpty()) {
            result.put("use_path_style", String.valueOf(!Boolean.parseBoolean(virtualHostedStyle)));
        }
        return result;
    }

    private static void putIfNotEmpty(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
