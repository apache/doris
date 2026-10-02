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

/** Property names for the native Delta connector. */
public final class DeltaConnectorProperties {

    public static final String TYPE = "delta";
    public static final String CATALOG_TYPE = "delta.catalog.type";
    public static final String CATALOG_TYPE_PATH = "path";
    public static final String CATALOG_TYPE_UNITY = "unity";
    public static final String TABLE_PATH = "delta.table.path";
    public static final String DATABASE = "delta.database";
    public static final String TABLE = "delta.table";
    public static final String UNITY_URI = "unity.uri";
    public static final String UNITY_AUTH_TYPE = "unity.auth.type";
    public static final String UNITY_CATALOG = "unity.catalog";
    public static final String UNITY_TOKEN = "unity.token";
    public static final String UNITY_OAUTH_URI = "unity.oauth.uri";
    public static final String UNITY_OAUTH_CLIENT_ID = "unity.oauth.client-id";
    public static final String UNITY_OAUTH_CLIENT_SECRET = "unity.oauth.client-secret";
    public static final String UNITY_CONNECT_TIMEOUT_MS = "unity.connect-timeout-ms";
    public static final String UNITY_READ_TIMEOUT_MS = "unity.read-timeout-ms";
    public static final String UNITY_CREDENTIAL_MIN_LIFETIME_MS =
            "unity.credential-min-lifetime-ms";
    public static final long DEFAULT_UNITY_CONNECT_TIMEOUT_MS = 10_000L;
    public static final long DEFAULT_UNITY_READ_TIMEOUT_MS = 30_000L;
    public static final long DEFAULT_UNITY_CREDENTIAL_MIN_LIFETIME_MS = 60_000L;
    public static final String WRITE_ENABLED = "delta.write.enabled";
    public static final String CREATE_ENABLED = "delta.create.enabled";
    public static final String DROP_ENABLED = "delta.drop.enabled";

    public static String catalogType(java.util.Map<String, String> properties) {
        String configured = properties.get(CATALOG_TYPE);
        if (configured != null && !configured.trim().isEmpty()) {
            return configured.trim().toLowerCase(java.util.Locale.ROOT);
        }
        return properties.containsKey(TABLE_PATH) ? CATALOG_TYPE_PATH : CATALOG_TYPE_UNITY;
    }

    static long positiveLongProperty(java.util.Map<String, String> properties, String key,
            long defaultValue) {
        String value = properties.get(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            if (parsed <= 0) {
                throw new IllegalArgumentException(
                        "Delta catalog property '" + key + "' must be greater than zero");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Delta catalog property '" + key + "' must be a positive integer", e);
        }
    }

    private DeltaConnectorProperties() {
    }
}
