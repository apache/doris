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

package org.apache.doris.catalog.authorizer.openfga;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Configuration for the OpenFGA access controller, parsed from the per-catalog property map.
 *
 * <p>These properties come from the {@code access_controller.properties.*} prefix on a catalog (the
 * prefix is stripped by {@code ExternalCatalog.initAccessController} before it reaches the factory),
 * or from the authorization config file for the internal catalog. Example:
 *
 * <pre>
 *   "access_controller.class" = "openfga-doris",
 *   "access_controller.properties.api_url" = "http://openfga:8080",
 *   "access_controller.properties.store_id" = "01J...",
 *   "access_controller.properties.model_id" = "01J...",
 *   "access_controller.properties.api_token" = "&lt;preshared-key&gt;"
 * </pre>
 */
public class OpenFgaConfig {
    public static final String API_URL = "api_url";
    public static final String STORE_ID = "store_id";
    public static final String MODEL_ID = "model_id";
    // Either key is accepted for the preshared key / bearer token.
    public static final String TOKEN = "token";
    public static final String API_TOKEN = "api_token";
    public static final String CONNECT_TIMEOUT_MILLIS = "connect_timeout_millis";
    public static final String READ_TIMEOUT_MILLIS = "read_timeout_millis";
    public static final String OBJECT_SEPARATOR = "object_separator";
    public static final String GLOBAL_OBJECT_ID = "global_object_id";
    public static final String USER_TYPE = "user_type";

    public static final String DEFAULT_OBJECT_SEPARATOR = "/";
    public static final String DEFAULT_GLOBAL_OBJECT_ID = "doris";
    public static final String DEFAULT_USER_TYPE = "user";
    public static final long DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000L;
    public static final long DEFAULT_READ_TIMEOUT_MILLIS = 5000L;

    private final String apiUrl;
    private final String storeId;
    private final String modelId;
    private final String token;
    private final long connectTimeoutMillis;
    private final long readTimeoutMillis;
    private final String objectSeparator;
    private final String globalObjectId;
    private final String userType;

    private OpenFgaConfig(String apiUrl, String storeId, String modelId, String token, long connectTimeoutMillis,
            long readTimeoutMillis, String objectSeparator, String globalObjectId, String userType) {
        this.apiUrl = apiUrl;
        this.storeId = storeId;
        this.modelId = modelId;
        this.token = token;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.objectSeparator = objectSeparator;
        this.globalObjectId = globalObjectId;
        this.userType = userType;
    }

    public static OpenFgaConfig fromProps(Map<String, String> prop) {
        if (prop == null) {
            throw new IllegalArgumentException("openfga access controller properties must not be null");
        }
        String apiUrl = prop.get(API_URL);
        if (StringUtils.isEmpty(apiUrl)) {
            throw new IllegalArgumentException("openfga access controller requires '" + API_URL + "'");
        }
        String storeId = prop.get(STORE_ID);
        if (StringUtils.isEmpty(storeId)) {
            throw new IllegalArgumentException("openfga access controller requires '" + STORE_ID + "'");
        }
        String modelId = prop.get(MODEL_ID);
        String token = prop.get(API_TOKEN);
        if (StringUtils.isEmpty(token)) {
            token = prop.get(TOKEN);
        }
        long connectTimeout = parseLong(prop.get(CONNECT_TIMEOUT_MILLIS), DEFAULT_CONNECT_TIMEOUT_MILLIS,
                CONNECT_TIMEOUT_MILLIS);
        long readTimeout = parseLong(prop.get(READ_TIMEOUT_MILLIS), DEFAULT_READ_TIMEOUT_MILLIS, READ_TIMEOUT_MILLIS);
        String separator = prop.getOrDefault(OBJECT_SEPARATOR, DEFAULT_OBJECT_SEPARATOR);
        String globalObjectId = prop.getOrDefault(GLOBAL_OBJECT_ID, DEFAULT_GLOBAL_OBJECT_ID);
        String userType = prop.getOrDefault(USER_TYPE, DEFAULT_USER_TYPE);
        return new OpenFgaConfig(apiUrl, storeId, modelId, token, connectTimeout, readTimeout, separator,
                globalObjectId, userType);
    }

    private static long parseLong(String value, long defaultValue, String key) {
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("openfga access controller property '" + key
                    + "' must be a number, got: " + value);
        }
    }

    public String getApiUrl() {
        return apiUrl;
    }

    public String getStoreId() {
        return storeId;
    }

    public String getModelId() {
        return modelId;
    }

    public String getToken() {
        return token;
    }

    public long getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public long getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public String getObjectSeparator() {
        return objectSeparator;
    }

    public String getGlobalObjectId() {
        return globalObjectId;
    }

    public String getUserType() {
        return userType;
    }
}
