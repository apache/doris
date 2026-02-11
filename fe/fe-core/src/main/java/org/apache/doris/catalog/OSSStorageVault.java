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

import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Storage vault for Alibaba Cloud OSS (Object Storage Service).
 * Supports both ECS instance profile credentials and explicit access key credentials.
 */
public class OSSStorageVault extends StorageVault {

    private Resource resource;

    public static final String OSS_ROOT_PATH = "oss.root.path";

    /**
     * Property keys for OSS storage vault configuration.
     * These map to OSSProperties constants for consistency.
     */
    public static class PropertyKey {
        // Basic OSS configuration
        public static final String ENDPOINT = "oss.endpoint";
        public static final String REGION = "oss.region";
        public static final String BUCKET = "bucket";
        public static final String ROOT_PATH = OSS_ROOT_PATH;

        // Authentication - optional for ECS instance profile
        public static final String ACCESS_KEY = "oss.access_key";
        public static final String SECRET_KEY = "oss.secret_key";
        public static final String SESSION_TOKEN = "oss.session_token";

        // Connection configuration
        public static final String MAX_CONNECTIONS = "oss.connection.maximum";
        public static final String REQUEST_TIMEOUT_MS = "oss.connection.request.timeout";
        public static final String CONNECTION_TIMEOUT_MS = "oss.connection.timeout";

        // Provider type
        public static final String PROVIDER = StorageProperties.FS_PROVIDER_KEY;
    }

    public OSSStorageVault(String name, boolean ifNotExists,
            boolean setAsDefault, CreateResourceStmt stmt) throws DdlException {
        super(name, StorageVault.StorageVaultType.OSS, ifNotExists, setAsDefault);
        resource = Resource.fromStmt(stmt);
    }

    @Override
    public void modifyProperties(ImmutableMap<String, String> properties) throws DdlException {
        resource.setProperties(properties);
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return resource.getCopiedProperties();
    }

    @Override
    public void checkCreationProperties(Map<String, String> properties) throws UserException {
        super.checkCreationProperties(properties);

        // Endpoint is required
        if (!properties.containsKey(PropertyKey.ENDPOINT)
                || StringUtils.isBlank(properties.get(PropertyKey.ENDPOINT))) {
            throw new DdlException("OSS endpoint is required. Please set " + PropertyKey.ENDPOINT);
        }

        // Bucket is required
        if (!properties.containsKey(PropertyKey.BUCKET)
                || StringUtils.isBlank(properties.get(PropertyKey.BUCKET))) {
            throw new DdlException("OSS bucket is required. Please set " + PropertyKey.BUCKET);
        }

        // Root path is required
        if (!properties.containsKey(PropertyKey.ROOT_PATH)
                || StringUtils.isBlank(properties.get(PropertyKey.ROOT_PATH))) {
            throw new DdlException("OSS root path is required. Please set " + PropertyKey.ROOT_PATH);
        }

        // Access key and secret key are optional (can use ECS instance profile)
        // If one is provided, both must be provided
        boolean hasAccessKey = properties.containsKey(PropertyKey.ACCESS_KEY)
                && StringUtils.isNotBlank(properties.get(PropertyKey.ACCESS_KEY));
        boolean hasSecretKey = properties.containsKey(PropertyKey.SECRET_KEY)
                && StringUtils.isNotBlank(properties.get(PropertyKey.SECRET_KEY));

        if (hasAccessKey != hasSecretKey) {
            throw new DdlException("Both access_key and secret_key must be provided together, "
                    + "or neither (to use ECS instance profile)");
        }

        // Validate timeout values if provided
        validateTimeoutProperty(properties, PropertyKey.REQUEST_TIMEOUT_MS, "request timeout");
        validateTimeoutProperty(properties, PropertyKey.CONNECTION_TIMEOUT_MS, "connection timeout");

        // Validate max connections if provided
        if (properties.containsKey(PropertyKey.MAX_CONNECTIONS)) {
            try {
                int maxConn = Integer.parseInt(properties.get(PropertyKey.MAX_CONNECTIONS));
                if (maxConn <= 0) {
                    throw new DdlException("max_connections must be positive, got: " + maxConn);
                }
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid max_connections value: "
                        + properties.get(PropertyKey.MAX_CONNECTIONS));
            }
        }
    }

    /**
     * Validate timeout property value.
     */
    private void validateTimeoutProperty(Map<String, String> properties,
            String key, String name) throws DdlException {
        if (properties.containsKey(key)) {
            try {
                int timeout = Integer.parseInt(properties.get(key));
                if (timeout <= 0) {
                    throw new DdlException(name + " must be positive, got: " + timeout);
                }
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid " + name + " value: " + properties.get(key));
            }
        }
    }
}
