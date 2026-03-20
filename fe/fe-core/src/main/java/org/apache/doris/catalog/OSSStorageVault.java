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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

/**
 * Storage vault for Alibaba Cloud OSS (Object Storage Service).
 * Supports both ECS instance profile credentials and explicit access key credentials.
 */
public class OSSStorageVault extends StorageVault {

    private Resource resource;

    public static final String OSS_ROOT_PATH = "oss.root.path";

    /**
     * Properties that can be altered for an existing OSS storage vault.
     * Includes OSS-specific properties but excludes immutable properties like bucket, endpoint, and type.
     * Note: TYPE is excluded because changing vault type (OSS->S3, etc.) is not supported.
     */
    public static final HashSet<String> ALLOW_ALTER_PROPERTIES = new HashSet<>(Arrays.asList(
            StorageVault.PropertyKey.VAULT_NAME,
            PropertyKey.ACCESS_KEY,
            PropertyKey.SECRET_KEY,
            PropertyKey.SESSION_TOKEN,
            PropertyKey.ROLE_ARN,
            PropertyKey.EXTERNAL_ID,
            PropertyKey.MAX_CONNECTIONS,
            PropertyKey.REQUEST_TIMEOUT_MS,
            PropertyKey.CONNECTION_TIMEOUT_MS
    ));

    /**
     * Property keys for OSS storage vault configuration.
     * These map to OSSProperties constants for consistency.
     */
    public static class PropertyKey {
        // Basic OSS configuration
        public static final String ENDPOINT = "oss.endpoint";
        public static final String REGION = "oss.region";
        public static final String BUCKET = "oss.bucket";
        public static final String ROOT_PATH = OSS_ROOT_PATH;

        // Authentication - optional for ECS instance profile
        public static final String ACCESS_KEY = "oss.access_key";
        public static final String SECRET_KEY = "oss.secret_key";
        public static final String SESSION_TOKEN = "oss.session_token";

        // AssumeRole configuration
        public static final String ROLE_ARN = "oss.role_arn";
        public static final String EXTERNAL_ID = "oss.external_id";

        // Connection configuration
        public static final String MAX_CONNECTIONS = "oss.connection.maximum";
        public static final String REQUEST_TIMEOUT_MS = "oss.connection.request.timeout";
        public static final String CONNECTION_TIMEOUT_MS = "oss.connection.timeout";

        // Provider type
        public static final String PROVIDER = StorageProperties.FS_PROVIDER_KEY;
    }

    public OSSStorageVault(String name, boolean ifNotExists,
            boolean setAsDefault, CreateResourceCommand command) throws DdlException {
        super(name, StorageVault.StorageVaultType.OSS, ifNotExists, setAsDefault);
        resource = Resource.fromCommand(command);
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

        // Region validation - warn if not provided as it may be required for some operations
        String endpoint = properties.get(PropertyKey.ENDPOINT);
        if (!properties.containsKey(PropertyKey.REGION)
                || StringUtils.isBlank(properties.get(PropertyKey.REGION))) {
            // Check if region can be inferred from endpoint
            if (!canInferRegionFromEndpoint(endpoint)) {
                throw new DdlException("OSS region cannot be inferred from endpoint '" + endpoint + "'. "
                        + "Please explicitly set " + PropertyKey.REGION + ". "
                        + "Example: 'oss.region' = 'cn-hangzhou'");
            }
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

        // Validate root path format - must not start with '/' to avoid invalid OSS keys
        String rootPath = properties.get(PropertyKey.ROOT_PATH);
        if (rootPath.startsWith("/")) {
            throw new DdlException("OSS root path must not start with '/'. "
                    + "Use relative path like 'doris/data' instead of '/doris/data'. Got: " + rootPath);
        }
        if (rootPath.endsWith("/")) {
            throw new DdlException("OSS root path must not end with '/'. "
                    + "Use 'doris/data' instead of 'doris/data/'. Got: " + rootPath);
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
     * Validate timeout property value (in milliseconds).
     * Ensures the timeout is a positive integer value in milliseconds.
     */
    private void validateTimeoutProperty(Map<String, String> properties,
            String key, String name) throws DdlException {
        if (properties.containsKey(key)) {
            try {
                int timeout = Integer.parseInt(properties.get(key));
                if (timeout <= 0) {
                    throw new DdlException(name + " must be positive (in milliseconds), got: " + timeout);
                }
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid " + name + " value (must be milliseconds as integer): "
                        + properties.get(key));
            }
        }
    }

    /**
     * Check if region can be inferred from the OSS endpoint.
     * Supports both native OSS and S3-compatible endpoint formats.
     * Examples:
     *   - oss-cn-hangzhou.aliyuncs.com → can infer region (native OSS)
     *   - oss-cn-beijing-internal.aliyuncs.com → can infer region (native OSS VPC)
     *   - s3.cn-shanghai.aliyuncs.com → can infer region (S3-compatible)
     *   - custom.example.com → cannot infer region
     */
    private boolean canInferRegionFromEndpoint(String endpoint) {
        if (StringUtils.isBlank(endpoint)) {
            return false;
        }

        // Remove protocol if present
        String host = endpoint.replaceFirst("^https?://", "");

        // Check for standard Alibaba Cloud OSS endpoint patterns
        // Pattern 1: oss-{region}.aliyuncs.com or oss-{region}-internal.aliyuncs.com (native OSS)
        if (host.matches("^oss-[a-z0-9-]+(?:-internal)?\\.aliyuncs\\.com$")) {
            return true;
        }

        // Pattern 2: s3.{region}.aliyuncs.com (S3-compatible endpoint)
        if (host.matches("^s3\\.([a-z0-9-]+)\\.aliyuncs\\.com$")) {
            return true;
        }

        // Custom endpoints cannot have region inferred
        return false;
    }
}
