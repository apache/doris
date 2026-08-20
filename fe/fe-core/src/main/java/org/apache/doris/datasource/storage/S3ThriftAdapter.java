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

import org.apache.doris.datasource.property.common.AwsCredentialsProviderMode;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.thrift.TCredProviderType;
import org.apache.doris.thrift.TS3StorageParam;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * fe-core-only Thrift glue: builds {@link TS3StorageParam} from the persisted user-facing
 * {@code s3.*} property map. Behaviourally identical to the legacy
 * {@code S3Properties.getS3TStorageParam} (direct code move; the legacy class stays as the
 * golden-test oracle until Phase D deletes it).
 *
 * <p>IMPORTANT: this reads the USER key namespace ({@code s3.endpoint}, {@code s3.access_key},
 * ...) — do NOT feed it a backend {@code AWS_*} map.</p>
 */
public final class S3ThriftAdapter {

    private static final String ENDPOINT = S3CompatibleFileSystemProperties.PROP_ENDPOINT;
    private static final String REGION = S3CompatibleFileSystemProperties.PROP_REGION;
    private static final String ACCESS_KEY = S3CompatibleFileSystemProperties.PROP_ACCESS_KEY;
    private static final String SECRET_KEY = S3CompatibleFileSystemProperties.PROP_SECRET_KEY;
    private static final String SESSION_TOKEN = S3CompatibleFileSystemProperties.PROP_SESSION_TOKEN;
    private static final String ROOT_PATH = S3CompatibleFileSystemProperties.PROP_ROOT_PATH;
    private static final String BUCKET = S3CompatibleFileSystemProperties.PROP_BUCKET;
    private static final String ROLE_ARN = S3CompatibleFileSystemProperties.PROP_ROLE_ARN;
    private static final String EXTERNAL_ID = S3CompatibleFileSystemProperties.PROP_EXTERNAL_ID;
    private static final String CREDENTIALS_PROVIDER_TYPE =
            S3CompatibleFileSystemProperties.PROP_CREDENTIALS_PROVIDER_TYPE;
    private static final String ENV_CREDENTIALS_PROVIDER_TYPE = "AWS_CREDENTIALS_PROVIDER_TYPE";
    private static final String MAX_CONNECTIONS = S3CompatibleFileSystemProperties.PROP_MAX_CONNECTIONS;
    private static final String REQUEST_TIMEOUT_MS = S3CompatibleFileSystemProperties.PROP_REQUEST_TIMEOUT_MS;
    private static final String CONNECTION_TIMEOUT_MS = S3CompatibleFileSystemProperties.PROP_CONNECTION_TIMEOUT_MS;
    private static final String USE_PATH_STYLE = S3CompatibleFileSystemProperties.PROP_USE_PATH_STYLE;

    private static final String DEFAULT_MAX_CONNECTIONS =
            S3CompatibleFileSystemProperties.DEFAULT_MAX_CONNECTIONS_VALUE;
    private static final String DEFAULT_REQUEST_TIMEOUT_MS =
            S3CompatibleFileSystemProperties.DEFAULT_REQUEST_TIMEOUT_MS_VALUE;
    private static final String DEFAULT_CONNECTION_TIMEOUT_MS =
            S3CompatibleFileSystemProperties.DEFAULT_CONNECTION_TIMEOUT_MS_VALUE;

    private S3ThriftAdapter() {
    }

    /** Direct move of legacy {@code S3Properties.getS3TStorageParam}. */
    public static TS3StorageParam getS3TStorageParam(Map<String, String> properties) {
        TS3StorageParam s3Info = new TS3StorageParam();

        if (properties.containsKey(ROLE_ARN)) {
            s3Info.setRoleArn(properties.get(ROLE_ARN));
            if (properties.containsKey(EXTERNAL_ID)) {
                s3Info.setExternalId(properties.get(EXTERNAL_ID));
            }
            s3Info.setCredProviderType(getTCredProviderType(properties));
        }

        s3Info.setEndpoint(properties.get(ENDPOINT));
        s3Info.setRegion(properties.get(REGION));
        s3Info.setAk(properties.get(ACCESS_KEY));
        s3Info.setSk(properties.get(SECRET_KEY));
        s3Info.setToken(properties.get(SESSION_TOKEN));

        s3Info.setRootPath(properties.get(ROOT_PATH));
        s3Info.setBucket(properties.get(BUCKET));
        String maxConnections = properties.get(MAX_CONNECTIONS);
        s3Info.setMaxConn(Integer.parseInt(maxConnections == null
                ? DEFAULT_MAX_CONNECTIONS : maxConnections));
        String requestTimeoutMs = properties.get(REQUEST_TIMEOUT_MS);
        s3Info.setRequestTimeoutMs(Integer.parseInt(requestTimeoutMs == null
                ? DEFAULT_REQUEST_TIMEOUT_MS : requestTimeoutMs));
        String connTimeoutMs = properties.get(CONNECTION_TIMEOUT_MS);
        s3Info.setConnTimeoutMs(Integer.parseInt(connTimeoutMs == null
                ? DEFAULT_CONNECTION_TIMEOUT_MS : connTimeoutMs));
        String usePathStyle = properties.getOrDefault(USE_PATH_STYLE, "false");
        s3Info.setUsePathStyle(Boolean.parseBoolean(usePathStyle));
        return s3Info;
    }

    /**
     * Legacy S3Properties.getCredentialsProviderMode: the user key wins over the legacy
     * {@code AWS_CREDENTIALS_PROVIDER_TYPE} env-style key; INSTANCE_PROFILE is the default in
     * the thrift/PB context (assume-role paths).
     */
    static AwsCredentialsProviderMode getCredentialsProviderMode(Map<String, String> properties,
            AwsCredentialsProviderMode defaultMode) {
        String mode = properties.get(CREDENTIALS_PROVIDER_TYPE);
        if (StringUtils.isBlank(mode)) {
            mode = properties.get(ENV_CREDENTIALS_PROVIDER_TYPE);
        }
        if (StringUtils.isBlank(mode)) {
            return defaultMode;
        }
        return AwsCredentialsProviderMode.fromString(mode);
    }

    private static TCredProviderType getTCredProviderType(Map<String, String> properties) {
        AwsCredentialsProviderMode mode = getCredentialsProviderMode(properties,
                AwsCredentialsProviderMode.INSTANCE_PROFILE);
        switch (mode) {
            case DEFAULT:
                return TCredProviderType.DEFAULT;
            case ENV:
                return TCredProviderType.ENV;
            case SYSTEM_PROPERTIES:
                return TCredProviderType.SYSTEM_PROPERTIES;
            case WEB_IDENTITY:
                return TCredProviderType.WEB_IDENTITY;
            case CONTAINER:
                return TCredProviderType.CONTAINER;
            case INSTANCE_PROFILE:
                return TCredProviderType.INSTANCE_PROFILE;
            case ANONYMOUS:
                return TCredProviderType.ANONYMOUS;
            default:
                throw new IllegalArgumentException("Unsupported AWS credentials provider mode: " + mode);
        }
    }
}
