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

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.CredProviderTypePB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderMode;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Map;

/**
 * fe-core-only cloud meta-service glue: builds {@link Cloud.ObjectStoreInfoPB} from the
 * persisted user-facing {@code s3.*} property map. Behaviourally identical to the legacy
 * {@code S3Properties.getObjStoreInfoPB}; this is a WIRE CONTRACT with the cloud meta-service
 * (master plan §2.7-③), so every builder field is locked by a golden test against the legacy
 * implementation until Phase D deletes it.
 *
 * <p>IMPORTANT: this reads the USER key namespace ({@code s3.endpoint}, {@code s3.access_key},
 * ...) — do NOT feed it a backend {@code AWS_*} map.</p>
 */
public final class CloudObjectStoreAdapter {

    private static final String ENDPOINT = S3CompatibleFileSystemProperties.PROP_ENDPOINT;
    private static final String REGION = S3CompatibleFileSystemProperties.PROP_REGION;
    private static final String ACCESS_KEY = S3CompatibleFileSystemProperties.PROP_ACCESS_KEY;
    private static final String SECRET_KEY = S3CompatibleFileSystemProperties.PROP_SECRET_KEY;
    private static final String ROOT_PATH = S3CompatibleFileSystemProperties.PROP_ROOT_PATH;
    private static final String BUCKET = S3CompatibleFileSystemProperties.PROP_BUCKET;
    private static final String EXTERNAL_ENDPOINT = S3CompatibleFileSystemProperties.PROP_EXTERNAL_ENDPOINT;
    private static final String ROLE_ARN = S3CompatibleFileSystemProperties.PROP_ROLE_ARN;
    private static final String EXTERNAL_ID = S3CompatibleFileSystemProperties.PROP_EXTERNAL_ID;
    private static final String CREDENTIALS_PROVIDER_TYPE =
            S3CompatibleFileSystemProperties.PROP_CREDENTIALS_PROVIDER_TYPE;
    private static final String ENV_CREDENTIALS_PROVIDER_TYPE = "AWS_CREDENTIALS_PROVIDER_TYPE";
    private static final String USE_PATH_STYLE = S3CompatibleFileSystemProperties.PROP_USE_PATH_STYLE;
    private static final String FS_PROVIDER_KEY = S3CompatibleFileSystemProperties.PROP_PROVIDER;

    private CloudObjectStoreAdapter() {
    }

    /** Direct move of legacy {@code S3Properties.getObjStoreInfoPB}. */
    public static Cloud.ObjectStoreInfoPB.Builder getObjStoreInfoPB(Map<String, String> properties) {
        Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder();
        if (properties.containsKey(ENDPOINT)) {
            builder.setEndpoint(properties.get(ENDPOINT));
        }
        if (properties.containsKey(REGION)) {
            builder.setRegion(properties.get(REGION));
        }
        if (properties.containsKey(ACCESS_KEY)) {
            builder.setAk(properties.get(ACCESS_KEY));
        }
        if (properties.containsKey(SECRET_KEY)) {
            builder.setSk(properties.get(SECRET_KEY));
        }
        if (properties.containsKey(ROOT_PATH)) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(ROOT_PATH)),
                    "%s cannot be empty", ROOT_PATH);
            builder.setPrefix(properties.get(ROOT_PATH));
        }
        if (properties.containsKey(BUCKET)) {
            builder.setBucket(properties.get(BUCKET));
        }
        if (properties.containsKey(EXTERNAL_ENDPOINT)) {
            builder.setExternalEndpoint(properties.get(EXTERNAL_ENDPOINT));
        }
        if (properties.containsKey(FS_PROVIDER_KEY)) {
            // S3 Provider properties should be case insensitive.
            builder.setProvider(Provider.valueOf(properties.get(FS_PROVIDER_KEY).toUpperCase()));
        }

        if (properties.containsKey(USE_PATH_STYLE)) {
            String value = properties.get(USE_PATH_STYLE);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "use_path_style cannot be empty");
            Preconditions.checkArgument(value.equalsIgnoreCase("true")
                            || value.equalsIgnoreCase("false"),
                    "Invalid use_path_style value: %s only 'true' or 'false' is acceptable", value);
            builder.setUsePathStyle(value.equalsIgnoreCase("true"));
        }

        if (hasCredentialsProviderType(properties)) {
            builder.setCredProviderType(getCredProviderTypePB(properties));
        }

        if (properties.containsKey(ROLE_ARN)) {
            String roleArn = properties.get(ROLE_ARN);
            if (!Strings.isNullOrEmpty(roleArn)) {
                builder.setRoleArn(roleArn);
            }
            if (!Strings.isNullOrEmpty(roleArn) && properties.containsKey(EXTERNAL_ID)) {
                builder.setExternalId(properties.get(EXTERNAL_ID));
            }
            if (!Strings.isNullOrEmpty(roleArn) && !builder.hasCredProviderType()) {
                builder.setCredProviderType(getCredProviderTypePB(properties));
            }
        }

        return builder;
    }

    private static boolean hasCredentialsProviderType(Map<String, String> properties) {
        return properties.containsKey(CREDENTIALS_PROVIDER_TYPE)
                || properties.containsKey(ENV_CREDENTIALS_PROVIDER_TYPE);
    }

    private static CredProviderTypePB getCredProviderTypePB(Map<String, String> properties) {
        AwsCredentialsProviderMode mode = S3ThriftAdapter.getCredentialsProviderMode(properties,
                AwsCredentialsProviderMode.INSTANCE_PROFILE);
        switch (mode) {
            case DEFAULT:
                return CredProviderTypePB.DEFAULT;
            case ENV:
                return CredProviderTypePB.ENV;
            case SYSTEM_PROPERTIES:
                return CredProviderTypePB.SYSTEM_PROPERTIES;
            case WEB_IDENTITY:
                return CredProviderTypePB.WEB_IDENTITY;
            case CONTAINER:
                return CredProviderTypePB.CONTAINER;
            case INSTANCE_PROFILE:
                return CredProviderTypePB.INSTANCE_PROFILE;
            case ANONYMOUS:
                return CredProviderTypePB.ANONYMOUS;
            default:
                throw new IllegalArgumentException("Unsupported AWS credentials provider mode: " + mode);
        }
    }
}
