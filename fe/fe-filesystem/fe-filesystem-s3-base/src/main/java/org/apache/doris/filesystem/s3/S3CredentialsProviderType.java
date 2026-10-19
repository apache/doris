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

package org.apache.doris.filesystem.s3;

import org.apache.commons.lang3.StringUtils;

/**
 * AWS SDK v2 credentials provider mode for S3 filesystem access.
 */
public enum S3CredentialsProviderType {
    DEFAULT("DEFAULT"),
    ENV("ENV"),
    SYSTEM_PROPERTIES("SYSTEM_PROPERTIES"),
    WEB_IDENTITY("WEB_IDENTITY"),
    CONTAINER("CONTAINER"),
    INSTANCE_PROFILE("INSTANCE_PROFILE"),
    ANONYMOUS("ANONYMOUS");

    private final String mode;

    S3CredentialsProviderType(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static S3CredentialsProviderType fromString(String value) {
        if (StringUtils.isBlank(value)) {
            return DEFAULT;
        }
        String normalized = value.trim().toUpperCase().replace('-', '_');
        switch (normalized) {
            case "ENV":
            case "ENVIRONMENT":
                return ENV;
            case "SYSTEM_PROPERTIES":
                return SYSTEM_PROPERTIES;
            case "WEB_IDENTITY":
            case "WEB_IDENTITY_TOKEN_FILE":
                return WEB_IDENTITY;
            case "CONTAINER":
                return CONTAINER;
            case "INSTANCE_PROFILE":
                return INSTANCE_PROFILE;
            case "ANONYMOUS":
                return ANONYMOUS;
            case "DEFAULT":
                return DEFAULT;
            default:
                throw new IllegalArgumentException("Unsupported s3.credentials_provider_type: " + value);
        }
    }
}
