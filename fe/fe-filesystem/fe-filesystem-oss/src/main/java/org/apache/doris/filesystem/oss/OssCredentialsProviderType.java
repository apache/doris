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

package org.apache.doris.filesystem.oss;

import org.apache.commons.lang3.StringUtils;

/**
 * Credential provider mode for OSS filesystem access.
 *
 * <p>Mirrors {@code S3CredentialsProviderType} for consistency across providers.
 */
public enum OssCredentialsProviderType {

    /**
     * Try in order: INSTANCE_PROFILE → OIDC (if env configured) → ENV → static ak/sk → anonymous.
     */
    DEFAULT("DEFAULT"),

    /**
     * ECS instance metadata (100.100.100.200). Role name from {@code oss.ecs_ram_role_name}
     * or auto-discovered from the metadata base URL when not set.
     */
    INSTANCE_PROFILE("INSTANCE_PROFILE"),

    /**
     * RRSA / Kubernetes pod identity via STS AssumeRoleWithOIDC.
     * Role ARN and token file from {@code oss.oidc_provider_arn}/{@code oss.oidc_token_file}
     * or from env vars {@code ALIBABA_CLOUD_ROLE_ARN} / {@code ALIBABA_CLOUD_OIDC_TOKEN_FILE}
     * / {@code ALIBABA_CLOUD_OIDC_PROVIDER_ARN}.
     */
    OIDC("OIDC"),

    /**
     * Environment variables: {@code OSS_ACCESS_KEY_ID} and {@code OSS_ACCESS_KEY_SECRET}.
     */
    ENV("ENV"),

    /** No credentials — public buckets only. */
    ANONYMOUS("ANONYMOUS");

    private final String mode;

    OssCredentialsProviderType(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static OssCredentialsProviderType fromString(String value) {
        if (StringUtils.isBlank(value)) {
            return DEFAULT;
        }
        String normalized = value.trim().toUpperCase().replace('-', '_');
        switch (normalized) {
            case "INSTANCE_PROFILE":
            case "ECS":
            case "ECS_RAM_ROLE":
                return INSTANCE_PROFILE;
            case "OIDC":
            case "RRSA":
            case "WEB_IDENTITY":
                return OIDC;
            case "ENV":
            case "ENVIRONMENT":
                return ENV;
            case "ANONYMOUS":
                return ANONYMOUS;
            case "DEFAULT":
                return DEFAULT;
            default:
                throw new IllegalArgumentException(
                        "Unsupported oss.credentials_provider: " + value);
        }
    }
}
