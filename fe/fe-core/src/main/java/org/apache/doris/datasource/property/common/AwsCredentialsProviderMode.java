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

package org.apache.doris.datasource.property.common;

import lombok.Getter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

public enum AwsCredentialsProviderMode {

    DEFAULT("DEFAULT", DefaultDorisAwsCredentialsProviderChain.create(), new com.amazonaws.auth
            .AWSCredentialsProviderChain(
            new com.amazonaws.auth.InstanceProfileCredentialsProvider(), com.amazonaws.auth
            .WebIdentityTokenCredentialsProvider.create(),
            new com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper(),
            new com.amazonaws.auth.EnvironmentVariableCredentialsProvider(), new com.amazonaws.auth
            .SystemPropertiesCredentialsProvider()),
            DefaultDorisAwsCredentialsProviderChain.class.getName()),
    ENV("ENV", EnvironmentVariableCredentialsProvider.create(), new com.amazonaws.auth
            .EnvironmentVariableCredentialsProvider(), EnvironmentVariableCredentialsProvider.class.getName()),
    SYSTEM_PROPERTIES("SYSTEM_PROPERTIES", SystemPropertyCredentialsProvider.create(), new com.amazonaws
            .auth.SystemPropertiesCredentialsProvider(), SystemPropertyCredentialsProvider.class.getName()),
    WEB_IDENTITY("WEB_IDENTITY", WebIdentityTokenFileCredentialsProvider.create(), new com.amazonaws
            .auth.WebIdentityTokenCredentialsProvider(), WebIdentityTokenFileCredentialsProvider.class.getName()),
    CONTAINER("CONTAINER", ContainerCredentialsProvider.create(), new com.amazonaws
            .auth.ContainerCredentialsProvider(), ContainerCredentialsProvider.class.getName()),
    INSTANCE_PROFILE("INSTANCE_PROFILE", InstanceProfileCredentialsProvider.create(),
            new com.amazonaws.auth.InstanceProfileCredentialsProvider(), InstanceProfileCredentialsProvider
            .class.getName());

    private final String mode;

    @Getter
    private final com.amazonaws.auth.AWSCredentialsProvider credentialsProviderV1;
    @Getter
    private final AwsCredentialsProvider credentialsProviderV2;
    @Getter
    private final String className;

    AwsCredentialsProviderMode(String mode, AwsCredentialsProvider credentialsProviderV2,
                               com.amazonaws.auth.AWSCredentialsProvider credentialsProviderV1, String className) {
        this.mode = mode;
        this.credentialsProviderV2 = credentialsProviderV2;
        this.credentialsProviderV1 = credentialsProviderV1;
        this.className = className;
    }

    /**
     * Parse from user-provided string (case-insensitive)
     * Supported examples:
     * "auto", "env", "system-properties", "system_properties",
     * "web_identity", "web-identity", "profile",
     * "container", "instance_profile", "instance-profile"
     */
    public static AwsCredentialsProviderMode fromString(String value) {
        if (value == null || value.isEmpty()) {
            return DEFAULT;
        }

        String normalized = value.trim().toUpperCase().replace('-', '_');

        switch (normalized) {
            case "ENV":
                return ENV;
            case "SYSTEM_PROPERTIES":
                return SYSTEM_PROPERTIES;
            case "WEB_IDENTITY":
                return WEB_IDENTITY;
            case "CONTAINER":
                return CONTAINER;
            case "INSTANCE_PROFILE":
                return INSTANCE_PROFILE;
            case "DEFAULT":
                return DEFAULT;
            default:
                throw new IllegalArgumentException("Unsupported AWS credentials provider: " + value);
        }
    }

}
