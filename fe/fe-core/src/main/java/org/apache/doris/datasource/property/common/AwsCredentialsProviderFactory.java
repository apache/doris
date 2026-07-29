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
//
// Copied from
// https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/branch-3.4.0/
//

package org.apache.doris.datasource.property.common;


import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

import java.util.ArrayList;
import java.util.List;

public final class AwsCredentialsProviderFactory {

    private AwsCredentialsProviderFactory() {
    }

    /* =========================
     * AWS SDK V2
     * ========================= */

    private static boolean isWebIdentityConfigured() {
        return System.getenv("AWS_ROLE_ARN") != null
                && System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != null;
    }

    private static boolean isContainerCredentialsConfigured() {
        return System.getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI") != null
                || System.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") != null;
    }

    public static String getV2ClassName(AwsCredentialsProviderMode mode, boolean includeAnonymousInDefault) {
        switch (mode) {
            case ENV:
                return EnvironmentVariableCredentialsProvider.class.getName();
            case SYSTEM_PROPERTIES:
                return SystemPropertyCredentialsProvider.class.getName();
            case WEB_IDENTITY:
                return WebIdentityTokenFileCredentialsProvider.class.getName();
            case CONTAINER:
                return ContainerCredentialsProvider.class.getName();
            case INSTANCE_PROFILE:
                return InstanceProfileCredentialsProvider.class.getName();
            case ANONYMOUS:
                return AnonymousCredentialsProvider.class.getName();
            case DEFAULT:
                List<String> providers = new ArrayList<>();
                providers.add(EnvironmentVariableCredentialsProvider.class.getName());
                providers.add(SystemPropertyCredentialsProvider.class.getName());
                providers.add(InstanceProfileCredentialsProvider.class.getName());
                if (isWebIdentityConfigured()) {
                    providers.add(WebIdentityTokenFileCredentialsProvider.class.getName());
                }
                if (isContainerCredentialsConfigured()) {
                    providers.add(ContainerCredentialsProvider.class.getName());
                }
                if (includeAnonymousInDefault) {
                    providers.add(AnonymousCredentialsProvider.class.getName());
                }
                return String.join(",", providers);
            default:
                throw new UnsupportedOperationException(
                        "AWS SDK V2 does not support credentials provider mode: " + mode);
        }
    }

}
