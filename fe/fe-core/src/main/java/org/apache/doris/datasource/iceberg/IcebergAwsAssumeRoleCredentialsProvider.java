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

package org.apache.doris.datasource.iceberg;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Map;
import java.util.Objects;

public class IcebergAwsAssumeRoleCredentialsProvider implements AwsCredentialsProvider {

    public static final String ASSUME_ROLE_ARN = "assume-role.arn";
    public static final String ASSUME_ROLE_REGION = "assume-role.region";
    public static final String ASSUME_ROLE_EXTERNAL_ID = "assume-role.external-id";
    public static final String ASSUME_ROLE_SOURCE_CREDENTIALS_PROVIDER =
            "assume-role.source-credentials-provider";

    private static final String DEFAULT_ROLE_SESSION_NAME = "aws-sdk-java-v2-fe";

    private final StsAssumeRoleCredentialsProvider provider;

    private IcebergAwsAssumeRoleCredentialsProvider(StsAssumeRoleCredentialsProvider provider) {
        this.provider = provider;
    }

    public static IcebergAwsAssumeRoleCredentialsProvider create(Map<String, String> props) {
        String roleArn = Objects.requireNonNull(props.get(ASSUME_ROLE_ARN),
                ASSUME_ROLE_ARN + " is required");
        String region = Objects.requireNonNull(props.get(ASSUME_ROLE_REGION),
                ASSUME_ROLE_REGION + " is required");
        String externalId = props.get(ASSUME_ROLE_EXTERNAL_ID);
        String sourceCredentialsProviderClassName = Objects.requireNonNull(
                props.get(ASSUME_ROLE_SOURCE_CREDENTIALS_PROVIDER),
                ASSUME_ROLE_SOURCE_CREDENTIALS_PROVIDER + " is required");

        StsClient stsClient = StsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(createSourceCredentialsProvider(sourceCredentialsProviderClassName))
                .build();
        StsAssumeRoleCredentialsProvider assumeRoleProvider = StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(builder -> {
                    builder.roleArn(roleArn)
                            .roleSessionName(DEFAULT_ROLE_SESSION_NAME);
                    if (StringUtils.isNotBlank(externalId)) {
                        builder.externalId(externalId);
                    }
                })
                .build();
        return new IcebergAwsAssumeRoleCredentialsProvider(assumeRoleProvider);
    }

    private static AwsCredentialsProvider createSourceCredentialsProvider(String className) {
        try {
            Class<?> providerClass = Class.forName(className);
            if (!AwsCredentialsProvider.class.isAssignableFrom(providerClass)) {
                throw new IllegalArgumentException(className + " does not implement "
                        + AwsCredentialsProvider.class.getName());
            }
            return (AwsCredentialsProvider) providerClass.getMethod("create").invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot create AWS credentials provider: " + className, e);
        }
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return provider.resolveCredentials();
    }
}
