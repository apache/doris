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
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Map;
import java.util.Objects;

public class IcebergAwsAssumeRoleCredentialsProvider implements AwsCredentialsProvider {

    private static final String DEFAULT_ROLE_SESSION_NAME = "aws-sdk-java-v2-fe";

    private final StsAssumeRoleCredentialsProvider provider;

    private IcebergAwsAssumeRoleCredentialsProvider(StsAssumeRoleCredentialsProvider provider) {
        this.provider = provider;
    }

    public static IcebergAwsAssumeRoleCredentialsProvider create(Map<String, String> props) {
        AwsProperties awsProperties = new AwsProperties(props);
        AwsClientProperties awsClientProperties = new AwsClientProperties(props);
        String roleArn = Objects.requireNonNull(awsProperties.clientAssumeRoleArn(),
                AwsProperties.CLIENT_ASSUME_ROLE_ARN + " is required");
        String region = Objects.requireNonNull(awsProperties.clientAssumeRoleRegion(),
                AwsProperties.CLIENT_ASSUME_ROLE_REGION + " is required");
        String externalId = awsProperties.clientAssumeRoleExternalId();
        String roleSessionName = StringUtils.defaultIfBlank(awsProperties.clientAssumeRoleSessionName(),
                DEFAULT_ROLE_SESSION_NAME);

        StsClient stsClient = StsClient.builder()
                .region(Region.of(region))
                .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                .build();
        StsAssumeRoleCredentialsProvider assumeRoleProvider = StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(builder -> {
                    builder.roleArn(roleArn)
                            .roleSessionName(roleSessionName)
                            .durationSeconds(awsProperties.clientAssumeRoleTimeoutSec());
                    if (StringUtils.isNotBlank(externalId)) {
                        builder.externalId(externalId);
                    }
                    if (!awsProperties.stsClientAssumeRoleTags().isEmpty()) {
                        builder.tags(awsProperties.stsClientAssumeRoleTags());
                    }
                })
                .build();
        return new IcebergAwsAssumeRoleCredentialsProvider(assumeRoleProvider);
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return provider.resolveCredentials();
    }
}
