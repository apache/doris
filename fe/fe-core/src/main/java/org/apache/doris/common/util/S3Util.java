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

package org.apache.doris.common.util;

import org.apache.doris.common.credentials.CloudCredential;

import com.google.common.base.Strings;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.time.Duration;

public class S3Util {
    private static AwsCredentialsProvider getAwsCredencialsProvider(CloudCredential credential) {
        AwsCredentials awsCredential;
        AwsCredentialsProvider awsCredentialsProvider;
        if (!credential.isTemporary()) {
            awsCredential = AwsBasicCredentials.create(credential.getAccessKey(), credential.getSecretKey());
        } else {
            awsCredential = AwsSessionCredentials.create(credential.getAccessKey(), credential.getSecretKey(),
                        credential.getSessionToken());
        }

        if (!credential.isWhole()) {
            awsCredentialsProvider = AwsCredentialsProviderChain.of(
                    SystemPropertyCredentialsProvider.create(),
                    EnvironmentVariableCredentialsProvider.create(),
                    WebIdentityTokenFileCredentialsProvider.create(),
                    ProfileCredentialsProvider.create(),
                    InstanceProfileCredentialsProvider.create());
        } else {
            awsCredentialsProvider = StaticCredentialsProvider.create(awsCredential);
        }

        return awsCredentialsProvider;
    }

    @Deprecated
    public static S3Client buildS3Client(URI endpoint, String region, CloudCredential credential,
            boolean isUsePathStyle) {
        EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy
                .builder()
                .baseDelay(Duration.ofSeconds(1))
                .maxBackoffTime(Duration.ofMinutes(1))
                .build();
        // retry 3 time with Equal backoff
        RetryPolicy retryPolicy = RetryPolicy
                .builder()
                .numRetries(3)
                .backoffStrategy(backoffStrategy)
                .build();
        ClientOverrideConfiguration clientConf = ClientOverrideConfiguration
                .builder()
                // set retry policy
                .retryPolicy(retryPolicy)
                // using AwsS3V4Signer
                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                .build();
        return S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30)).build())
                .endpointOverride(endpoint)
                .credentialsProvider(getAwsCredencialsProvider(credential))
                .region(Region.of(region))
                .overrideConfiguration(clientConf)
                // disable chunkedEncoding because of bos not supported
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(isUsePathStyle)
                        .build())
                .build();
    }

    /**
     * Using (accessKey, secretKey) or roleArn for creating different credentials provider when creating s3client
     * @param endpoint AWS endpoint (eg: "https://s3.us-east-1.amazonaws.com")
     * @param region AWS region identifier (eg: "us-east-1")
     * @param accessKey AWS access key ID
     * @param secretKey AWS secret access key, paired with accessKey
     * @param sessionToken AWS temporary session token for short-term credentials
     * @param roleArn AWS iam role arn to assume (format: "arn:aws:iam::123456789012:role/role-name")
     * @param externalId  AWS External ID for cross-account role assumption security
     * @return
     */
    private static AwsCredentialsProvider getAwsCredencialsProvider(URI endpoint, String region, String accessKey,
            String secretKey, String sessionToken, String roleArn, String externalId) {

        if (!Strings.isNullOrEmpty(accessKey) && !Strings.isNullOrEmpty(secretKey)) {
            if (Strings.isNullOrEmpty(sessionToken)) {
                return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
            } else {
                return StaticCredentialsProvider.create(AwsSessionCredentials.create(accessKey,
                        secretKey, sessionToken));
            }
        }

        if (!Strings.isNullOrEmpty(roleArn)) {
            StsClient stsClient = StsClient.builder()
                    .credentialsProvider(InstanceProfileCredentialsProvider.create())
                    .build();

            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(builder -> {
                        builder.roleArn(roleArn).roleSessionName("aws-sdk-java-v2-fe");
                        if (!Strings.isNullOrEmpty(externalId)) {
                            builder.externalId(externalId);
                        }
                    }).build();
        }
        return AwsCredentialsProviderChain.of(SystemPropertyCredentialsProvider.create(),
                    EnvironmentVariableCredentialsProvider.create(),
                    WebIdentityTokenFileCredentialsProvider.create(),
                    ProfileCredentialsProvider.create(),
                    InstanceProfileCredentialsProvider.create());
    }

    public static S3Client buildS3Client(URI endpoint, String region, boolean isUsePathStyle,
                                         AwsCredentialsProvider credential) {
        EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy
                .builder()
                .baseDelay(Duration.ofSeconds(1))
                .maxBackoffTime(Duration.ofMinutes(1))
                .build();
        // retry 3 time with Equal backoff
        RetryPolicy retryPolicy = RetryPolicy
                .builder()
                .numRetries(3)
                .backoffStrategy(backoffStrategy)
                .build();
        ClientOverrideConfiguration clientConf = ClientOverrideConfiguration
                .builder()
                // set retry policy
                .retryPolicy(retryPolicy)
                // using AwsS3V4Signer
                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                .build();
        return S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30)).build())
                .endpointOverride(endpoint)
                .credentialsProvider(credential)
                .region(Region.of(region))
                .overrideConfiguration(clientConf)
                // disable chunkedEncoding because of bos not supported
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(isUsePathStyle)
                        .build())
                .build();
    }

    public static S3Client buildS3Client(URI endpoint, String region, boolean isUsePathStyle, String accessKey,
            String secretKey, String sessionToken, String roleArn, String externalId) {
        EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy
                .builder()
                .baseDelay(Duration.ofSeconds(1))
                .maxBackoffTime(Duration.ofMinutes(1))
                .build();
        // retry 3 time with Equal backoff
        RetryPolicy retryPolicy = RetryPolicy
                .builder()
                .numRetries(3)
                .backoffStrategy(backoffStrategy)
                .build();
        ClientOverrideConfiguration clientConf = ClientOverrideConfiguration
                .builder()
                // set retry policy
                .retryPolicy(retryPolicy)
                // using AwsS3V4Signer
                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                .build();
        return S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30)).build())
                .endpointOverride(endpoint)
                .credentialsProvider(getAwsCredencialsProvider(endpoint, region, accessKey, secretKey,
                        sessionToken, roleArn, externalId))
                .region(Region.of(region))
                .overrideConfiguration(clientConf)
                // disable chunkedEncoding because of bos not supported
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(isUsePathStyle)
                        .build())
                .build();
    }

    public static String getLongestPrefix(String globPattern) {
        int length = globPattern.length();
        int earliestSpecialCharIndex = length;

        char[] specialChars = {'*', '?', '[', '{', '\\'};

        for (char specialChar : specialChars) {
            int index = globPattern.indexOf(specialChar);
            if (index != -1 && index < earliestSpecialCharIndex) {
                earliestSpecialCharIndex = index;
            }
        }

        return globPattern.substring(0, earliestSpecialCharIndex);
    }
}
