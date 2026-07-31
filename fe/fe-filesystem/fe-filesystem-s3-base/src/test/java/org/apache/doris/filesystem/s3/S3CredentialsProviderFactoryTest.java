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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class S3CredentialsProviderFactoryTest {

    @Test
    void createClientProvider_staticCredentialsOverrideProviderType() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        raw.put("s3.access_key", "ak");
        raw.put("s3.secret_key", "sk");
        raw.put("s3.credentials_provider_type", "ENV");
        S3FileSystemProperties properties = S3FileSystemProperties.of(raw);

        AwsCredentialsProvider provider = S3CredentialsProviderFactory.createClientProvider(properties);

        Assertions.assertInstanceOf(StaticCredentialsProvider.class, provider);
        Assertions.assertEquals("ak", provider.resolveCredentials().accessKeyId());
    }

    @Test
    void createClientProvider_usesConfiguredProviderTypeWithoutStaticCredentials() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        raw.put("s3.credentials_provider_type", "ENV");
        S3FileSystemProperties properties = S3FileSystemProperties.of(raw);

        AwsCredentialsProvider provider = S3CredentialsProviderFactory.createClientProvider(properties);

        Assertions.assertInstanceOf(EnvironmentVariableCredentialsProvider.class, provider);
    }

    @Test
    void createStsSourceProvider_usesConfiguredProviderTypeWithoutStaticCredentials() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        raw.put("s3.role_arn", "arn:aws:iam::123456789012:role/MyRole");
        raw.put("s3.credentials_provider_type", "ENV");
        S3FileSystemProperties properties = S3FileSystemProperties.of(raw);

        AwsCredentialsProvider provider = S3CredentialsProviderFactory.createStsSourceProvider(properties);

        Assertions.assertInstanceOf(EnvironmentVariableCredentialsProvider.class, provider);
    }

    @Test
    void createDefaultProviderChainIncludesProfileCredentialsProvider() {
        AwsCredentialsProvider provider = S3CredentialsProviderFactory.create(
                S3CredentialsProviderType.DEFAULT, true);

        Assertions.assertInstanceOf(AwsCredentialsProviderChain.class, provider);
        Assertions.assertTrue(providerClasses((AwsCredentialsProviderChain) provider)
                .contains(ProfileCredentialsProvider.class));
    }

    @Test
    void hadoopClassNameDefaultIncludesProfileCredentialsProvider() {
        String className = S3CredentialsProviderFactory.hadoopClassName(
                S3CredentialsProviderType.DEFAULT, true);

        Assertions.assertTrue(className.contains(ProfileCredentialsProvider.class.getName()));
    }

    private static List<Class<?>> providerClasses(AwsCredentialsProviderChain provider) {
        try {
            Field field = AwsCredentialsProviderChain.class.getDeclaredField("credentialsProviders");
            field.setAccessible(true);
            List<?> providers = (List<?>) field.get(provider);
            return providers.stream().map(Object::getClass).collect(java.util.stream.Collectors.toList());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
