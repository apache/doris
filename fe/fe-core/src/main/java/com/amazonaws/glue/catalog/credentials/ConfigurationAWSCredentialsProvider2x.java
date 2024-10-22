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

package com.amazonaws.glue.catalog.credentials;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

/**
 * This is credentials provider for AWS SDK 2.x, comparing to ConfigurationAWSCredentialsProvider,
 * which is for AWS SDK 1.x.
 * See: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/migration-client-credentials.html
 */
public class ConfigurationAWSCredentialsProvider2x implements AwsCredentialsProvider {

    private AwsBasicCredentials awsBasicCredentials;

    private ConfigurationAWSCredentialsProvider2x(AwsBasicCredentials awsBasicCredentials) {
        this.awsBasicCredentials = awsBasicCredentials;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return awsBasicCredentials;
    }

    public static AwsCredentialsProvider create(Map<String, String> config) {
        String ak = config.get("glue.access_key");
        String sk = config.get("glue.secret_key");
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(ak, sk);
        return new ConfigurationAWSCredentialsProvider2x(awsBasicCredentials);
    }
}
