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

package org.apache.doris.datasource.iceberg.s3tables;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.Map;

/**
 * AWS credentials provider for S3 Tables catalog.
 * Supports explicit credentials (access key + secret key + optional session token)
 * with fallback to AWS default credentials chain (environment variables, instance profile,
 * web identity token, container credentials, etc.) when explicit credentials are not provided.
 */
public class CustomAwsCredentialsProvider implements AwsCredentialsProvider {
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final AwsCredentialsProvider fallbackProvider;

    public CustomAwsCredentialsProvider(String accessKeyId, String secretAccessKey, String sessionToken) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.fallbackProvider = hasExplicitCredentials() ? null : DefaultCredentialsProvider.create();
    }

    private boolean hasExplicitCredentials() {
        return accessKeyId != null && !accessKeyId.isEmpty()
                && secretAccessKey != null && !secretAccessKey.isEmpty();
    }

    private boolean hasSessionToken() {
        return sessionToken != null && !sessionToken.isEmpty();
    }

    @Override
    public AwsCredentials resolveCredentials() {
        if (hasExplicitCredentials()) {
            if (hasSessionToken()) {
                return AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
            }
            return AwsBasicCredentials.create(accessKeyId, secretAccessKey);
        }
        return fallbackProvider.resolveCredentials();
    }

    public static CustomAwsCredentialsProvider create(Map<String, String> props) {
        return new CustomAwsCredentialsProvider(
                props.get("s3.access-key-id"),
                props.get("s3.secret-access-key"),
                props.get("s3.session-token"));
    }
}
