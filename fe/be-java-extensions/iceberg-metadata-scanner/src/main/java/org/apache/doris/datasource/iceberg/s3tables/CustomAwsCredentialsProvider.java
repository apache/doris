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

import java.util.Map;

// Copy from
// fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/s3tables/CustomAwsCredentialsProvider.java
public class CustomAwsCredentialsProvider implements AwsCredentialsProvider {
    private final String accessKeyId;
    private final String secretAccessKey;

    public CustomAwsCredentialsProvider(String accessKeyId, String secretAccessKey) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return AwsBasicCredentials.create(accessKeyId, secretAccessKey);
    }

    public static CustomAwsCredentialsProvider create(Map<String, String> props) {
        return new CustomAwsCredentialsProvider(props.get("s3.access-key-id"), props.get("s3.secret-access-key"));
    }
}
