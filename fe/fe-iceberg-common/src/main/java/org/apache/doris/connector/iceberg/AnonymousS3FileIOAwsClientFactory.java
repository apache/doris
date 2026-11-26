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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.HashMap;
import java.util.Map;

/** Keeps anonymous object access separate from the catalog's authenticated AWS metadata clients. */
public class AnonymousS3FileIOAwsClientFactory implements S3FileIOAwsClientFactory {
    private static final long serialVersionUID = 1L;

    private AwsClientFactory delegate;

    @Override
    public void initialize(Map<String, String> properties) {
        Map<String, String> fileIoProperties = new HashMap<>(properties);
        // A Glue assume-role factory or explicit object keys must not override anonymous FileIO.
        fileIoProperties.remove(AwsProperties.CLIENT_FACTORY);
        fileIoProperties.remove(S3FileIOProperties.ACCESS_KEY_ID);
        fileIoProperties.remove(S3FileIOProperties.SECRET_ACCESS_KEY);
        fileIoProperties.remove(S3FileIOProperties.SESSION_TOKEN);
        fileIoProperties.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                AnonymousCredentialsProvider.class.getName());
        delegate = AwsClientFactories.from(fileIoProperties);
    }

    @Override
    public S3Client s3() {
        return delegate.s3();
    }

    @Override
    public S3AsyncClient s3Async() {
        return delegate.s3Async();
    }
}
