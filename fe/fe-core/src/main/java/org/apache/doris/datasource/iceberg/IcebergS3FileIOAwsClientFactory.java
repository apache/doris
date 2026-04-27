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

import org.apache.doris.datasource.property.storage.S3Properties;

import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;

public class IcebergS3FileIOAwsClientFactory implements S3FileIOAwsClientFactory {

    private S3Properties s3Properties;
    private S3FileIOProperties s3FileIOProperties;
    private HttpClientProperties httpClientProperties;

    public IcebergS3FileIOAwsClientFactory() {
        this.s3FileIOProperties = new S3FileIOProperties();
        this.httpClientProperties = new HttpClientProperties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.s3Properties = S3Properties.of(properties);
        this.s3FileIOProperties = new S3FileIOProperties(properties);
        this.httpClientProperties = new HttpClientProperties(properties);
    }

    @Override
    public S3Client s3() {
        return S3Client.builder()
                .region(Region.of(s3Properties.getRegion()))
                .credentialsProvider(s3Properties.getAwsCredentialsProvider())
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .applyMutation(s3FileIOProperties::applyServiceConfigurations)
                .applyMutation(s3FileIOProperties::applySignerConfiguration)
                .applyMutation(s3FileIOProperties::applyS3AccessGrantsConfigurations)
                .applyMutation(s3FileIOProperties::applyUserAgentConfigurations)
                .applyMutation(s3FileIOProperties::applyRetryConfigurations)
                .build();
    }

    @Override
    public S3AsyncClient s3Async() {
        if (s3FileIOProperties.isS3CRTEnabled()) {
            return S3AsyncClient.crtBuilder()
                    .region(Region.of(s3Properties.getRegion()))
                    .credentialsProvider(s3Properties.getAwsCredentialsProvider())
                    .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                    .applyMutation(s3FileIOProperties::applyS3CrtConfigurations)
                    .build();
        }
        return S3AsyncClient.builder()
                .region(Region.of(s3Properties.getRegion()))
                .credentialsProvider(s3Properties.getAwsCredentialsProvider())
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .build();
    }
}
