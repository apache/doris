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

import org.apache.doris.common.util.S3Util;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.CredentialSupplier;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.HashMap;
import java.util.Map;

/**
 * Applies Doris FE's default S3 endpoint scheme when Iceberg creates its AWS SDK client.
 */
public final class DorisS3FileIOAwsClientFactory implements S3FileIOAwsClientFactory, CredentialSupplier {

    private AwsClientFactory delegate;

    public static void configure(Map<String, String> properties) {
        if (StringUtils.isNotBlank(properties.get(S3FileIOProperties.ENDPOINT))) {
            properties.putIfAbsent(S3FileIOProperties.CLIENT_FACTORY,
                    DorisS3FileIOAwsClientFactory.class.getName());
        }
    }

    @Override
    public void initialize(Map<String, String> properties) {
        Map<String, String> clientProperties = new HashMap<>(properties);
        clientProperties.remove(S3FileIOProperties.CLIENT_FACTORY);
        String endpoint = clientProperties.get(S3FileIOProperties.ENDPOINT);
        if (StringUtils.isNotBlank(endpoint)) {
            clientProperties.put(S3FileIOProperties.ENDPOINT, S3Util.buildEndpointUrl(endpoint));
        }
        delegate = AwsClientFactories.from(clientProperties);
    }

    @Override
    public S3Client s3() {
        return delegate.s3();
    }

    @Override
    public S3AsyncClient s3Async() {
        return delegate.s3Async();
    }

    @Override
    public String getCredential() {
        if (delegate instanceof CredentialSupplier) {
            return ((CredentialSupplier) delegate).getCredential();
        }
        return null;
    }
}
