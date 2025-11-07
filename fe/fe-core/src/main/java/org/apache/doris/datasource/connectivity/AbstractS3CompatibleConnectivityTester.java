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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.common.util.S3URI;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.thrift.TStorageBackendType;

import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractS3CompatibleConnectivityTester implements StorageConnectivityTester {
    private static final String TEST_LOCATION = "test_location";
    protected final AbstractS3CompatibleProperties properties;
    protected final String testLocation;

    public AbstractS3CompatibleConnectivityTester(AbstractS3CompatibleProperties properties, String testLocation) {
        this.properties = properties;
        this.testLocation = testLocation;
    }

    @Override
    public TStorageBackendType getStorageType() {
        return TStorageBackendType.S3;
    }

    @Override
    public Map<String, String> getBackendProperties() {
        Map<String, String> props = new HashMap<>(properties.getBackendConfigProperties());
        props.put(TEST_LOCATION, testLocation);
        return props;
    }

    @Override
    public void testFeConnection() throws Exception {
        S3URI s3Uri = S3URI.create(testLocation,
                Boolean.parseBoolean(properties.getUsePathStyle()),
                Boolean.parseBoolean(properties.getForceParsingByStandardUrl()));

        String endpoint = properties.getEndpoint();

        try (S3Client client = S3Util.buildS3Client(
                URI.create(endpoint),
                properties.getRegion(),
                Boolean.parseBoolean(properties.getUsePathStyle()),
                properties.getAwsCredentialsProvider())) {
            client.headBucket(b -> b.bucket(s3Uri.getBucket()));
        }
    }
}
