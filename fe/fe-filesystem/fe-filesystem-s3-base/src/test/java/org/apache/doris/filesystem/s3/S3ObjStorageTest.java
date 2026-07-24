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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** Unit tests for {@link S3ObjStorage} constructor behavior. */
class S3ObjStorageTest {

    // ------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------

    @Test
    void getClient_endpointOnlyConfigurationUsesRegionBuiltByProperties() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://minio.local");
        props.put("AWS_ACCESS_KEY", "ak");
        props.put("AWS_SECRET_KEY", "sk");

        S3ObjStorage storage = new S3ObjStorage(props);
        S3Client client = storage.getClient();
        try {
            Assertions.assertEquals(Region.US_EAST_1, client.serviceClientConfiguration().region());
        } finally {
            storage.close();
        }
    }

    @Test
    void getClient_appliesConfiguredSchemeOnlyToBareEndpoint() throws Exception {
        assertEndpoint("minio.local:9000", "https", "https://minio.local:9000");
        assertEndpoint("minio.local:9000", "http", "http://minio.local:9000");
        assertEndpoint("http://minio.local:9000", "https", "http://minio.local:9000");
        assertEndpoint("https://minio.local:9000", "http", "https://minio.local:9000");
    }

    private static void assertEndpoint(String endpoint, String scheme, String expected) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", endpoint);
        props.put("AWS_REGION", "us-east-1");
        props.put("AWS_ACCESS_KEY", "ak");
        props.put("AWS_SECRET_KEY", "sk");
        props.put("s3_client_http_scheme", scheme);

        S3ObjStorage storage = new S3ObjStorage(props);
        try {
            Assertions.assertEquals(URI.create(expected),
                    storage.getClient().serviceClientConfiguration().endpointOverride().orElseThrow());
        } finally {
            storage.close();
        }
    }

    // ------------------------------------------------------------------
    // close()
    // ------------------------------------------------------------------

    @Test
    void close_doesNotThrowWhenClientNotBuilt() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://ep");
        props.put("AWS_REGION", "us-east-1");

        S3ObjStorage storage = new S3ObjStorage(props);
        storage.close();
    }
}
