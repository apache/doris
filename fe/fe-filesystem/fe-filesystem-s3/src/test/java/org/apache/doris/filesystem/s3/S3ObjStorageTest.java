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

import java.util.HashMap;
import java.util.Map;

/** Unit tests for {@link S3ObjStorage} constructor behavior. */
class S3ObjStorageTest {

    // ------------------------------------------------------------------
    // Constructor & getProperties()
    // ------------------------------------------------------------------

    @Test
    void constructor_mapInputBindsThroughS3FileSystemProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");
        props.put("s3.endpoint", "https://ep");
        props.put("s3.region", "us-east-1");
        props.put("AWS_BUCKET", "my-bucket");

        S3ObjStorage storage = new S3ObjStorage(props);
        Map<String, String> stored = storage.getProperties();

        Assertions.assertEquals("ak", stored.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", stored.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("https://ep", stored.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-east-1", stored.get("AWS_REGION"));
        Assertions.assertEquals("my-bucket", stored.get("AWS_BUCKET"));

        Assertions.assertThrows(UnsupportedOperationException.class, () -> stored.put("new", "val"),
                "getProperties() should return unmodifiable map");
    }

    @Test
    void constructor_acceptsEndpointOnlyConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://minio.local");
        props.put("AWS_ACCESS_KEY", "ak");
        props.put("AWS_SECRET_KEY", "sk");

        S3ObjStorage storage = new S3ObjStorage(props);
        Map<String, String> stored = storage.getProperties();

        Assertions.assertEquals("https://minio.local", stored.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-east-1", stored.get("AWS_REGION"));
    }

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
    void constructor_usePathStyleDefaultsFalse() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.amazonaws.com");
        props.put("AWS_REGION", "us-east-1");

        S3ObjStorage storage = new S3ObjStorage(props);
        Map<String, String> stored = storage.getProperties();

        Assertions.assertEquals("false", stored.getOrDefault("use_path_style", "false"));
    }

    @Test
    void constructor_usePathStyleTrueWhenSet() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://minio.local");
        props.put("AWS_REGION", "us-west-2");
        props.put("use_path_style", "true");

        S3ObjStorage storage = new S3ObjStorage(props);
        Map<String, String> stored = storage.getProperties();

        Assertions.assertEquals("true", stored.get("use_path_style"));
    }

    @Test
    void constructor_originalMapMutationDoesNotAffectStorage() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ACCESS_KEY", "original");
        props.put("AWS_SECRET_KEY", "secret");
        props.put("AWS_ENDPOINT", "https://ep");
        props.put("AWS_REGION", "us-east-1");

        S3ObjStorage storage = new S3ObjStorage(props);
        props.put("AWS_ACCESS_KEY", "mutated");

        Assertions.assertEquals("original", storage.getProperties().get("AWS_ACCESS_KEY"),
                "Constructor must copy the input map");
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
