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

package org.apache.doris.datasource.property.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class GCSPropertiesTest {

    private Map<String, String> origProps;

    @BeforeEach
    public void setUp() {
        origProps = new HashMap<>();
    }

    @Test
    public void testGuessIsMeByEndpoint() {
        origProps.put("gs.endpoint", "https://storage.googleapis.com");
        Assertions.assertTrue(GCSProperties.guessIsMe(origProps));
        origProps.clear();
        origProps.put("s3.endpoint", "https://storage.googleapis.com");
        Assertions.assertTrue(GCSProperties.guessIsMe(origProps));
        origProps.clear();
        origProps.put("endpoint", "https://my.custom.endpoint.com");
        Assertions.assertFalse(GCSProperties.guessIsMe(origProps));
        origProps.put("gs.endpoint", "https://my.custom.endpoint.com");
        Assertions.assertTrue(GCSProperties.guessIsMe(origProps));
    }

    @Test
    public void testDefaultValues() {
        origProps.put("gs.endpoint", "https://storage.googleapis.com");
        origProps.put("gs.access_key", "myAccessKey");
        origProps.put("gs.secret_key", "mySecretKey");
        GCSProperties gcsProperties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("https://storage.googleapis.com", gcsProperties.getEndpoint());
        Assertions.assertEquals("us-east1", gcsProperties.getRegion()); // default
        Assertions.assertEquals("myAccessKey", gcsProperties.getAccessKey());
        Assertions.assertEquals("mySecretKey", gcsProperties.getSecretKey());
        Assertions.assertEquals("false", gcsProperties.getUsePathStyle());
    }

    @Test
    public void testOverrideRegionAndPathStyle() {
        origProps.put("gs.endpoint", "https://storage.googleapis.com");
        origProps.put("gs.access_key", "myAccessKey");
        origProps.put("gs.secret_key", "mySecretKey");
        origProps.put("gs.use_path_style", "true");

        GCSProperties gcsProperties = (GCSProperties) StorageProperties.createPrimary(origProps);
        gcsProperties.setRegion("asia-northeast1");

        Assertions.assertEquals("asia-northeast1", gcsProperties.getRegion());
        Assertions.assertEquals("true", gcsProperties.getUsePathStyle());
    }

    @Test
    public void testGenerateBackendS3Configuration() {
        origProps.put("gs.endpoint", "https://storage.googleapis.com");
        origProps.put("gs.access_key", "myAccessKey");
        origProps.put("gs.secret_key", "mySecretKey");
        origProps.put("gs.connection.maximum", "200");
        origProps.put("gs.connection.request.timeout", "999");
        origProps.put("gs.connection.timeout", "888");
        origProps.put("gs.use_path_style", "true");

        GCSProperties gcsProperties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> s3Props = gcsProperties.generateBackendS3Configuration();

        Assertions.assertEquals("https://storage.googleapis.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-east1", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myAccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("mySecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("200", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("999", s3Props.get("AWS_REQUEST_TIMEOUT_MS"));
        Assertions.assertEquals("888", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("true", s3Props.get("use_path_style"));
    }
}
