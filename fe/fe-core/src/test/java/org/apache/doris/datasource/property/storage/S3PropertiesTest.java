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

public class S3PropertiesTest {
    private Map<String, String> origProps;

    private static String secretKey = "";
    private static String accessKey = "";
    private static String hdfsPath = "";

    @BeforeEach
    public void setUp() {
        origProps = new HashMap<>();
    }

    @Test
    public void testS3Properties() {
        origProps.put("s3.endpoint", "https://cos.example.com");
        origProps.put("s3.access_key", "myS3AccessKey");
        origProps.put("s3.secret_key", "myS3SecretKey");
        origProps.put("s3.region", "us-west-1");
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        origProps = new HashMap<>();
        origProps.put("s3.endpoint", "https://s3.example.com");
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.create(origProps), "Property cos.access_key is required.");
        origProps.put("s3.access_key", "myS3AccessKey");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.create(origProps), "Property cos.secret_key is required.");
        origProps.put("s3.secret_key", "myS3SecretKey");
        StorageProperties.create(origProps);
    }

    @Test
    public void testToNativeS3Configuration() {
        origProps.put("s3.endpoint", "https://cos.example.com");
        origProps.put("s3.access_key", "myS3AccessKey");
        origProps.put("s3.secret_key", "myS3SecretKey");
        origProps.put("s3.region", "us-west-1");
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        origProps.put("use_path_style", "true");
        origProps.put("s3.connection.maximum", "88");
        origProps.put("s3.connection.timeout", "6000");
        origProps.put("test_non_storage_param", "6000");


        S3Properties s3Properties = (S3Properties) StorageProperties.create(origProps).get(1);
        Map<String, String> s3Props = new HashMap<>();
        s3Properties.toNativeS3Configuration(s3Props);
        Map<String, String> s3Config = s3Properties.getOrigProps();
        Assertions.assertTrue(!s3Config.containsKey("test_non_storage_param"));

        origProps.forEach((k, v) -> {
            if (!k.equals("test_non_storage_param") && !k.equals(StorageProperties.FS_S3_SUPPORT)) {
                Assertions.assertEquals(v, s3Config.get(k));
            }
        });


        // Validate the S3 properties
        Assertions.assertEquals("https://cos.example.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-1", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myS3AccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myS3SecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("88", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("6000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("true", s3Props.get("use_path_style"));
        origProps.remove("use_path_style");
        origProps.remove("s3.connection.maximum");
        origProps.remove("s3.connection.timeout");
        s3Properties = (S3Properties) StorageProperties.create(origProps).get(1);
        s3Props = new HashMap<>();
        s3Properties.toNativeS3Configuration(s3Props);
        Assertions.assertEquals("false", s3Props.get("use_path_style"));
        Assertions.assertEquals("50", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("1000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
    }
}
