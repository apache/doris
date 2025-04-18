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

import org.apache.doris.common.UserException;

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
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Property cos.access_key is required.");
        origProps.put("s3.access_key", "myS3AccessKey");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Property cos.secret_key is required.");
        origProps.put("s3.secret_key", "myS3SecretKey");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Invalid endpoint format: https://s3.example.com");
        origProps.put("s3.endpoint", "s3.us-west-1.amazonaws.com");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createAll(origProps));
    }

    @Test
    public void testToNativeS3Configuration() throws UserException {
        origProps.put("s3.endpoint", "https://cos.example.com");
        origProps.put("s3.access_key", "myS3AccessKey");
        origProps.put("s3.secret_key", "myS3SecretKey");
        origProps.put("s3.region", "us-west-1");
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        origProps.put("use_path_style", "true");
        origProps.put("s3.connection.maximum", "88");
        origProps.put("s3.connection.timeout", "6000");
        origProps.put("test_non_storage_param", "6000");


        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            StorageProperties.createAll(origProps).get(1);
        }, "Invalid endpoint format: https://cos.example.com");
        origProps.put("s3.endpoint", "s3.us-west-1.amazonaws.com");
        S3Properties s3Properties = (S3Properties) StorageProperties.createAll(origProps).get(0);
        Map<String, String> s3Props = s3Properties.getBackendConfigProperties();
        Map<String, String> s3Config = s3Properties.getMatchedProperties();
        Assertions.assertTrue(!s3Config.containsKey("test_non_storage_param"));

        origProps.forEach((k, v) -> {
            if (!k.equals("test_non_storage_param") && !k.equals(StorageProperties.FS_S3_SUPPORT)) {
                Assertions.assertEquals(v, s3Config.get(k));
            }
        });
        // Validate the S3 properties
        Assertions.assertEquals("s3.us-west-1.amazonaws.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-1", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myS3AccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myS3SecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("88", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("6000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("true", s3Props.get("use_path_style"));
        origProps.remove("use_path_style");
        origProps.remove("s3.connection.maximum");
        origProps.remove("s3.connection.timeout");
        s3Props = s3Properties.getBackendConfigProperties();

        Assertions.assertEquals("true", s3Props.get("use_path_style"));
        Assertions.assertEquals("88", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("6000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
    }


    @Test
    public void testGetRegion() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("s3.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        origProps.put("s3.access_key", "myCOSAccessKey");
        origProps.put("s3.secret_key", "myCOSSecretKey");
        origProps.put("s3.region", "cn-hangzhou");
        OSSProperties ossProperties = (OSSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("cn-hangzhou", ossProperties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", ossProperties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", ossProperties.getSecretKey());
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", ossProperties.getEndpoint());
        origProps = new HashMap<>();
        origProps.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        origProps.put("s3.access_key", "myCOSAccessKey");
        origProps.put("s3.secret_key", "myCOSSecretKey");
        origProps.put("s3.region", "us-west-2");
        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-west-2", s3Properties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", s3Properties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", s3Properties.getSecretKey());
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", s3Properties.getEndpoint());


    }

    @Test
    public void testGetRegionWithDefault() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("uri", "https://example-bucket.s3.us-west-2.amazonaws.com/path/to/file.txt\n");
        origProps.put("s3.access_key", "myCOSAccessKey");
        origProps.put("s3.secret_key", "myCOSSecretKey");
        origProps.put("s3.region", "us-west-2");
        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-west-2", s3Properties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", s3Properties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", s3Properties.getSecretKey());
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", s3Properties.getEndpoint());
        Map<String, String> s3EndpointProps = new HashMap<>();
        s3EndpointProps.put("oss.access_key", "myCOSAccessKey");
        s3EndpointProps.put("oss.secret_key", "myCOSSecretKey");
        s3EndpointProps.put("oss.region", "cn-hangzhou");
        origProps.put("uri", "s3://examplebucket-1250000000/test/file.txt");
        //not support
        Assertions.assertThrowsExactly(RuntimeException.class, () -> StorageProperties.createPrimary(s3EndpointProps), "Property cos.endpoint is required.");
    }
}
