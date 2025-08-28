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
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class COSPropertiesTest {
    private Map<String, String> origProps;

    private static String secretKey = "";
    private static String accessKey = "";
    private static String hdfsPath = "";

    @BeforeEach
    public void setUp() {
        origProps = new HashMap<>();
    }

    @Test
    public void testCOSProperties() throws UserException {
        origProps.put("cos.endpoint", "https://cos.example.com");
        origProps.put("cos.access_key", "myCOSAccessKey");
        origProps.put("cos.secret_key", "myCOSSecretKey");
        origProps.put("cos.region", "ap-beijing-1");
        origProps.put("connection.maximum", "88");
        origProps.put("connection.request.timeout", "100");
        origProps.put("connection.timeout", "1000");
        origProps.put("use_path_style", "true");
        origProps.put(StorageProperties.FS_COS_SUPPORT, "true");
        origProps.put("test_non_storage_param", "6000");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Invalid endpoint format: https://cos.example.com");
        origProps.put("cos.endpoint", "cos.ap-beijing-1.myqcloud.com");
        COSProperties cosProperties = (COSProperties) StorageProperties.createAll(origProps).get(0);
        Map<String, String> cosConfig = cosProperties.getMatchedProperties();
        Assertions.assertTrue(!cosConfig.containsKey("test_non_storage_param"));

        origProps.forEach((k, v) -> {
            if (!k.equals("test_non_storage_param") && !k.equals(StorageProperties.FS_COS_SUPPORT)) {
                Assertions.assertEquals(v, cosConfig.get(k));
            }
        });
        origProps = new HashMap<>();
        origProps.put("cos.endpoint", "https://cos.example.com");
        origProps.put(StorageProperties.FS_COS_SUPPORT, "true");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Property cos.access_key is required.");
        origProps.put("cos.access_key", "myCOSAccessKey");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Property cos.secret_key is required.");
        origProps.put("cos.secret_key", "myCOSSecretKey");
        //no any exception
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> StorageProperties.createPrimary(origProps), "Invalid endpoint format: https://cos.example.com");
        origProps.put("cos.endpoint", "cos.ap-beijing.myqcloud.com");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testToNativeS3Configuration() throws UserException {
        origProps.put("cos.endpoint", "cos.ap-beijing.myqcloud.com");
        origProps.put("cos.access_key", "myCOSAccessKey");
        origProps.put("cos.secret_key", "myCOSSecretKey");
        origProps.put("test_non_storage_param", "6000");
        origProps.put("connection.maximum", "88");
        origProps.put("connection.request.timeout", "100");
        origProps.put("connection.timeout", "1000");
        origProps.put(StorageProperties.FS_COS_SUPPORT, "true");
        //origProps.put("cos.region", "ap-beijing");

        COSProperties cosProperties = (COSProperties) StorageProperties.createAll(origProps).get(0);
        Map<String, String> s3Props = cosProperties.generateBackendS3Configuration();
        Map<String, String> cosConfig = cosProperties.getMatchedProperties();
        Assertions.assertTrue(!cosConfig.containsKey("test_non_storage_param"));

        origProps.forEach((k, v) -> {
            if (!k.equals("test_non_storage_param") && !k.equals(StorageProperties.FS_COS_SUPPORT)) {
                Assertions.assertEquals(v, cosConfig.get(k));
            }
        });
        // Validate the S3 properties
        Assertions.assertEquals("cos.ap-beijing.myqcloud.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("ap-beijing", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myCOSAccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myCOSSecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("88", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("100", s3Props.get("AWS_REQUEST_TIMEOUT_MS"));
        Assertions.assertEquals("1000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("false", s3Props.get("use_path_style"));
        origProps.put("use_path_style", "true");
        cosProperties = (COSProperties) StorageProperties.createAll(origProps).get(0);
        s3Props = cosProperties.generateBackendS3Configuration();
        Assertions.assertEquals("true", s3Props.get("use_path_style"));
        // Add any additional assertions for other properties if needed
    }

    @Test
    public void testGetRegion() throws UserException {
        origProps.put("cos.endpoint", "cos.ap-beijing.myqcloud.com");
        origProps.put("cos.access_key", "myCOSAccessKey");
        origProps.put("cos.secret_key", "myCOSSecretKey");
        COSProperties cosProperties = (COSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("ap-beijing", cosProperties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", cosProperties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", cosProperties.getSecretKey());
        Assertions.assertEquals("cos.ap-beijing.myqcloud.com", cosProperties.getEndpoint());
    }

    @Test
    public void testGetRegionWithDefault() throws UserException {
        origProps.put("uri", "https://examplebucket-1250000000.cos.ap-beijing.myqcloud.com/test/file.txt");
        origProps.put("cos.access_key", "myCOSAccessKey");
        origProps.put("cos.secret_key", "myCOSSecretKey");
        COSProperties cosProperties = (COSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("ap-beijing", cosProperties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", cosProperties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", cosProperties.getSecretKey());
        Assertions.assertEquals("cos.ap-beijing.myqcloud.com", cosProperties.getEndpoint());
        Map<String, String> cosNoEndpointProps = new HashMap<>();
        cosNoEndpointProps.put("cos.access_key", "myCOSAccessKey");
        cosNoEndpointProps.put("cos.secret_key", "myCOSSecretKey");
        cosNoEndpointProps.put("cos.region", "ap-beijing");
        origProps.put("uri", "s3://examplebucket-1250000000/test/file.txt");
        //not support this case
        Assertions.assertThrowsExactly(StoragePropertiesException.class, () -> StorageProperties.createPrimary(cosNoEndpointProps), "Property cos.endpoint is required.");
    }

    @Test
    public void testMissingAccessKey() {
        origProps.put("cos.endpoint", "cos.ap-beijing.myqcloud.com");
        origProps.put("cos.secret_key", "myCOSSecretKey");
        Assertions.assertThrows(StoragePropertiesException.class, () -> StorageProperties.createPrimary(origProps),
                 "Please set access_key and secret_key or omit both for anonymous access to public bucket.");
    }

    @Test
    public void testMissingSecretKey() {
        origProps.put("cos.endpoint", "cos.ap-beijing.myqcloud.com");
        origProps.put("cos.access_key", "myCOSAccessKey");
        Assertions.assertThrows(StoragePropertiesException.class, () -> StorageProperties.createPrimary(origProps),
                 "Please set access_key and secret_key or omit both for anonymous access to public bucket.");
    }
}
