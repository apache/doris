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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class OSSPropertiesTest {

    private static String ossAccessKey = "";
    private static String ossSecretKey = "";
    private static String hdfsPath = "";

    @Test
    public void testBasicCreateTest() {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "https://oss.aliyuncs.com");
        origProps.put("oss.access_key", "myOSSAccessKey");
        origProps.put("oss.secret_key", "myOSSSecretKey");
        origProps.put(StorageProperties.FS_OSS_SUPPORT, "true");
        Map<String, String> finalOrigProps = origProps;
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> StorageProperties.createPrimary(finalOrigProps), "Property oss.endpoint is required.");
        origProps.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        Map<String, String> finalOrigProps1 = origProps;
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(finalOrigProps1));
        origProps = new HashMap<>();
        origProps.put("oss.endpoint", "https://oss.aliyuncs.com");
        Map<String, String> finalOrigProps2 = origProps;
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> StorageProperties.createPrimary(finalOrigProps2));

    }


    @Test
    public void testToNativeS3Configuration() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.access_key", "myOSSAccessKey");
        origProps.put("oss.secret_key", "myOSSSecretKey");
        origProps.put("oss.endpoint", "oss-cn-beijing-internal.aliyuncs.com");
        origProps.put(StorageProperties.FS_OSS_SUPPORT, "true");
        origProps.put("connection.maximum", "88");
        origProps.put("connection.request.timeout", "100");
        origProps.put("connection.timeout", "1000");
        origProps.put("use_path_style", "true");
        origProps.put("test_non_storage_param", "6000");
        OSSProperties ossProperties = (OSSProperties) StorageProperties.createAll(origProps).get(0);
        Map<String, String> s3Props;

        Map<String, String> ossConfig = ossProperties.getMatchedProperties();
        Assertions.assertTrue(!ossConfig.containsKey("test_non_storage_param"));

        origProps.forEach((k, v) -> {
            if (!k.equals("test_non_storage_param") && !k.equals(StorageProperties.FS_OSS_SUPPORT)) {
                Assertions.assertEquals(v, ossConfig.get(k));
            }
        });


        s3Props = ossProperties.generateBackendS3Configuration();
        Assertions.assertEquals("oss-cn-beijing-internal.aliyuncs.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cn-beijing-internal", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myOSSAccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myOSSSecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("88", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("100", s3Props.get("AWS_REQUEST_TIMEOUT_MS"));
        Assertions.assertEquals("1000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("true", s3Props.get("use_path_style"));
        origProps.remove("use_path_style");
        ossProperties = (OSSProperties) StorageProperties.createAll(origProps).get(0);
        s3Props = ossProperties.generateBackendS3Configuration();
        Assertions.assertEquals("false", s3Props.get("use_path_style"));
    }

    @Test
    public void testGetRegion() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        origProps.put("oss.access_key", "myCOSAccessKey");
        origProps.put("oss.secret_key", "myCOSSecretKey");
        OSSProperties ossProperties = (OSSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("cn-hangzhou", ossProperties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", ossProperties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", ossProperties.getSecretKey());
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", ossProperties.getEndpoint());
    }

    @Test
    public void testGetRegionWithDefault() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("uri", "https://examplebucket-1250000000.oss-cn-hangzhou.aliyuncs.com/test/file.txt");
        origProps.put("oss.access_key", "myCOSAccessKey");
        origProps.put("oss.secret_key", "myCOSSecretKey");
        OSSProperties ossProperties = (OSSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("cn-hangzhou", ossProperties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", ossProperties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", ossProperties.getSecretKey());
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", ossProperties.getEndpoint());
        Map<String, String> cosNoEndpointProps = new HashMap<>();
        cosNoEndpointProps.put("oss.access_key", "myCOSAccessKey");
        cosNoEndpointProps.put("oss.secret_key", "myCOSSecretKey");
        cosNoEndpointProps.put("oss.region", "cn-hangzhou");
        origProps.put("uri", "s3://examplebucket-1250000000/test/file.txt");
        // not support
        Assertions.assertThrowsExactly(RuntimeException.class, () -> StorageProperties.createPrimary(cosNoEndpointProps), "Property cos.endpoint is required.");
    }
}
