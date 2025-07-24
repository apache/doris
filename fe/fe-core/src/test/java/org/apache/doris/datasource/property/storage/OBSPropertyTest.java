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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class OBSPropertyTest {
    private Map<String, String> origProps = new HashMap<>();

    @Test
    public void testBasicCreateTest() throws UserException {
        //Map<String, String> origProps = new HashMap<>();
        origProps.put("obs.endpoint", "https://obs.example.com");
        origProps.put("obs.access_key", "myOBSAccessKey");
        origProps.put("obs.secret_key", "myOBSSecretKey");
        origProps.put(StorageProperties.FS_OBS_SUPPORT, "true");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Invalid endpoint format: https://obs.example.com");

        // Test creation without additional properties
        origProps = new HashMap<>();
        origProps.put("obs.endpoint", "https://obs.example.com");
        origProps.put(StorageProperties.FS_OBS_SUPPORT, "true");

        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Property obs.access_key is required.");
        origProps.put("obs.access_key", "myOBSAccessKey");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Property obs.secret_key is required.");
        origProps.put("obs.secret_key", "myOBSSecretKey");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createAll(origProps), "Invalid endpoint format: https://obs.example.com");
        origProps.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createAll(origProps));
        origProps.put("obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createAll(origProps));
    }

    @Test
    public void testToNativeS3Configuration() throws UserException {
        origProps.put("obs.access_key", "myOBSAccessKey");
        origProps.put("obs.secret_key", "myOBSSecretKey");
        origProps.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        origProps.put("connection.maximum", "88");
        origProps.put("connection.request.timeout", "100");
        origProps.put("connection.timeout", "1000");
        origProps.put("use_path_style", "true");
        origProps.put("test_non_storage_param", "test_non_storage_value");
        origProps.put(StorageProperties.FS_OBS_SUPPORT, "true");
        OBSProperties obsProperties = (OBSProperties) StorageProperties.createAll(origProps).get(0);
        Map<String, String> s3Props = new HashMap<>();
        Map<String, String> obsConfig = obsProperties.getMatchedProperties();
        Assertions.assertTrue(!obsConfig.containsKey("test_non_storage_param"));

        origProps.forEach((k, v) -> {
            if (!k.equals("test_non_storage_param") && !k.equals(StorageProperties.FS_OBS_SUPPORT)) {
                Assertions.assertEquals(v, obsConfig.get(k));
            }
        });

        s3Props = obsProperties.getBackendConfigProperties();
        Assertions.assertEquals("obs.cn-north-4.myhuaweicloud.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cn-north-4", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myOBSAccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myOBSSecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("88", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("100", s3Props.get("AWS_REQUEST_TIMEOUT_MS"));
        Assertions.assertEquals("1000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("true", s3Props.get("use_path_style"));
        origProps.remove("use_path_style");
        obsProperties = (OBSProperties) StorageProperties.createAll(origProps).get(0);
        s3Props = obsProperties.getBackendConfigProperties();
        Assertions.assertEquals("false", s3Props.get("use_path_style"));
    }


    @Test
    public void testGetRegion() throws UserException {
        origProps.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        origProps.put("obs.access_key", "myCOSAccessKey");
        origProps.put("obs.secret_key", "myCOSSecretKey");
        OBSProperties obsProperties = (OBSProperties) StorageProperties.createAll(origProps).get(0);
        Assertions.assertEquals("cn-north-4", obsProperties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", obsProperties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", obsProperties.getSecretKey());
        Assertions.assertEquals("obs.cn-north-4.myhuaweicloud.com", obsProperties.getEndpoint());
    }

    @Test
    public void testGetRegionWithDefault() throws UserException {
        origProps.put("uri", "https://examplebucket-1250000000.obs.cn-north-4.myhuaweicloud.com/test/file.txt");
        origProps.put("obs.access_key", "myCOSAccessKey");
        origProps.put("obs.secret_key", "myCOSSecretKey");
        OBSProperties obsProperties = (OBSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("cn-north-4", obsProperties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", obsProperties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", obsProperties.getSecretKey());
        Assertions.assertEquals("obs.cn-north-4.myhuaweicloud.com", obsProperties.getEndpoint());
        Map<String, String> cosNoEndpointProps = new HashMap<>();
        cosNoEndpointProps.put("obs.access_key", "myCOSAccessKey");
        cosNoEndpointProps.put("obs.secret_key", "myCOSSecretKey");
        cosNoEndpointProps.put("obs.region", "ap-beijing");
        cosNoEndpointProps.put("uri", "s3://examplebucket-1250000000/myhuaweicloud.com/test/file.txt");
        //not support
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> StorageProperties.createPrimary(cosNoEndpointProps), "Property cos.endpoint is required.");
    }

    @Test
    public void testmissingAccessKey() {
        origProps.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        origProps.put("obs.secret_key", "myCOSSecretKey");
        Assertions.assertThrows(StoragePropertiesException.class, () -> StorageProperties.createPrimary(origProps),
                 "Please set access_key and secret_key or omit both for anonymous access to public bucket.");
    }

    @Test
    public void testMissingSecretKey() {
        origProps.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        origProps.put("obs.access_key", "myCOSAccessKey");
        Assertions.assertThrows(StoragePropertiesException.class, () -> StorageProperties.createPrimary(origProps),
                 "Please set access_key and secret_key or omit both for anonymous access to public bucket.");
    }

    private static String obsAccessKey = "";
    private static String obsSecretKey = "";

}
