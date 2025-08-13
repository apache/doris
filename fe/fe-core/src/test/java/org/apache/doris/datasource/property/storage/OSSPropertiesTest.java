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

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

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
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Invalid endpoint: https://oss.aliyuncs.com", () -> StorageProperties.createPrimary(finalOrigProps));
        origProps.put("oss.endpoint", "oss-cn-shenzhen-finance-1-internal.aliyuncs.com");
        Map<String, String> finalOrigProps1 = origProps;
        OSSProperties ossProperties = (OSSProperties) StorageProperties.createPrimary(finalOrigProps1);
        Assertions.assertEquals("oss-cn-shenzhen-finance-1-internal.aliyuncs.com", ossProperties.getEndpoint());
        Assertions.assertEquals("cn-shenzhen-finance-1", ossProperties.getRegion());
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(finalOrigProps1));
        origProps = new HashMap<>();
        origProps.put("oss.endpoint", "oss-cn-shenzhen-finance-1-internal.aliyuncs.com");
        Map<String, String> finalOrigProps2 = origProps;
        // allow both access_key and secret_key to be empty for anonymous access
        ExceptionChecker.expectThrowsNoException(() -> StorageProperties.createPrimary(finalOrigProps2));
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
        Assertions.assertEquals("cn-beijing", s3Props.get("AWS_REGION"));
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
        origProps.put("oss.access_key", "myOSSAccessKey");
        origProps.put("oss.secret_key", "myOSSSecretKey");
        OSSProperties ossProperties = (OSSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("cn-hangzhou", ossProperties.getRegion());
        Assertions.assertEquals("myOSSAccessKey", ossProperties.getAccessKey());
        Assertions.assertEquals("myOSSSecretKey", ossProperties.getSecretKey());
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", ossProperties.getEndpoint());
        origProps.put("oss.endpoint", "oss-cn-hangzhou-internal.aliyuncs.com");
        Assertions.assertEquals("cn-hangzhou", ((OSSProperties) StorageProperties.createPrimary(origProps)).getRegion());
        origProps.put("oss.endpoint", "s3.oss-cn-shanghai.aliyuncs.com");
        Assertions.assertEquals("cn-shanghai", ((OSSProperties) StorageProperties.createPrimary(origProps)).getRegion());
        origProps.put("oss.endpoint", "s3.oss-cn-hongkong-internal.aliyuncs.com");
        Assertions.assertEquals("cn-hongkong", ((OSSProperties) StorageProperties.createPrimary(origProps)).getRegion());
        origProps.put("oss.endpoint", "https://s3.oss-cn-hongkong-internal.aliyuncs.com");
        Assertions.assertEquals("cn-hongkong", ((OSSProperties) StorageProperties.createPrimary(origProps)).getRegion());
        origProps.put("oss.endpoint", "http://s3.oss-cn-hongkong.aliyuncs.com");
        Assertions.assertEquals("cn-hongkong", ((OSSProperties) StorageProperties.createPrimary(origProps)).getRegion());
        origProps.put("oss.endpoint", "https://dlf.cn-beijing.aliyuncs.com");
        Assertions.assertEquals("cn-beijing", ((OSSProperties) StorageProperties.createAll(origProps).get(1)).getRegion());
    }

    @Test
    public void testGetRegionWithDefault() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("uri", "https://examplebucket-1250000000.oss-cn-hangzhou.aliyuncs.com/test/file.txt");
        origProps.put("oss.access_key", "myOSSAccessKey");
        origProps.put("oss.secret_key", "myOSSSecretKey");
        OSSProperties ossProperties = (OSSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("cn-hangzhou", ossProperties.getRegion());
        Assertions.assertEquals("myOSSAccessKey", ossProperties.getAccessKey());
        Assertions.assertEquals("myOSSSecretKey", ossProperties.getSecretKey());
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", ossProperties.getEndpoint());
        Map<String, String> ossNoEndpointProps = new HashMap<>();
        ossNoEndpointProps.put("oss.access_key", "myOSSAccessKey");
        ossNoEndpointProps.put("oss.secret_key", "myOSSSecretKey");
        ossNoEndpointProps.put("oss.region", "cn-hangzhou");
        origProps.put("uri", "s3://examplebucket-1250000000/test/file.txt");
        // oss support without endpoint
        ExceptionChecker.expectThrowsNoException(() -> StorageProperties.createPrimary(ossNoEndpointProps));
    }

    @Test
    public void testMissingAccessKey() {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        origProps.put("oss.secret_key", "myOSSSecretKey");
        ExceptionChecker.expectThrowsWithMsg(StoragePropertiesException.class,
                "Please set access_key and secret_key or omit both for anonymous access to public bucket.",
                () -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testMissingSecretKey() {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        origProps.put("oss.access_key", "myOSSAccessKey");
        ExceptionChecker.expectThrowsWithMsg(StoragePropertiesException.class,
                "Please set access_key and secret_key or omit both for anonymous access to public bucket.",
                () -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testNotEndpoint() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("uri", "oss://examplebucket-1250000000/test/file.txt");
        origProps.put("oss.access_key", "myOSSAccessKey");
        origProps.put("oss.secret_key", "myOSSSecretKey");
        origProps.put("oss.region", "cn-hangzhou");
        Assertions.assertEquals("oss-cn-hangzhou-internal.aliyuncs.com",
                ((OSSProperties) StorageProperties.createPrimary(origProps)).getEndpoint());
        origProps.put("dlf.access.public", "true");
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com",
                ((OSSProperties) StorageProperties.createPrimary(origProps)).getEndpoint());
        origProps.put("uri", "https://doris-regression-hk.oss-cn-hangzhou-internal.aliyuncs.com/regression/datalake/pipeline_data/data_page_v2_gzip.parquet");
        Assertions.assertEquals("oss-cn-hangzhou-internal.aliyuncs.com", ((OSSProperties) StorageProperties.createPrimary(origProps)).getEndpoint());
    }
}
