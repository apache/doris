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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class OBSPropertyTest {
    private Map<String, String> origProps = new HashMap<>();

    @Test
    public void testBasicCreateTest() {
        //Map<String, String> origProps = new HashMap<>();
        origProps.put("obs.endpoint", "https://obs.example.com");
        origProps.put("obs.access_key", "myOBSAccessKey");
        origProps.put("obs.secret_key", "myOBSSecretKey");
        origProps.put(StorageProperties.FS_OBS_SUPPORT, "true");

        ObjectStorageProperties properties = (ObjectStorageProperties) StorageProperties.create(origProps).get(1);
        Configuration conf = properties.getHadoopConfiguration();

        Assertions.assertEquals("https://obs.example.com", conf.get("fs.obs.endpoint"));
        Assertions.assertEquals("myOBSAccessKey", conf.get("fs.obs.access.key"));
        Assertions.assertEquals("myOBSSecretKey", conf.get("fs.obs.secret.key"));
        Assertions.assertEquals("org.apache.hadoop.fs.obs.OBSFileSystem", conf.get("fs.obs.impl"));

        // Test creation without additional properties
        origProps = new HashMap<>();
        origProps.put("obs.endpoint", "https://obs.example.com");
        origProps.put(StorageProperties.FS_OBS_SUPPORT, "true");

        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.create(origProps), "Property obs.access_key is required.");
        origProps.put("obs.access_key", "myOBSAccessKey");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.create(origProps), "Property obs.secret_key is required.");
        origProps.put("obs.secret_key", "myOBSSecretKey");
        StorageProperties.create(origProps);
    }

    @Test
    public void testToNativeS3Configuration() {
        origProps.put("obs.access_key", "myOBSAccessKey");
        origProps.put("obs.secret_key", "myOBSSecretKey");
        origProps.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        origProps.put("connection.maximum", "88");
        origProps.put("connection.request.timeout", "100");
        origProps.put("connection.timeout", "1000");
        origProps.put("use_path_style", "true");
        origProps.put(StorageProperties.FS_OBS_SUPPORT, "true");
        OBSProperties obsProperties = (OBSProperties) StorageProperties.create(origProps).get(1);
        Map<String, String> s3Props = new HashMap<>();
        Map<String, String> obsConfig = obsProperties.getOrigProps();
        Assertions.assertTrue(!obsConfig.containsKey("test_non_storage_param"));

        origProps.forEach((k, v) -> {
            if (!k.equals("test_non_storage_param") && !k.equals(StorageProperties.FS_OBS_SUPPORT)) {
                Assertions.assertEquals(v, obsConfig.get(k));
            }
        });

        obsProperties.toNativeS3Configuration(s3Props);
        Assertions.assertEquals("obs.cn-north-4.myhuaweicloud.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cn-north-4", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myOBSAccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myOBSSecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("88", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("100", s3Props.get("AWS_REQUEST_TIMEOUT_MS"));
        Assertions.assertEquals("1000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("true", s3Props.get("use_path_style"));
        origProps.remove("use_path_style");
        obsProperties = (OBSProperties) StorageProperties.create(origProps).get(1);
        s3Props = new HashMap<>();
        obsProperties.toNativeS3Configuration(s3Props);
        Assertions.assertEquals("false", s3Props.get("use_path_style"));
    }

    private static String obsAccessKey = "";
    private static String obsSecretKey = "";
    private static String hdfsPath = "";

    /**
     * This test method verifies the integration of OBS (Object Storage Service) with HDFS
     * by setting OBS-specific properties and testing the ability to list files from an
     * HDFS path. It demonstrates how OBS properties can be converted into HDFS configuration
     * settings and used to interact with HDFS.
     * <p>
     * The method:
     * 1. Sets OBS properties such as access key, secret key, and endpoint.
     * 2. Converts OBS properties to HDFS configuration using the `toHadoopConfiguration()` method.
     * 3. Uses the HDFS configuration to connect to the file system.
     * 4. Lists the files in the specified HDFS path and prints the file paths to the console.
     * <p>
     * Note:
     * This test is currently disabled (@Disabled) and will not be executed unless enabled.
     * The test requires valid OBS credentials (access key and secret key) and a valid
     * HDFS path to function correctly.
     *
     * @throws URISyntaxException if the URI for the HDFS path is malformed.
     * @throws IOException        if there are issues with file system access or OBS properties.
     */
    @Disabled
    @Test
    public void testToHadoopConfiguration() throws URISyntaxException, IOException {
        origProps.put("obs.access_key", obsAccessKey);
        origProps.put("obs.secret_key", obsSecretKey);
        origProps.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        origProps.put(StorageProperties.FS_OBS_SUPPORT, "true");
        OBSProperties obsProperties = (OBSProperties) StorageProperties.create(origProps).get(1);
        Configuration configuration = obsProperties.getHadoopConfiguration();
        FileSystem fs = FileSystem.get(new URI(hdfsPath), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));
        for (FileStatus status : fileStatuses) {
            System.out.println("File Path: " + status.getPath());
        }
    }
}
