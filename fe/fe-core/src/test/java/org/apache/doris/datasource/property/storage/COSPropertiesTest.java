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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
    public void testCOSProperties() {
        origProps.put("cos.endpoint", "https://cos.example.com");
        origProps.put("cos.access_key", "myCOSAccessKey");
        origProps.put("cos.secret_key", "myCOSSecretKey");
        origProps.put("cos.region", "us-west-1");
        origProps.put("cos.max_connections", "100");
        origProps.put("cos.request_timeout", "3000");
        origProps.put("cos.connection_timeout", "1000");
        origProps.put("cos.use_path_style", "true");
        origProps.put(StorageProperties.FS_COS_SUPPORT, "true");
        COSProperties cosProperties = (COSProperties) StorageProperties.create(origProps).get(1);
        Map<String, String> config = new HashMap<>();
        cosProperties.toHadoopConfiguration(config);

        // Validate the configuration
        Assertions.assertEquals("https://cos.example.com", config.get("fs.cos.endpoint"));
        Assertions.assertEquals("myCOSAccessKey", config.get("fs.cosn.userinfo.secretId"));
        Assertions.assertEquals("myCOSSecretKey", config.get("fs.cosn.userinfo.secretKey"));
        origProps = new HashMap<>();
        origProps.put("cos.endpoint", "https://cos.example.com");
        origProps.put(StorageProperties.FS_COS_SUPPORT, "true");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.create(origProps), "Property cos.access_key is required.");
        origProps.put("cos.access_key", "myCOSAccessKey");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.create(origProps), "Property cos.secret_key is required.");
        origProps.put("cos.secret_key", "myCOSSecretKey");
        //no any exception
        StorageProperties.create(origProps);
    }

    @Test
    public void testToNativeS3Configuration() {
        origProps.put("cos.endpoint", "cos.ap-beijing.myqcloud.com");
        origProps.put("cos.access_key", "myCOSAccessKey");
        origProps.put("cos.secret_key", "myCOSSecretKey");
        origProps.put(StorageProperties.FS_COS_SUPPORT, "true");
        //origProps.put("cos.region", "ap-beijing");

        COSProperties cosProperties = (COSProperties) StorageProperties.create(origProps).get(1);
        Map<String, String> s3Props = new HashMap<>();
        cosProperties.toNativeS3Configuration(s3Props);

        // Validate the S3 properties
        Assertions.assertEquals("cos.ap-beijing.myqcloud.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("ap-beijing", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myCOSAccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myCOSSecretKey", s3Props.get("AWS_SECRET_KEY"));
        // Add any additional assertions for other properties if needed
    }

    /**
     * This test method is used for verifying the connectivity and integration between
     * the COS (Cloud Object Storage) and HDFS (Hadoop Distributed File System) by
     * setting COS-specific properties and testing the ability to list files from an
     * HDFS path.
     * <p>
     * The method:
     * 1. Sets COS properties such as endpoint, access key, and secret key.
     * 2. Converts COS properties to HDFS configuration.
     * 3. Uses the HDFS configuration to connect to the file system.
     * 4. Lists the files in the specified HDFS path and prints the file paths to the console.
     * <p>
     * Note:
     * This test is currently disabled (@Disabled) and will not be executed unless enabled.
     * The test requires valid COS credentials (access key and secret key) and a valid
     * HDFS path to function correctly.
     *
     * @throws URISyntaxException if the URI for the HDFS path is malformed.
     * @throws IOException        if there are issues with file system access or COS properties.
     */
    @Disabled
    @Test
    public void testCOSHdfsPropertiesTest() throws URISyntaxException, IOException {
        origProps.put("cos.endpoint", "cos.ap-beijing.myqcloud.com");
        origProps.put("cos.access_key", accessKey);
        origProps.put("cos.secret_key", secretKey);
        origProps.put(StorageProperties.FS_COS_SUPPORT, "true");
        COSProperties cosProperties = (COSProperties) StorageProperties.create(origProps).get(1);

        Map<String, String> hdfsParams = new HashMap<>();
        cosProperties.toHadoopConfiguration(hdfsParams);
        Configuration configuration = new Configuration(false);
        for (Map.Entry<String, String> entry : hdfsParams.entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        FileSystem fs = FileSystem.get(new URI(hdfsPath), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));
        for (FileStatus status : fileStatuses) {
            System.out.println("File Path: " + status.getPath());
        }
    }
}
