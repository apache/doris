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
        S3Properties s3Properties = (S3Properties) StorageProperties.create(origProps).get(1);
        Map<String, String> config = new HashMap<>();
        s3Properties.toHadoopConfiguration(config);

        // Validate the configuration
        Assertions.assertEquals("myS3AccessKey", config.get("fs.s3a.access.key"));
        Assertions.assertEquals("myS3SecretKey", config.get("fs.s3a.secret.key"));
        Assertions.assertEquals("us-west-1", config.get("fs.s3a.region"));
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

        S3Properties s3Properties = (S3Properties) StorageProperties.create(origProps).get(1);
        Map<String, String> s3Props = new HashMap<>();
        s3Properties.toNativeS3Configuration(s3Props);

        // Validate the S3 properties
        Assertions.assertEquals("https://cos.example.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-1", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myS3AccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myS3SecretKey", s3Props.get("AWS_SECRET_KEY"));
        // Add any additional assertions for other properties if needed
    }

    /**
     * This test method verifies the integration between S3 (Amazon Simple Storage Service)
     * and HDFS by setting S3-specific properties and testing the ability to list files
     * from an HDFS path. It demonstrates how S3 properties can be converted into
     * Hadoop configuration settings and used to interact with HDFS.
     * <p>
     * The method:
     * 1. Sets S3 properties such as access key, secret key, endpoint, and region.
     * 2. Converts S3 properties to HDFS configuration using the `toHadoopConfiguration()` method.
     * 3. Uses the HDFS configuration to connect to the file system.
     * 4. Lists the files in the specified HDFS path and prints the file paths to the console.
     * <p>
     * Note:
     * This test is currently disabled (@Disabled) and will not be executed unless enabled.
     * The test requires valid S3 credentials (access key and secret key) and a valid
     * HDFS path to function correctly.
     *
     * @throws URISyntaxException if the URI for the HDFS path is malformed.
     * @throws IOException        if there are issues with file system access or S3 properties.
     */
    @Disabled
    @Test
    public void testS3HdfsPropertiesTest() throws URISyntaxException, IOException {
        origProps.put("s3.endpoint", "s3.ap-northeast-1.amazonaws.com");
        origProps.put("s3.access_key", accessKey);
        origProps.put("s3.secret_key", secretKey);
        origProps.put("s3.region", "ap-northeast-1");
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        S3Properties s3Properties = (S3Properties) StorageProperties.create(origProps).get(1);

        Map<String, String> hdfsParams = new HashMap<>();
        s3Properties.toHadoopConfiguration(hdfsParams);
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
