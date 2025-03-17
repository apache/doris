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
        ObjectStorageProperties properties = (ObjectStorageProperties) StorageProperties.create(origProps).get(1);
        Configuration conf = properties.getHadoopConfiguration();
        Assertions.assertEquals("https://oss.aliyuncs.com", conf.get("fs.oss.endpoint"));
        Assertions.assertEquals("myOSSAccessKey", conf.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("myOSSSecretKey", conf.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals("org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem", conf.get("fs.oss.impl"));
        origProps = new HashMap<>();
        origProps.put("oss.endpoint", "https://oss.aliyuncs.com");
        StorageProperties.create(origProps);
    }


    @Test
    public void testToNativeS3Configuration() {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.access_key", "myOSSAccessKey");
        origProps.put("oss.secret_key", "myOSSSecretKey");
        origProps.put("oss.endpoint", "oss-cn-beijing-internal.aliyuncs.com");
        origProps.put(StorageProperties.FS_OSS_SUPPORT, "true");
        OSSProperties ossProperties = (OSSProperties) StorageProperties.create(origProps).get(1);
        Map<String, String> s3Props = new HashMap<>();


        ossProperties.toNativeS3Configuration(s3Props);
        Assertions.assertEquals("oss-cn-beijing-internal.aliyuncs.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cn-beijing-internal", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myOSSAccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myOSSSecretKey", s3Props.get("AWS_SECRET_KEY"));
    }


    /**
     * This test method verifies the integration between OSS (Object Storage Service)
     * and HDFS by setting OSS-specific properties and testing the ability to list
     * files from an HDFS path. It demonstrates how OSS properties can be converted
     * into Hadoop configuration settings and used to interact with HDFS.
     * <p>
     * The method:
     * 1. Sets OSS properties such as access key, secret key, and endpoint.
     * 2. Converts OSS properties to HDFS configuration using the `toHadoopConfiguration()` method.
     * 3. Uses the HDFS configuration to connect to the file system.
     * 4. Lists the files in the specified HDFS path and prints the file paths to the console.
     * <p>
     * Note:
     * This test is currently disabled (@Disabled) and will not be executed unless enabled.
     * The test requires valid OSS credentials (access key and secret key) and a valid
     * HDFS path to function correctly.
     *
     * @throws URISyntaxException if the URI for the HDFS path is malformed.
     * @throws IOException        if there are issues with file system access or OSS properties.
     */
    @Disabled
    @Test
    public void testOSSHdfsProperties() throws IOException, URISyntaxException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.access_key", ossAccessKey);
        origProps.put("oss.secret_key", ossSecretKey);
        origProps.put("oss.endpoint", "oss-cn-beijing-internal.aliyuncs.com");
        origProps.put(StorageProperties.FS_OSS_SUPPORT, "true");
        OSSProperties ossProperties = (OSSProperties) StorageProperties.create(origProps).get(1);
        // ossParams.put("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.JindoOSS");
        Configuration configuration = ossProperties.getHadoopConfiguration();
        FileSystem fs = FileSystem.get(new URI(hdfsPath), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));
        for (FileStatus status : fileStatuses) {
            System.out.println("File Path: " + status.getPath());
        }
    }
}
