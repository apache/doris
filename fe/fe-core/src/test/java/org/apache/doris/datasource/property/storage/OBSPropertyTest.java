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
import org.apache.hadoop.fs.FileSystem;
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
        properties.toHadoopConfiguration(origProps);

        Assertions.assertEquals("https://obs.example.com", origProps.get("fs.obs.endpoint"));
        Assertions.assertEquals("myOBSAccessKey", origProps.get("fs.obs.access.key"));
        Assertions.assertEquals("myOBSSecretKey", origProps.get("fs.obs.secret.key"));
        Assertions.assertEquals("org.apache.hadoop.fs.obs.OBSFileSystem", origProps.get("fs.obs.impl"));

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
        origProps.put(StorageProperties.FS_OBS_SUPPORT, "true");
        OBSProperties obsProperties = (OBSProperties) StorageProperties.create(origProps).get(1);
        Map<String, String> s3Props = new HashMap<>();


        obsProperties.toNativeS3Configuration(s3Props);
        Assertions.assertEquals("obs.cn-north-4.myhuaweicloud.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cn-north-4", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myOBSAccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myOBSSecretKey", s3Props.get("AWS_SECRET_KEY"));
    }

    private static String obsAccessKey = "";
    private static String obsSecretKey = "";
    private static String hdfsPath = "";

    @Disabled
    @Test
    public void testToHadoopConfiguration() throws URISyntaxException, IOException {
        origProps.put("obs.access_key", obsAccessKey);
        origProps.put("obs.secret_key", obsSecretKey);
        origProps.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        origProps.put(StorageProperties.FS_OBS_SUPPORT, "true");
        OBSProperties obsProperties = (OBSProperties) StorageProperties.create(origProps).get(1);
        Map<String, String> hdfsParams = new HashMap<>();
        obsProperties.toHadoopConfiguration(hdfsParams);
        Configuration configuration = new Configuration(false);
        for (Map.Entry<String, String> entry : hdfsParams.entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        FileSystem.get(new URI(hdfsPath), configuration);

    }
}
