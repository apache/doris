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

import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class OSSHdfsPropertiesTest {
    @Test
    public void testValidOSSConfiguration() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "cn-shanghai.oss-dls.aliyuncs.com");
        origProps.put("oss.access_key", "testAccessKey");
        origProps.put("oss.secret_key", "testSecretKey");
        origProps.put("oss.region", "cn-shanghai");
        origProps.put("uri", "oss://my-bucket/path");

        StorageProperties props = StorageProperties.createPrimary(origProps);
        Assertions.assertInstanceOf(OSSHdfsProperties.class, props);

        Configuration conf = ((OSSHdfsProperties) props).getHadoopConfiguration();
        Assertions.assertEquals("cn-shanghai.oss-dls.aliyuncs.com", conf.get("fs.oss.endpoint"));
        Assertions.assertEquals("testAccessKey", conf.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("testSecretKey", conf.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals("cn-shanghai", conf.get("fs.oss.region"));
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem", conf.get("fs.oss.impl"));
    }

    @Test
    public void testReadConfigFromFile() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "cn-shanghai.oss-dls.aliyuncs.com");
        origProps.put("oss.access_key", "testAccessKey");
        origProps.put("oss.secret_key", "testSecretKey");
        origProps.put("oss.region", "cn-shanghai");
        origProps.put("uri", "oss://my-bucket/path");
        origProps.put("hadoop.config.resources", "hdfs-site.xml,core-site.xml");
        URL hdfsFileUrl = OSSHdfsPropertiesTest.class.getClassLoader().getResource("plugins");
        Config.hadoop_config_dir = hdfsFileUrl.getPath().toString() + "/hadoop_conf/";
        origProps.put("hadoop.config.resources", "osshdfs1/core-site.xml");
        StorageProperties props = StorageProperties.createPrimary(origProps);
        Assertions.assertInstanceOf(OSSHdfsProperties.class, props);
        Configuration conf = ((OSSHdfsProperties) props).getHadoopConfiguration();
        Assertions.assertEquals("cn-shanghai.oss-dls.aliyuncs.com", conf.get("fs.oss.endpoint"));
        Assertions.assertEquals("testAccessKey", conf.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("testSecretKey", conf.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals("cn-shanghai", conf.get("fs.oss.region"));
        Assertions.assertEquals("test", conf.get("fs.oss.test.key"));
        Map<String, String> backendConfigProperties = props.getBackendConfigProperties();
        Assertions.assertEquals("test", backendConfigProperties.get("fs.oss.test.key"));
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem", backendConfigProperties.get("fs.oss.impl"));
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOSS", backendConfigProperties.get("fs.AbstractFileSystem.oss.impl"));
        Assertions.assertEquals("cn-shanghai.oss-dls.aliyuncs.com", backendConfigProperties.get("fs.oss.endpoint"));
        Assertions.assertEquals("testAccessKey", backendConfigProperties.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("testSecretKey", backendConfigProperties.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals("cn-shanghai", backendConfigProperties.get("fs.oss.region"));
    }

    @Test
    public void testInvalidEndpoint() {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "invalid.aliyuncs.com");
        origProps.put("oss.access_key", "testAccessKey");
        origProps.put("oss.secret_key", "testSecretKey");
        origProps.put("oss.region", "cn-shanghai");
        Assertions.assertThrows(RuntimeException.class, () -> {
            StorageProperties.createPrimary(origProps);
        });
        origProps.put("oss.endpoint", "cn-shanghai.oss-dls.aliyuncs.com");
        Assertions.assertDoesNotThrow(() -> {
            StorageProperties.createPrimary(origProps);
        });
    }

    @Test
    public void testValidateUri() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("uri", "oss://my-bucket/path");
        OSSHdfsProperties props = new OSSHdfsProperties(origProps);
        Assertions.assertEquals("oss://bucket/path", props.validateAndNormalizeUri("oss://bucket/path"));
        Assertions.assertThrows(UserException.class, () -> props.validateAndNormalizeUri("hdfs://bucket"));
        Assertions.assertThrows(UserException.class, () -> props.validateAndNormalizeUri(""));
        Assertions.assertThrows(UserException.class, () -> props.validateAndNormalizeUri("test"));
        Assertions.assertEquals("oss://bucket/path", props.validateAndNormalizeUri("oss://bucket/path"));
        Assertions.assertEquals("oss://my-bucket/path", props.validateAndGetUri(origProps));
    }

    @Test
    public void testGetStorageName() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "cn-shanghai.oss-dls.aliyuncs.com");
        origProps.put("oss.access_key", "testAccessKey");
        origProps.put("oss.secret_key", "testSecretKey");
        origProps.put("oss.region", "cn-shanghai");

        OSSHdfsProperties props = (OSSHdfsProperties) StorageProperties.createAll(origProps).get(0);
        Assertions.assertEquals("HDFS", props.getStorageName());
    }

}
