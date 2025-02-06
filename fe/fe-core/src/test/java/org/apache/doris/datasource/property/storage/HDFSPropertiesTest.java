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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;

public class HDFSPropertiesTest {


    @Test
    public void testBasicHdfsCreate() {
        Map<String, String> origProps = Maps.newHashMap();
        origProps.put(StorageProperties.FS_HDFS_SUPPORT, "true");
        //tobe fixed
        /*
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            StorageProperties.create(origProps);
        },"Property hadoop.username is required.");
        origProps.put("hadoop.username", "hadoop");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            StorageProperties.create(origProps);
        },"Property hadoop.config.resources is required.");
        origProps.put("hadoop.config.resources", "/hadoop-test/");
        */
        List<StorageProperties> storageProperties = StorageProperties.create(origProps);
        HDFSProperties hdfsProperties = (HDFSProperties) storageProperties.get(0);
        Configuration conf = new Configuration();
        hdfsProperties.toHadoopConfiguration(conf);
        Assertions.assertEquals("simple", conf.get("hadoop.security.authentication"));
    }

    @Test
    public void testBasicS3Create() {
        Map<String, String> origProps = Maps.newHashMap();
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        //fixme s3 properties don't need hadoop.config.resources and hadoop.username
        origProps.put("hadoop.config.resources", "/hadoop-test/");
        origProps.put("hadoop.username", "hadoop");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            StorageProperties.create(origProps);
        }, "Property s3.access_key is required.");
        // s3 properties
        origProps.put("s3.access_key", "access_key");
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> {
            StorageProperties.create(origProps);
        }, "Property s3.secret.key is required.");
        origProps.put("s3.secret_key", "secret_key");
        S3Properties s3Properties = (S3Properties) StorageProperties.create(origProps).get(1);
        s3Properties.toBackendS3ClientProperties(origProps);
    }

}
