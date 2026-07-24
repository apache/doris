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

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Phase A5 golden tests freezing current fe-core OSSHdfsProperties behaviour. */
public class OSSHdfsPropertiesParityTest {

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.hdfs.endpoint", "cn-hangzhou.oss-dls.aliyuncs.com");
        props.put("oss.hdfs.access_key", "myAk");
        props.put("oss.hdfs.secret_key", "mySk");
        props.put("uri", "oss://mybucket/warehouse/t");
        return props;
    }

    @Test
    public void testIdentity() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        Assertions.assertTrue(sp instanceof OSSHdfsProperties);
        Assertions.assertEquals(StorageProperties.Type.OSS_HDFS, sp.getType());
        // 2.4-1: "OSSHDFS", no underscore — LocationPath/BrokerDesc depend on this.
        Assertions.assertEquals("OSSHDFS", sp.getStorageName());
        Assertions.assertEquals(ImmutableSet.of("hdfs"), ((OSSHdfsProperties) sp).schemas());
    }

    @Test
    public void testBackendMapExact() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "fs.oss.endpoint", "cn-hangzhou.oss-dls.aliyuncs.com",
                "fs.oss.accessKeyId", "myAk",
                "fs.oss.accessKeySecret", "mySk",
                "fs.oss.region", "cn-hangzhou",
                "fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem",
                "fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.JindoOSS",
                "fs.defaultFS", "oss://mybucket"
        ), sp.getBackendConfigProperties());
    }

    @Test
    public void testDeprecatedFlagRoutesAndDlfEndpointRewritten() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.hdfs.enabled", "true");
        props.put("oss.endpoint", "dlf.cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "myAk");
        props.put("oss.secret_key", "mySk");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Assertions.assertTrue(sp instanceof OSSHdfsProperties);
        // dlf endpoint is rewritten to <region>.oss-dls.aliyuncs.com
        Assertions.assertEquals("cn-hangzhou.oss-dls.aliyuncs.com",
                sp.getBackendConfigProperties().get("fs.oss.endpoint"));
        Assertions.assertEquals("cn-hangzhou",
                sp.getBackendConfigProperties().get("fs.oss.region"));
    }
}
