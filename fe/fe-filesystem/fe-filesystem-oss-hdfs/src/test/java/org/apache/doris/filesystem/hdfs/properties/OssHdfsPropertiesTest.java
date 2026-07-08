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

package org.apache.doris.filesystem.hdfs.properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class OssHdfsPropertiesTest {

    private Map<String, String> resolve(Map<String, String> raw) {
        OssHdfsProperties props = new OssHdfsProperties(raw);
        props.initNormalizeAndCheckProps();
        return props.getBackendConfigProperties();
    }

    private Map<String, String> baseProps() {
        Map<String, String> raw = new HashMap<>();
        raw.put("oss.hdfs.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        raw.put("oss.hdfs.access_key", "ak");
        raw.put("oss.hdfs.secret_key", "sk");
        return raw;
    }

    @Test
    void jindoImplAndCredentialsArePopulated() {
        Map<String, String> resolved = resolve(baseProps());

        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem",
                resolved.get("fs.oss.impl"));
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOSS",
                resolved.get("fs.AbstractFileSystem.oss.impl"));
        Assertions.assertEquals("ak", resolved.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("sk", resolved.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals("cn-beijing.oss-dls.aliyuncs.com", resolved.get("fs.oss.endpoint"));
    }

    @Test
    void regionIsExtractedFromOssDlsEndpoint() {
        Map<String, String> raw = baseProps();
        raw.put("oss.hdfs.endpoint", "cn-shanghai.oss-dls.aliyuncs.com");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("cn-shanghai", resolved.get("fs.oss.region"));
    }

    @Test
    void explicitRegionIsHonored() {
        Map<String, String> raw = baseProps();
        raw.put("oss.hdfs.region", "cn-hangzhou");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("cn-hangzhou", resolved.get("fs.oss.region"));
    }

    @Test
    void unparseableEndpointWithoutRegionThrows() {
        Map<String, String> raw = baseProps();
        raw.put("oss.hdfs.endpoint", "not-a-known-host.example.com");

        Assertions.assertThrows(IllegalArgumentException.class, () -> resolve(raw));
    }

    @Test
    void guessIsMeTrueForOssDlsEndpoint() {
        Map<String, String> raw = new HashMap<>();
        raw.put("oss.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        Assertions.assertTrue(OssHdfsProperties.guessIsMe(raw));
    }

    @Test
    void guessIsMeFalseForPlainOssEndpoint() {
        Map<String, String> raw = new HashMap<>();
        raw.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        Assertions.assertFalse(OssHdfsProperties.guessIsMe(raw));
    }
}
