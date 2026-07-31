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

    /**
     * fe-core's StoragePropertiesConverter hands the plugin the normalized backend map
     * ({@code fs.oss.*} keys), not the raw user keys; rebinding it must yield the same config.
     */
    @Test
    void convertedBackendMapRoundTrips() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.oss.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        raw.put("fs.oss.accessKeyId", "ak");
        raw.put("fs.oss.accessKeySecret", "sk");
        raw.put("fs.oss.region", "cn-beijing");
        raw.put("_STORAGE_TYPE_", "OSS_HDFS");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("cn-beijing.oss-dls.aliyuncs.com", resolved.get("fs.oss.endpoint"));
        Assertions.assertEquals("ak", resolved.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("sk", resolved.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals("cn-beijing", resolved.get("fs.oss.region"));
    }

    /**
     * The converter map also carries entries the kernel loaded from XML config files (Jindo
     * tuning keys) plus {@code fs.defaultFS}; rebinding must pass them through instead of
     * rebuilding the config from the fixed credential/endpoint fields only.
     */
    @Test
    void convertedBackendMapPreservesPassThroughEntries() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.oss.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        raw.put("fs.oss.accessKeyId", "ak");
        raw.put("fs.oss.accessKeySecret", "sk");
        raw.put("fs.oss.region", "cn-beijing");
        raw.put("fs.oss.tmp.data.dirs", "/tmp/jindo");
        raw.put("fs.oss.upload.thread.concurrency", "16");
        raw.put("fs.defaultFS", "oss://bucket");
        raw.put("hadoop.username", "hive");
        raw.put("_STORAGE_TYPE_", "OSS_HDFS");
        raw.put("_HADOOP_CONFIG_DIR_", "/opt/hadoop/conf");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("/tmp/jindo", resolved.get("fs.oss.tmp.data.dirs"));
        Assertions.assertEquals("16", resolved.get("fs.oss.upload.thread.concurrency"));
        Assertions.assertEquals("oss://bucket", resolved.get("fs.defaultFS"));
        Assertions.assertEquals("hive", resolved.get("hadoop.username"));
        // System-injected markers are not Hadoop config and must not leak through.
        Assertions.assertFalse(resolved.containsKey("_STORAGE_TYPE_"));
        Assertions.assertFalse(resolved.containsKey("_HADOOP_CONFIG_DIR_"));
        // Derived keys still win over pass-through duplicates.
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem",
                resolved.get("fs.oss.impl"));
        Assertions.assertEquals("ak", resolved.get("fs.oss.accessKeyId"));
    }

    @Test
    void convertedSecurityTokenRoundTrips() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.oss.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        raw.put("fs.oss.accessKeyId", "ak");
        raw.put("fs.oss.accessKeySecret", "sk");
        raw.put("fs.oss.securityToken", "token");

        Assertions.assertEquals("token", resolve(raw).get("fs.oss.securityToken"));
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
