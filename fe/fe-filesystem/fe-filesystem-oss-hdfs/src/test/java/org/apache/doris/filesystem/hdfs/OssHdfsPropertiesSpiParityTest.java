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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.hdfs.properties.OssHdfsProperties;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.StorageKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Phase A1 acceptance: SPI-side OssHdfsProperties bound via
 * {@link OssHdfsFileSystemProvider#bind} must reproduce the fe-core golden outputs frozen
 * by fe-core's OSSHdfsPropertiesParityTest (A5). Expected maps copied from that oracle.
 */
class OssHdfsPropertiesSpiParityTest {

    private static final OssHdfsFileSystemProvider PROVIDER = new OssHdfsFileSystemProvider();

    private static void assertExactMap(Map<String, String> expected, Map<String, String> actual) {
        Assertions.assertEquals(new TreeMap<>(expected), new TreeMap<>(actual));
    }

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.hdfs.endpoint", "cn-hangzhou.oss-dls.aliyuncs.com");
        props.put("oss.hdfs.access_key", "myAk");
        props.put("oss.hdfs.secret_key", "mySk");
        props.put("uri", "oss://mybucket/warehouse/t");
        return props;
    }

    @Test
    void testIdentity() {
        OssHdfsProperties p = PROVIDER.bind(baseProps());
        // SPI-native name is OSS_HDFS; the fe-core-facing "OSSHDFS" (2.4-1) is a facade
        // mapping concern, not a provider concern.
        Assertions.assertEquals("OSS_HDFS", p.providerName());
        Assertions.assertEquals(StorageKind.HDFS_COMPATIBLE, p.kind());
        Assertions.assertEquals(FileSystemType.HDFS, p.type());
        Assertions.assertEquals(Set.of("oss", "hdfs"), p.getSupportedSchemes());
        Assertions.assertEquals(BackendStorageKind.HDFS, p.backendKind());
    }

    @Test
    void testBackendMapMatchesFeCoreGolden() {
        OssHdfsProperties p = PROVIDER.bind(baseProps());
        // fe-core oracle: OSSHdfsPropertiesParityTest.testBackendMapExact
        Map<String, String> golden = new HashMap<>();
        golden.put("fs.oss.endpoint", "cn-hangzhou.oss-dls.aliyuncs.com");
        golden.put("fs.oss.accessKeyId", "myAk");
        golden.put("fs.oss.accessKeySecret", "mySk");
        golden.put("fs.oss.region", "cn-hangzhou");
        golden.put("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
        golden.put("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.JindoOSS");
        golden.put("fs.defaultFS", "oss://mybucket");
        assertExactMap(golden, p.getBackendConfigProperties());
        assertExactMap(golden, p.toHadoopProperties().orElseThrow().toHadoopConfigurationMap());
        Assertions.assertFalse(p.isKerberos());
    }

    @Test
    void testDlfEndpointRewrittenMatchesFeCoreGolden() {
        // fe-core oracle: OSSHdfsPropertiesParityTest.testDeprecatedFlagRoutesAndDlfEndpointRewritten
        Map<String, String> props = new HashMap<>();
        props.put("oss.hdfs.enabled", "true");
        props.put("oss.endpoint", "dlf.cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "myAk");
        props.put("oss.secret_key", "mySk");
        OssHdfsProperties p = PROVIDER.bind(props);
        Assertions.assertEquals("cn-hangzhou.oss-dls.aliyuncs.com",
                p.getBackendConfigProperties().get("fs.oss.endpoint"));
        Assertions.assertEquals("cn-hangzhou",
                p.getBackendConfigProperties().get("fs.oss.region"));
    }

    @Test
    void testValidateUriOssSchemeOnly() {
        OssHdfsProperties p = PROVIDER.bind(baseProps());
        Assertions.assertEquals("oss://mybucket/x", p.validateAndNormalizeUri("oss://mybucket/x"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> p.validateAndNormalizeUri("hdfs://ns1/x"));
    }
}
