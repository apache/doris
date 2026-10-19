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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class OssHdfsFileSystemProviderTest {

    private final OssHdfsFileSystemProvider provider = new OssHdfsFileSystemProvider();

    private Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void claimsOssHdfsMarkerAndOssScheme() {
        Assertions.assertTrue(provider.supports(props("_STORAGE_TYPE_", "OSS_HDFS")));
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "oss://bucket/p")));
    }

    @Test
    void claimsOssDlsEndpointWithoutMarker() {
        Assertions.assertTrue(provider.supports(props("oss.endpoint", "cn-beijing.oss-dls.aliyuncs.com")));
    }

    @Test
    void rejectsHdfsAndJfsAndHdfsMarker() {
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "hdfs://ns")));
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "jfs://cluster")));
        Assertions.assertFalse(provider.supports(props("_STORAGE_TYPE_", "HDFS")));
    }

    @Test
    void rejectsNativeOssMarker() {
        // A native-OSS (S3-compatible) marker belongs to OssFileSystemProvider, never here.
        Assertions.assertFalse(provider.supports(props("_STORAGE_TYPE_", "OSS")));
    }

    @Test
    void claimsExplicitOssHdfsFlagsLikeKernel() {
        // StorageProperties.createPrimary treats these flags as OSS-HDFS before native OSS,
        // even with a plain (non-dls) Aliyun endpoint.
        Assertions.assertTrue(provider.supports(props(
                "fs.oss-hdfs.support", "true", "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com")));
        Assertions.assertTrue(provider.supports(props(
                "oss.hdfs.enabled", "true", "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com")));
    }

    @Test
    void yieldsExplicitNativeOssEvenWithOssUri() {
        // fs.oss.support=true declares native OSS; the bare oss:// URI fallback must not steal it.
        Assertions.assertFalse(provider.supports(props(
                "fs.oss.support", "true", "fs.defaultFS", "oss://bucket/p")));
    }

    @Test
    void ossHdfsFlagsWinOverNativeOssFlag() {
        // Kernel precedence: the OSS-HDFS flags are checked before fs.oss.support.
        Assertions.assertTrue(provider.supports(props(
                "fs.oss-hdfs.support", "true", "fs.oss.support", "true")));
    }

    @Test
    void sensitivePropertyKeysCoverSecretsButNotEndpoint() {
        java.util.Set<String> keys = provider.sensitivePropertyKeys();
        Assertions.assertTrue(keys.contains("oss.hdfs.access_key"));
        Assertions.assertTrue(keys.contains("oss.hdfs.secret_key"));
        Assertions.assertTrue(keys.contains("oss.hdfs.security_token"));
        Assertions.assertTrue(keys.contains("oss.security_token"));
        Assertions.assertTrue(keys.contains("fs.oss.accessKeySecret"));
        Assertions.assertFalse(keys.contains("oss.hdfs.endpoint"));
    }
}
