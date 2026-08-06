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

package org.apache.doris.filesystem.oss;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Native-OSS routing must stay disjoint from OSS-HDFS (JindoFS). OSS-HDFS backend config carries
 * {@code _STORAGE_TYPE_=OSS_HDFS} and an {@code fs.oss.endpoint} on {@code *.oss-dls.aliyuncs.com};
 * that endpoint contains {@code aliyuncs.com}, so the plain endpoint heuristic would otherwise make
 * this native-OSS provider claim an OSS-HDFS config too. Since providers are picked first-match-wins
 * over an unordered list, that overlap would make selection depend on registration order.
 */
class OssFileSystemProviderTest {

    private final OssFileSystemProvider provider = new OssFileSystemProvider();

    @Test
    void ossHdfsMarkerIsNotClaimedEvenWithAliyunEndpoint() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "OSS_HDFS");
        props.put("fs.oss.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        Assertions.assertFalse(provider.supports(props),
                "native OSS must not claim an OSS-HDFS config: " + props);
    }

    @Test
    void ossDlsEndpointWithoutMarkerIsNotClaimed() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.oss.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        Assertions.assertFalse(provider.supports(props),
                "oss-dls endpoint belongs to OSS-HDFS, not native OSS: " + props);
    }

    @Test
    void nativeOssMarkerIsClaimed() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "OSS");
        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void plainAliyunEndpointIsClaimed() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void explicitOssHdfsFlagsAreNotClaimed() {
        // StorageProperties.createPrimary treats these flags as OSS-HDFS before native OSS,
        // even when the endpoint is a plain (non-dls) Aliyun endpoint.
        Map<String, String> props = new HashMap<>();
        props.put("fs.oss-hdfs.support", "true");
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        Assertions.assertFalse(provider.supports(props),
                "explicit OSS-HDFS flag belongs to OssHdfsFileSystemProvider: " + props);

        Map<String, String> deprecated = new HashMap<>();
        deprecated.put("oss.hdfs.enabled", "true");
        deprecated.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        Assertions.assertFalse(provider.supports(deprecated),
                "deprecated OSS-HDFS flag belongs to OssHdfsFileSystemProvider: " + deprecated);
    }

    @Test
    void ossHdfsFlagWinsOverNativeOssFlag() {
        // Kernel precedence: OSS-HDFS flags are checked before fs.oss.support.
        Map<String, String> props = new HashMap<>();
        props.put("fs.oss-hdfs.support", "true");
        props.put("fs.oss.support", "true");
        Assertions.assertFalse(provider.supports(props));
    }
}
