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

/**
 * Routing must be strictly disjoint: for any configuration, at most one of
 * {@link HdfsFileSystemProvider} / {@link OssHdfsFileSystemProvider} claims it. The framework
 * picks providers first-match-wins over an unordered list, so overlap would make selection depend
 * on registration order.
 */
class HdfsProviderRoutingTest {

    private final HdfsFileSystemProvider hdfs = new HdfsFileSystemProvider();
    private final OssHdfsFileSystemProvider ossHdfs = new OssHdfsFileSystemProvider();

    private void assertHdfsOnly(Map<String, String> props) {
        Assertions.assertTrue(hdfs.supports(props), "expected HDFS to claim: " + props);
        Assertions.assertFalse(ossHdfs.supports(props), "expected OSS-HDFS NOT to claim: " + props);
    }

    private void assertOssHdfsOnly(Map<String, String> props) {
        Assertions.assertTrue(ossHdfs.supports(props), "expected OSS-HDFS to claim: " + props);
        Assertions.assertFalse(hdfs.supports(props), "expected HDFS NOT to claim: " + props);
    }

    @Test
    void hdfsStorageTypeMarkerGoesToHdfsOnly() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "HDFS");
        assertHdfsOnly(props);
    }

    @Test
    void ossHdfsStorageTypeMarkerGoesToOssHdfsOnly() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "OSS_HDFS");
        assertOssHdfsOnly(props);
    }

    @Test
    void hdfsUriGoesToHdfsOnly() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "hdfs://ns");
        assertHdfsOnly(props);
    }

    @Test
    void ossDlsEndpointWithoutMarkerGoesToOssHdfsOnly() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        assertOssHdfsOnly(props);
    }

    @Test
    void bareOssUriWithoutMarkerGoesToOssHdfsOnly() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "oss://bucket/path");
        assertOssHdfsOnly(props);
    }

    @Test
    void unrelatedConfigClaimedByNeither() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "s3://bucket/key");
        Assertions.assertFalse(hdfs.supports(props));
        Assertions.assertFalse(ossHdfs.supports(props));
    }
}
