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
 * Plain HDFS must own only hdfs/viewfs. jfs:// (fe-core marker "HDFS") belongs to the jfs plugin,
 * oss:// / OSS_HDFS marker to the oss-hdfs plugin. Routing is first-match-wins over an unordered
 * provider list, so these predicates must stay disjoint.
 */
class HdfsFileSystemProviderTest {

    private final HdfsFileSystemProvider provider = new HdfsFileSystemProvider();

    private Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void claimsHdfsAndViewfsSchemes() {
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "hdfs://ns")));
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "viewfs://cluster")));
    }

    @Test
    void claimsHdfsMarkerWithoutUri() {
        Assertions.assertTrue(provider.supports(props("_STORAGE_TYPE_", "HDFS")));
    }

    @Test
    void rejectsJfsScheme() {
        // jfs carries fe-core marker "HDFS" but belongs to the jfs plugin.
        Assertions.assertFalse(provider.supports(props("_STORAGE_TYPE_", "HDFS",
                "fs.defaultFS", "jfs://cluster")));
    }

    @Test
    void rejectsOssSchemeAndOssHdfsMarker() {
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "oss://bucket/p")));
        Assertions.assertFalse(provider.supports(props("_STORAGE_TYPE_", "OSS_HDFS")));
    }

    @Test
    void rejectsOfsScheme() {
        // ofs (Tencent CHDFS) is broker-routed by fe-core, never claimed by this SPI provider.
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "ofs://cluster/p")));
    }

    @Test
    void schemeMatchIsCaseInsensitive() {
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "HDFS://ns")));
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "ViewFS://cluster")));
    }
}
