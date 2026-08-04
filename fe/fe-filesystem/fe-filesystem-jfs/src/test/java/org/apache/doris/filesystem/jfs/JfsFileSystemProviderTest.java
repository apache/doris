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

package org.apache.doris.filesystem.jfs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class JfsFileSystemProviderTest {

    private final JfsFileSystemProvider provider = new JfsFileSystemProvider();

    private Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void claimsJfsScheme() {
        Assertions.assertTrue(provider.supports(props("fs.defaultFS", "jfs://myvol")));
    }

    @Test
    void rejectsOtherSchemesAndMarkers() {
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "hdfs://ns")));
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "viewfs://c")));
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "oss://bucket/p")));
        Assertions.assertFalse(provider.supports(props("_STORAGE_TYPE_", "HDFS")));
    }

    @Test
    void claimsJfsViaHdfsUriKey() {
        // fe-core injects the connection URI under HDFS_URI; supports() must honor that key too.
        Assertions.assertTrue(provider.supports(props("HDFS_URI", "jfs://myvol")));
    }

    @Test
    void rejectsUriWithoutScheme() {
        Assertions.assertFalse(provider.supports(props("fs.defaultFS", "myvol")));
    }
}
