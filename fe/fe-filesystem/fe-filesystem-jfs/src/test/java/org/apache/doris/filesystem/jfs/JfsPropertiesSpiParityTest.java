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

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.jfs.properties.JfsProperties;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.StorageKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Phase A1 acceptance for JuiceFS typed properties bound via {@link JfsFileSystemProvider#bind}.
 * fe-core has no dedicated JFS type (jfs rides HdfsProperties there); the golden facts here are
 * the jfs-specific derivations this plugin owns.
 */
class JfsPropertiesSpiParityTest {

    private static final JfsFileSystemProvider PROVIDER = new JfsFileSystemProvider();

    private static void assertExactMap(Map<String, String> expected, Map<String, String> actual) {
        Assertions.assertEquals(new TreeMap<>(expected), new TreeMap<>(actual));
    }

    @Test
    void testIdentityAndBackendMap() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "jfs://myjfs/warehouse/t");
        props.put("hadoop.username", "hadoop");
        props.put("juicefs.meta", "redis://127.0.0.1:6379/1");
        JfsProperties p = PROVIDER.bind(props);

        Assertions.assertEquals("JFS", p.providerName());
        Assertions.assertEquals(StorageKind.HDFS_COMPATIBLE, p.kind());
        Assertions.assertEquals(FileSystemType.JFS, p.type());
        Assertions.assertEquals(Set.of("jfs"), p.getSupportedSchemes());
        Assertions.assertEquals(BackendStorageKind.HDFS, p.backendKind());

        Map<String, String> golden = new HashMap<>();
        golden.put("fs.defaultFS", "jfs://myjfs");
        golden.put("hadoop.username", "hadoop");
        golden.put("juicefs.meta", "redis://127.0.0.1:6379/1");
        assertExactMap(golden, p.getBackendConfigProperties());
        assertExactMap(golden, p.toHadoopProperties().orElseThrow().toHadoopConfigurationMap());
        Assertions.assertFalse(p.isKerberos());
    }

    @Test
    void testValidateUriJfsSchemeOnly() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "jfs://myjfs/warehouse/t");
        JfsProperties p = PROVIDER.bind(props);
        Assertions.assertEquals("jfs://myjfs/x", p.validateAndNormalizeUri("jfs://myjfs/x"));
        Assertions.assertThrows(Exception.class, () -> p.validateAndNormalizeUri("oss://b/x"));
    }
}
