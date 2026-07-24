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
import org.apache.doris.filesystem.hdfs.properties.HdfsProperties;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.StorageKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Phase A1 acceptance: the SPI-side HdfsProperties bound via {@link HdfsFileSystemProvider#bind}
 * must reproduce the fe-core golden outputs frozen by fe-core's HdfsPropertiesParityTest (A5).
 * Expected maps below are copied verbatim from that oracle.
 */
class HdfsPropertiesSpiParityTest {

    private static final HdfsFileSystemProvider PROVIDER = new HdfsFileSystemProvider();

    private static void assertExactMap(Map<String, String> expected, Map<String, String> actual) {
        Assertions.assertEquals(new TreeMap<>(expected), new TreeMap<>(actual));
    }

    @Test
    void testIdentity() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        HdfsProperties p = PROVIDER.bind(props);
        Assertions.assertEquals("HDFS", p.providerName());
        Assertions.assertEquals(StorageKind.HDFS_COMPATIBLE, p.kind());
        Assertions.assertEquals(FileSystemType.HDFS, p.type());
        Assertions.assertEquals(Set.of("hdfs", "viewfs"), p.getSupportedSchemes());
        Assertions.assertEquals(BackendStorageKind.HDFS, p.backendKind());
        Assertions.assertSame(p, p.toBackendProperties().orElseThrow());
        Assertions.assertSame(p, p.toHadoopProperties().orElseThrow());
        Assertions.assertEquals(props, p.rawProperties());
    }

    @Test
    void testSimpleAuthBackendMapMatchesFeCoreGolden() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        props.put("hadoop.username", "hadoop");
        HdfsProperties p = PROVIDER.bind(props);
        // fe-core oracle: HdfsPropertiesParityTest.testSimpleAuthBackendMapExact
        Map<String, String> golden = new HashMap<>();
        golden.put("fs.hdfs.support", "true");
        golden.put("fs.defaultFS", "hdfs://nameservice1");
        golden.put("ipc.client.fallback-to-simple-auth-allowed", "true");
        golden.put("hdfs.security.authentication", "simple");
        golden.put("hadoop.username", "hadoop");
        assertExactMap(golden, p.getBackendConfigProperties());
        assertExactMap(golden, p.toBackendProperties().orElseThrow().toMap());
        // HDFS family: hadoop configuration map == backend map (generic fs.* passthrough
        // and disable-cache orchestration are the facade's job).
        assertExactMap(golden, p.toHadoopProperties().orElseThrow().toHadoopConfigurationMap());
        Assertions.assertFalse(p.isKerberos());
        Assertions.assertTrue(p.getExecutionAuthenticator() instanceof SimpleHadoopAuthenticator);
    }

    @Test
    void testKerberosBackendMapMatchesFeCoreGolden() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        props.put("hdfs.authentication.type", "kerberos");
        props.put("hadoop.kerberos.principal", "doris/_HOST@EXAMPLE.COM");
        props.put("hadoop.kerberos.keytab", "/etc/doris/doris.keytab");
        HdfsProperties p = PROVIDER.bind(props);
        // fe-core oracle: HdfsPropertiesParityTest.testKerberosBackendMapExact
        Map<String, String> golden = new HashMap<>();
        golden.put("fs.hdfs.support", "true");
        golden.put("fs.defaultFS", "hdfs://nameservice1");
        golden.put("ipc.client.fallback-to-simple-auth-allowed", "true");
        golden.put("hdfs.security.authentication", "kerberos");
        golden.put("hadoop.security.authentication", "kerberos");
        golden.put("hadoop.kerberos.principal", "doris/_HOST@EXAMPLE.COM");
        golden.put("hadoop.kerberos.keytab", "/etc/doris/doris.keytab");
        assertExactMap(golden, p.getBackendConfigProperties());
        Assertions.assertTrue(p.isKerberos());
        // Deliberately NOT calling getExecutionAuthenticator(): Kerberos construction
        // performs the JAAS login, which must stay lazy (no KDC in unit tests).
    }

    @Test
    void testKerberosWithoutKeytabThrows() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        props.put("hdfs.authentication.type", "kerberos");
        props.put("hadoop.kerberos.principal", "doris/_HOST@EXAMPLE.COM");
        Assertions.assertThrows(IllegalArgumentException.class, () -> PROVIDER.bind(props));
    }

    @Test
    void testHaKeysCopiedToBackendMap() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("fs.defaultFS", "hdfs://ns1");
        props.put("dfs.nameservices", "ns1");
        props.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        props.put("dfs.namenode.rpc-address.ns1.nn1", "host1:8020");
        props.put("dfs.namenode.rpc-address.ns1.nn2", "host2:8020");
        props.put("dfs.client.failover.proxy.provider.ns1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        HdfsProperties p = PROVIDER.bind(props);
        Map<String, String> backend = p.getBackendConfigProperties();
        Assertions.assertEquals("ns1", backend.get("dfs.nameservices"));
        Assertions.assertEquals("nn1,nn2", backend.get("dfs.ha.namenodes.ns1"));
        Assertions.assertEquals("hdfs://ns1", backend.get("fs.defaultFS"));
    }

    @Test
    void testValidateUri() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        HdfsProperties p = PROVIDER.bind(props);
        Assertions.assertEquals("hdfs://nameservice1/path/f.orc",
                p.validateAndNormalizeUri("hdfs://nameservice1/path/f.orc"));
    }
}
