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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.security.authentication.HadoopKerberosAuthenticator;
import org.apache.doris.common.security.authentication.HadoopSimpleAuthenticator;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Phase A5 golden tests freezing current fe-core HdfsProperties behaviour. */
public class HdfsPropertiesParityTest {

    @Test
    public void testIdentity() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Assertions.assertTrue(sp instanceof HdfsProperties);
        Assertions.assertEquals(StorageProperties.Type.HDFS, sp.getType());
        Assertions.assertEquals("HDFS", sp.getStorageName());
        // schemas() (used by ensureDisableCache) is {hdfs} only — NOT {hdfs,viewfs,jfs},
        // which is a different set (HdfsProperties.supportSchema, used for uri validation).
        Assertions.assertEquals(ImmutableSet.of("hdfs"), ((HdfsProperties) sp).schemas());
        Assertions.assertTrue(((HdfsProperties) sp).isExplicitlyConfigured());
    }

    @Test
    public void testSimpleAuthBackendMapExact() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        props.put("hadoop.username", "hadoop");
        StorageProperties sp = StorageProperties.createPrimary(props);
        // Note: user keys with hadoop./dfs./fs./juicefs. prefixes are copied into the
        // backend map verbatim — including the fs.hdfs.support routing flag itself.
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "fs.hdfs.support", "true",
                "fs.defaultFS", "hdfs://nameservice1",
                "ipc.client.fallback-to-simple-auth-allowed", "true",
                "hdfs.security.authentication", "simple",
                "hadoop.username", "hadoop"
        ), sp.getBackendConfigProperties());
        Assertions.assertTrue(((HdfsProperties) sp).getHadoopAuthenticator()
                instanceof HadoopSimpleAuthenticator);
        Assertions.assertFalse(((HdfsProperties) sp).isKerberos());
    }

    @Test
    public void testKerberosBackendMapExact() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        props.put("hdfs.authentication.type", "kerberos");
        props.put("hadoop.kerberos.principal", "doris/_HOST@EXAMPLE.COM");
        props.put("hadoop.kerberos.keytab", "/etc/doris/doris.keytab");
        StorageProperties sp = StorageProperties.createPrimary(props);
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "fs.hdfs.support", "true",
                "fs.defaultFS", "hdfs://nameservice1",
                "ipc.client.fallback-to-simple-auth-allowed", "true",
                "hdfs.security.authentication", "kerberos",
                "hadoop.security.authentication", "kerberos",
                "hadoop.kerberos.principal", "doris/_HOST@EXAMPLE.COM",
                "hadoop.kerberos.keytab", "/etc/doris/doris.keytab"
        ), sp.getBackendConfigProperties());
        Assertions.assertTrue(((HdfsProperties) sp).isKerberos());
        Assertions.assertTrue(((HdfsProperties) sp).getHadoopAuthenticator()
                instanceof HadoopKerberosAuthenticator);
    }

    @Test
    public void testKerberosWithoutKeytabThrows() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        props.put("hdfs.authentication.type", "kerberos");
        props.put("hadoop.kerberos.principal", "doris/_HOST@EXAMPLE.COM");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageProperties.createPrimary(props));
    }

    @Test
    public void testDisableCacheOnlyHdfsScheme() {
        // 2.4-6 ledger check: ensureDisableCache iterates schemas() == {hdfs}.
        // viewfs/jfs disable-cache keys are NOT set by fe-core today.
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://nameservice1/path/f.orc");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Configuration conf = sp.getHadoopStorageConfig();
        Assertions.assertEquals("true", conf.get("fs.hdfs.impl.disable.cache"));
        ParityAsserts.assertConfLacks(conf,
                "fs.viewfs.impl.disable.cache", "fs.jfs.impl.disable.cache");
    }

    @Test
    public void testUserHdfsSiteKeysCopiedToBackend() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.hdfs.support", "true");
        props.put("fs.defaultFS", "hdfs://ns1");
        props.put("dfs.nameservices", "ns1");
        props.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        props.put("dfs.namenode.rpc-address.ns1.nn1", "host1:8020");
        props.put("dfs.namenode.rpc-address.ns1.nn2", "host2:8020");
        props.put("dfs.client.failover.proxy.provider.ns1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Map<String, String> backend = sp.getBackendConfigProperties();
        Assertions.assertEquals("ns1", backend.get("dfs.nameservices"));
        Assertions.assertEquals("nn1,nn2", backend.get("dfs.ha.namenodes.ns1"));
        Assertions.assertEquals("hdfs://ns1", backend.get("fs.defaultFS"));
    }
}
