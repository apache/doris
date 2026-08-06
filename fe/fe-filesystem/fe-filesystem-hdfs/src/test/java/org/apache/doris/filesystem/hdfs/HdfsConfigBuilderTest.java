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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class HdfsConfigBuilderTest {

    @Test
    void buildSetsDisableCache() {
        Configuration conf = HdfsConfigBuilder.build(Map.of());
        Assertions.assertTrue(conf.getBoolean("fs.hdfs.impl.disable.cache", false));
        Assertions.assertTrue(conf.getBoolean("fs.AbstractFileSystem.hdfs.impl.disable.cache", false));
    }

    @Test
    void build_disablesCacheForAllServedSchemes() {
        Configuration conf = HdfsConfigBuilder.build(Map.of());
        for (String scheme : HdfsConfigBuilder.CACHE_DISABLE_SCHEMES) {
            Assertions.assertTrue(
                    conf.getBoolean("fs." + scheme + ".impl.disable.cache", false),
                    "fs." + scheme + ".impl.disable.cache should be true");
            Assertions.assertTrue(
                    conf.getBoolean("fs.AbstractFileSystem." + scheme + ".impl.disable.cache", false),
                    "fs.AbstractFileSystem." + scheme + ".impl.disable.cache should be true");
        }
    }

    @Test
    void build_disablesCacheForOssHdfsScheme() {
        // oss is served by OSS-HDFS (JindoFS) via DFSFileSystem, so its FS cache must be disabled
        // even though oss:// routing lives in OssHdfsFileSystemProvider, not SUPPORTED_SCHEMES.
        Configuration conf = HdfsConfigBuilder.build(Map.of());
        Assertions.assertTrue(conf.getBoolean("fs.oss.impl.disable.cache", false));
        Assertions.assertTrue(conf.getBoolean("fs.AbstractFileSystem.oss.impl.disable.cache", false));
    }

    @Test
    void buildSetsProvidedProperties() {
        Configuration conf = HdfsConfigBuilder.build(Map.of(
                "fs.defaultFS", "hdfs://namenode:8020",
                "dfs.replication", "3"
        ));
        Assertions.assertEquals("hdfs://namenode:8020", conf.get("fs.defaultFS"));
        Assertions.assertEquals("3", conf.get("dfs.replication"));
    }

    @Test
    void buildIgnoresNullAndEmptyValues() {
        Map<String, String> props = new java.util.HashMap<>();
        props.put("good.key", "good.value");
        props.put("empty.key", "");
        props.put("null.key", null);

        Configuration conf = HdfsConfigBuilder.build(props);
        Assertions.assertEquals("good.value", conf.get("good.key"));
        // empty and null values should not be set
        Assertions.assertFalse(conf.iterator().hasNext()
                && "empty.key".equals(conf.get("empty.key")));
    }

    @Test
    void buildPinsPluginClassLoaderNotTccl() {
        // WHY (test_string_dict_filter, hdfs scan): Hadoop resolves impl classes via Configuration.getClass,
        // which uses the conf's OWN classLoader field. new HdfsConfiguration() captures the thread-context CL
        // active AT CONSTRUCTION into that field. DFSFileSystem is built under a connector's plugin loader
        // during a scan, so unpinned the conf would carry that connector loader; then RPC.getProtocolEngine
        // loads ProtobufRpcEngine2 from the connector's hadoop-common copy while RPC/RpcEngine come from the
        // engine copy -> "class ProtobufRpcEngine2 cannot be cast to class RpcEngine". The conf MUST be pinned
        // to this plugin's loader. MUTATION: drop the setClassLoader in build() -> the conf keeps the foreign
        // TCCL below -> red. (A flat-classpath assertion alone cannot repro the real cross-loader cast, so we
        // install a distinct TCCL to make the captured-loader bug observable offline.)
        ClassLoader foreign = new java.net.URLClassLoader(new java.net.URL[0], null);
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(foreign);
            Configuration conf = HdfsConfigBuilder.build(Map.of());
            Assertions.assertSame(HdfsConfigBuilder.class.getClassLoader(), conf.getClassLoader());
            Assertions.assertNotSame(foreign, conf.getClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    @Test
    void isKerberosEnabledBothPresent() {
        Assertions.assertTrue(HdfsConfigBuilder.isKerberosEnabled(Map.of(
                "hadoop.kerberos.principal", "doris@REALM",
                "hadoop.kerberos.keytab", "/path/to/keytab"
        )));
    }

    @Test
    void isKerberosEnabledMissingPrincipal() {
        Assertions.assertFalse(HdfsConfigBuilder.isKerberosEnabled(Map.of(
                "hadoop.kerberos.keytab", "/path/to/keytab"
        )));
    }

    @Test
    void isKerberosEnabledMissingKeytab() {
        Assertions.assertFalse(HdfsConfigBuilder.isKerberosEnabled(Map.of(
                "hadoop.kerberos.principal", "doris@REALM"
        )));
    }

    @Test
    void isKerberosEnabledNeitherPresent() {
        Assertions.assertFalse(HdfsConfigBuilder.isKerberosEnabled(Map.of()));
    }
}
