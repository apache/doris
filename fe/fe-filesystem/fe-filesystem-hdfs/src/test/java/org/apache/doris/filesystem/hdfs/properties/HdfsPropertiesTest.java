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

package org.apache.doris.filesystem.hdfs.properties;

import org.apache.doris.filesystem.hdfs.HdfsConfigBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class HdfsPropertiesTest {

    private Map<String, String> resolve(Map<String, String> raw) {
        HdfsProperties props = new HdfsProperties(raw);
        props.initNormalizeAndCheckProps();
        return props.getBackendConfigProperties();
    }

    @Test
    void kerberosAuthParamsAreTranslatedToHadoopKeys() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "hdfs://ns");
        raw.put("hdfs.authentication.type", "kerberos");
        raw.put("hdfs.authentication.kerberos.principal", "doris/host@REALM");
        raw.put("hdfs.authentication.kerberos.keytab", "/etc/security/doris.keytab");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("kerberos", resolved.get("hadoop.security.authentication"));
        Assertions.assertEquals("doris/host@REALM", resolved.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("/etc/security/doris.keytab", resolved.get("hadoop.kerberos.keytab"));
        Assertions.assertTrue(HdfsConfigBuilder.isKerberosEnabled(resolved));
    }

    @Test
    void simpleAuthInjectsFallbackDefault() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "hdfs://ns");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("true", resolved.get("ipc.client.fallback-to-simple-auth-allowed"));
        Assertions.assertFalse(HdfsConfigBuilder.isKerberosEnabled(resolved));
    }

    @Test
    void ofsUriDerivesDefaultFsWhenNotExplicit() {
        // ofs is in HdfsFileSystemProvider.SUPPORTED_SCHEMES, so a bare ofs:// uri with no explicit
        // fs.defaultFS must still derive one; otherwise the backend config silently lacks fs.defaultFS.
        Map<String, String> raw = new HashMap<>();
        raw.put("uri", "ofs://cluster/path/to/file");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("ofs://cluster", resolved.get("fs.defaultFS"));
    }

    @Test
    void userOverriddenHadoopKeysArePreserved() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "hdfs://ns");
        raw.put("dfs.client.use.datanode.hostname", "true");
        raw.put("hadoop.rpc.protection", "privacy");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("true", resolved.get("dfs.client.use.datanode.hostname"));
        Assertions.assertEquals("privacy", resolved.get("hadoop.rpc.protection"));
    }

    @Test
    void xmlResourcesAreLoadedFromInjectedConfigDir(@TempDir Path tmp) throws Exception {
        Path site = tmp.resolve("my-hdfs-site.xml");
        Files.write(site, ("<?xml version=\"1.0\"?><configuration>"
                + "<property><name>dfs.custom.key</name><value>v1</value></property>"
                + "</configuration>").getBytes());

        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "hdfs://ns");
        raw.put("hadoop.config.resources", "my-hdfs-site.xml");
        raw.put("_HADOOP_CONFIG_DIR_", tmp.toString() + "/");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("v1", resolved.get("dfs.custom.key"));
    }

    @Test
    void xmlResourcesAreLoadedFromAbsolutePathWhenNoConfigDirInjected(@TempDir Path tmp) throws Exception {
        Path site = tmp.resolve("abs-hdfs-site.xml");
        Files.write(site, ("<?xml version=\"1.0\"?><configuration>"
                + "<property><name>dfs.custom.abs.key</name><value>v2</value></property>"
                + "</configuration>").getBytes());

        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "hdfs://ns");
        // No _HADOOP_CONFIG_DIR_ injected: the resource is given as a full absolute path
        // and must load as-is (configDir falls back to "").
        raw.put("hadoop.config.resources", site.toString());

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("v2", resolved.get("dfs.custom.abs.key"));
    }

    @Test
    void translationIsIdempotentOnAlreadyResolvedMap() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "hdfs://ns");
        raw.put("hdfs.authentication.type", "kerberos");
        raw.put("hdfs.authentication.kerberos.principal", "doris/host@REALM");
        raw.put("hdfs.authentication.kerberos.keytab", "/etc/security/doris.keytab");

        Map<String, String> once = resolve(raw);
        Map<String, String> twice = resolve(once);

        Assertions.assertEquals("kerberos", twice.get("hadoop.security.authentication"));
        Assertions.assertEquals("doris/host@REALM", twice.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("/etc/security/doris.keytab", twice.get("hadoop.kerberos.keytab"));
        Assertions.assertTrue(HdfsConfigBuilder.isKerberosEnabled(twice));
    }
}
