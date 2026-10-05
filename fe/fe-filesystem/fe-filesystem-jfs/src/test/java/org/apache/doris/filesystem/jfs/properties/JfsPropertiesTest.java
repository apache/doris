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

package org.apache.doris.filesystem.jfs.properties;

import org.apache.doris.filesystem.hdfs.SimpleHadoopAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class JfsPropertiesTest {

    private JfsProperties bind(Map<String, String> raw) {
        JfsProperties props = new JfsProperties(raw);
        props.initNormalizeAndCheckProps();
        return props;
    }

    private Map<String, String> resolve(Map<String, String> raw) {
        return bind(raw).getBackendConfigProperties();
    }

    @Test
    void jfsUriDerivesDefaultFs() {
        Map<String, String> raw = new HashMap<>();
        raw.put("uri", "jfs://myvol/path/to/file");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("jfs://myvol", resolved.get("fs.defaultFS"));
    }

    @Test
    void juicefsKeysArePassedThrough() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "jfs://myvol");
        raw.put("juicefs.meta", "redis://localhost:6379/0");

        Map<String, String> resolved = resolve(raw);

        Assertions.assertEquals("redis://localhost:6379/0", resolved.get("juicefs.meta"));
    }

    // Legacy fe-core served jfs:// with the SAME HdfsProperties class as hdfs://, so JuiceFS
    // carries the full kerberos semantics: both alias families, credential validation with the
    // oracle's message, authenticator selection, and the translated backend-map keys.

    @Test
    void kerberosViaTypedAliasFamilyIsTranslated() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "jfs://myvol");
        raw.put("hdfs.authentication.type", "kerberos");
        raw.put("hdfs.authentication.kerberos.principal", "doris/host@REALM");
        raw.put("hdfs.authentication.kerberos.keytab", "/etc/security/doris.keytab");

        JfsProperties props = bind(raw);

        Assertions.assertTrue(props.isKerberos());
        Map<String, String> resolved = props.getBackendConfigProperties();
        Assertions.assertEquals("kerberos", resolved.get("hdfs.security.authentication"));
        Assertions.assertEquals("kerberos", resolved.get("hadoop.security.authentication"));
        Assertions.assertEquals("doris/host@REALM", resolved.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("/etc/security/doris.keytab", resolved.get("hadoop.kerberos.keytab"));
        Assertions.assertEquals("true", resolved.get("ipc.client.fallback-to-simple-auth-allowed"));
    }

    @Test
    void kerberosViaHadoopAliasFamilyIsTranslated() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "jfs://myvol");
        raw.put("hadoop.security.authentication", "kerberos");
        raw.put("hadoop.kerberos.principal", "doris/host@REALM");
        raw.put("hadoop.kerberos.keytab", "/etc/security/doris.keytab");

        JfsProperties props = bind(raw);

        Assertions.assertTrue(props.isKerberos());
        Map<String, String> resolved = props.getBackendConfigProperties();
        Assertions.assertEquals("kerberos", resolved.get("hadoop.security.authentication"));
        Assertions.assertEquals("doris/host@REALM", resolved.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("/etc/security/doris.keytab", resolved.get("hadoop.kerberos.keytab"));
    }

    @Test
    void kerberosWithoutCredentialsFailsWithOracleMessage() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "jfs://myvol");
        raw.put("hdfs.authentication.type", "kerberos");
        raw.put("hdfs.authentication.kerberos.principal", "doris/host@REALM");

        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> bind(raw));
        Assertions.assertEquals(
                "HDFS authentication type is kerberos, but principal or keytab is not set.",
                e.getMessage());
    }

    @Test
    void simpleFallbackWithoutKerberosProps() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "jfs://myvol");
        raw.put("hadoop.username", "hadoop");

        JfsProperties props = bind(raw);

        Assertions.assertFalse(props.isKerberos());
        Map<String, String> resolved = props.getBackendConfigProperties();
        Assertions.assertEquals("simple", resolved.get("hdfs.security.authentication"));
        Assertions.assertEquals("true", resolved.get("ipc.client.fallback-to-simple-auth-allowed"));
        Assertions.assertEquals("hadoop", resolved.get("hadoop.username"));
        Assertions.assertInstanceOf(SimpleHadoopAuthenticator.class, props.getExecutionAuthenticator());
    }
}
