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

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.hdfs.SimpleHadoopAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The shared Kerberos/simple authentication model lifted into {@link HdfsCompatibleProperties}.
 * Golden facts are the legacy fe-core {@code HdfsProperties} oracle, which served hdfs://,
 * viewfs:// and jfs:// with one class: both kerberos alias families, alias precedence (typed
 * {@code hdfs.authentication.*} names come first in the annotation), the byte-identical
 * missing-credential validation message, and the backend-map auth translation.
 */
class HdfsCompatiblePropertiesAuthTest {

    /** Minimal concrete subclass: backend map = auth translation only. */
    private static final class TestAuthProperties extends HdfsCompatibleProperties {
        TestAuthProperties(Map<String, String> origProps) {
            super(origProps);
        }

        @Override
        public String providerName() {
            return "TEST_HDFS_COMPATIBLE";
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.HDFS;
        }

        @Override
        public Set<String> getSupportedSchemes() {
            return Set.of("hdfs");
        }

        @Override
        protected void doInitNormalizeAndCheckProps() {
            Map<String, String> props = new HashMap<>();
            applyAuthBackendConfig(props);
            this.backendConfigProperties = props;
        }
    }

    private static TestAuthProperties bind(Map<String, String> raw) {
        TestAuthProperties props = new TestAuthProperties(raw);
        props.initNormalizeAndCheckProps();
        return props;
    }

    @Test
    void typedAliasFamilyEnablesKerberos() {
        Map<String, String> raw = new HashMap<>();
        raw.put("hdfs.authentication.type", "kerberos");
        raw.put("hdfs.authentication.kerberos.principal", "doris/host@REALM");
        raw.put("hdfs.authentication.kerberos.keytab", "/etc/security/doris.keytab");

        TestAuthProperties props = bind(raw);

        Assertions.assertTrue(props.isKerberos());
        Map<String, String> backend = props.getBackendConfigProperties();
        Assertions.assertEquals("kerberos", backend.get("hdfs.security.authentication"));
        Assertions.assertEquals("kerberos", backend.get("hadoop.security.authentication"));
        Assertions.assertEquals("doris/host@REALM", backend.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("/etc/security/doris.keytab", backend.get("hadoop.kerberos.keytab"));
        Assertions.assertEquals("true", backend.get("ipc.client.fallback-to-simple-auth-allowed"));
    }

    @Test
    void hadoopAliasFamilyEnablesKerberos() {
        Map<String, String> raw = new HashMap<>();
        raw.put("hadoop.security.authentication", "kerberos");
        raw.put("hadoop.kerberos.principal", "doris/host@REALM");
        raw.put("hadoop.kerberos.keytab", "/etc/security/doris.keytab");

        TestAuthProperties props = bind(raw);

        Assertions.assertTrue(props.isKerberos());
        Map<String, String> backend = props.getBackendConfigProperties();
        Assertions.assertEquals("kerberos", backend.get("hadoop.security.authentication"));
        Assertions.assertEquals("doris/host@REALM", backend.get("hadoop.kerberos.principal"));
        Assertions.assertEquals("/etc/security/doris.keytab", backend.get("hadoop.kerberos.keytab"));
    }

    @Test
    void typedAliasTakesPrecedenceOverHadoopAlias() {
        // Annotation name order is the precedence contract: hdfs.authentication.* first.
        Map<String, String> raw = new HashMap<>();
        raw.put("hdfs.authentication.type", "simple");
        raw.put("hadoop.security.authentication", "kerberos");
        raw.put("hadoop.kerberos.principal", "doris/host@REALM");
        raw.put("hadoop.kerberos.keytab", "/etc/security/doris.keytab");

        TestAuthProperties props = bind(raw);

        Assertions.assertFalse(props.isKerberos());
        Assertions.assertEquals("simple",
                props.getBackendConfigProperties().get("hdfs.security.authentication"));
    }

    @Test
    void kerberosWithoutPrincipalOrKeytabFailsWithOracleMessage() {
        for (String[] partial : new String[][] {
                {"hdfs.authentication.kerberos.principal", "doris/host@REALM"},
                {"hdfs.authentication.kerberos.keytab", "/etc/security/doris.keytab"},
                {}}) {
            Map<String, String> raw = new HashMap<>();
            raw.put("hdfs.authentication.type", "kerberos");
            if (partial.length > 0) {
                raw.put(partial[0], partial[1]);
            }
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> bind(raw));
            Assertions.assertEquals(
                    "HDFS authentication type is kerberos, but principal or keytab is not set.",
                    e.getMessage());
        }
    }

    @Test
    void simpleDefaultsToSimpleAuthenticatorAndFallbackTrue() {
        TestAuthProperties props = bind(new HashMap<>());

        Assertions.assertFalse(props.isKerberos());
        Map<String, String> backend = props.getBackendConfigProperties();
        Assertions.assertEquals("simple", backend.get("hdfs.security.authentication"));
        Assertions.assertEquals("true", backend.get("ipc.client.fallback-to-simple-auth-allowed"));
        Assertions.assertNull(backend.get("hadoop.security.authentication"));
        Assertions.assertInstanceOf(SimpleHadoopAuthenticator.class, props.getExecutionAuthenticator());
    }

    @Test
    void explicitFallbackValueAndHadoopUsernameAreHonoured() {
        Map<String, String> raw = new HashMap<>();
        raw.put("ipc.client.fallback-to-simple-auth-allowed", "false");
        raw.put("hadoop.username", "etl_user");

        Map<String, String> backend = bind(raw).getBackendConfigProperties();

        Assertions.assertEquals("false", backend.get("ipc.client.fallback-to-simple-auth-allowed"));
        Assertions.assertEquals("etl_user", backend.get("hadoop.username"));
    }
}
