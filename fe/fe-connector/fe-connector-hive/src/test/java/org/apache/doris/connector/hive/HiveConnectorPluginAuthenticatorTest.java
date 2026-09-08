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

package org.apache.doris.connector.hive;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link HiveConnector#buildPluginAuthenticator(Map)} — the connector-owned plugin-side
 * authenticator resolution.
 *
 * <p>The load-bearing case is <b>HMS-metastore Kerberos with simple (non-Kerberos) storage</b>
 * (e.g. a Kerberized Hive Metastore over S3). After the catalog flip the FE-injected
 * {@code ConnectorContext.executeAuthenticated} resolves to NOOP (SIMPLE) auth, so a Kerberos HMS would be
 * silently downgraded unless the connector owns the login itself. These tests pin that the connector builds a
 * plugin authenticator from the HMS client principal/keytab facts. Simple-auth cases also pin the HMS UGI to
 * the configured Hadoop user, including the metastore alias and legacy default.
 *
 * <p>The actual keytab login is lazy (on first {@code doAs}), so these assertions never touch a KDC.
 */
public class HiveConnectorPluginAuthenticatorTest {

    @TempDir
    Path tempDir;

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /** Storage-level Kerberos (raw hadoop.security.authentication) — unchanged prior behavior. */
    @Test
    public void storageKerberosBuildsAuthenticator() {
        HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hadoop.security.authentication", "kerberos",
                        "hadoop.kerberos.principal", "doris@EXAMPLE.COM",
                        "hadoop.kerberos.keytab", "/etc/security/doris.keytab"));
        Assertions.assertNotNull(auth, "storage kerberos must yield a plugin authenticator");
    }

    /**
     * THE BLOCKER CASE: a Kerberized HMS whose data storage is simple. Storage auth is unset, so the storage
     * gate is off; the connector must fall back to the HMS client-principal/keytab facts and still build a
     * plugin authenticator (mirroring the fe-core HMS authenticator it replaces).
     */
    @Test
    public void hmsMetastoreKerberosWithSimpleStorageBuildsAuthenticator() {
        HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "kerberos",
                        "hive.metastore.client.principal", "doris@EXAMPLE.COM",
                        "hive.metastore.client.keytab", "/etc/security/doris.keytab"));
        Assertions.assertNotNull(auth,
                "HMS-metastore kerberos with simple storage must yield a plugin authenticator");
    }

    @Test
    public void hmsSimpleAuthUsesConfiguredHadoopUser() throws Exception {
        HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple",
                        "hadoop.username", "hive"));

        Assertions.assertNotNull(auth, "simple-auth HMS must yield a plugin authenticator");
        Assertions.assertEquals("hive", auth.getUGI().getUserName());
    }

    @Test
    public void hmsSimpleAuthUsesMetastoreUsernameAlias() throws Exception {
        HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple",
                        "hive.metastore.username", "hive"));

        Assertions.assertNotNull(auth, "simple-auth HMS must support the metastore username alias");
        Assertions.assertEquals("hive", auth.getUGI().getUserName());
    }

    @Test
    public void explicitSimpleHmsWinsOverKerberosStorage() throws Exception {
        HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple",
                        "hive.metastore.username", "metastore-user",
                        "hadoop.security.authentication", "kerberos",
                        "hadoop.kerberos.principal", "storage@EXAMPLE.COM",
                        "hadoop.kerberos.keytab", "/etc/security/storage.keytab"));

        Assertions.assertEquals("metastore-user", auth.getUGI().getUserName());
    }

    @Test
    public void blankHadoopUserFallsBackToLegacyDefault() throws Exception {
        for (String blank : new String[] {"", "   "}) {
            HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                    props("hive.metastore.uris", "thrift://hms:9083",
                            "hive.metastore.authentication.type", "simple",
                            "hadoop.username", blank));
            Assertions.assertEquals("hadoop", auth.getUGI().getUserName());
        }
    }

    @Test
    public void simpleHmsUserCanComeFromHiveConfResource() throws Exception {
        Path resource = tempDir.resolve("hive-site.xml");
        Files.writeString(resource, "<configuration><property><name>hadoop.username</name>"
                + "<value>resource-user</value></property></configuration>");
        String previous = System.setProperty("doris.hadoop.config.dir", tempDir + "/");
        try {
            HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                    props("hive.metastore.uris", "thrift://hms:9083",
                            "hive.metastore.authentication.type", "simple",
                            "hive.conf.resources", resource.getFileName().toString()));
            Assertions.assertEquals("resource-user", auth.getUGI().getUserName());
        } finally {
            if (previous == null) {
                System.clearProperty("doris.hadoop.config.dir");
            } else {
                System.setProperty("doris.hadoop.config.dir", previous);
            }
        }
    }

    @Test
    public void hmsSimpleAuthUsesLegacyDefaultUser() throws Exception {
        HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple"));
        Assertions.assertNotNull(auth, "simple-auth HMS must yield a plugin authenticator");
        Assertions.assertEquals("hadoop", auth.getUGI().getUserName());
    }

    @Test
    public void plainHmsUsesLegacyDefaultUser() throws Exception {
        HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083"));
        Assertions.assertNotNull(auth, "plain HMS must yield a plugin authenticator");
        Assertions.assertEquals("hadoop", auth.getUGI().getUserName());
    }

    /**
     * HMS declares kerberos auth-type but the client principal/keytab are blank — the {@code hasCredentials}
     * guard must reject it (an authenticator with no login pair would fail obscurely at first doAs).
     */
    @Test
    public void hmsKerberosWithBlankCredsReturnsNull() {
        HadoopAuthenticator auth = HiveConnector.buildPluginAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "kerberos"));
        Assertions.assertNull(auth, "kerberos HMS without a client principal/keytab pair must not build one");
    }

    @Test
    public void hmsClientUsesFeConfiguredSocketTimeout() {
        HiveConnector connector = new HiveConnector(HiveTestProperties.minimalMap(),
                new FakeConnectorContext(Map.of("hive_metastore_client_timeout_second", "47")));
        Assertions.assertEquals("47", connector.buildHmsClientConfig().getProperties()
                .get("hive.metastore.client.socket.timeout"));
    }

    @Test
    public void firstConnectorInitializesConfiguredHadoopResourceDirectory() throws Exception {
        Path resource = tempDir.resolve("hive-site.xml");
        Files.writeString(resource, "<configuration><property><name>hadoop.username</name>"
                + "<value>first-hive-user</value></property></configuration>");
        Map<String, String> properties = props(
                "hive.metastore.uris", "thrift://hms:9083",
                "hive.metastore.authentication.type", "simple",
                "hive.conf.resources", resource.getFileName().toString());
        String previous = System.getProperty("doris.hadoop.config.dir");
        System.clearProperty("doris.hadoop.config.dir");
        try {
            new HiveConnector(properties,
                    new FakeConnectorContext(Map.of("hadoop_config_dir", tempDir.toString())));
            Assertions.assertEquals("first-hive-user",
                    HiveConnector.buildPluginAuthenticator(properties).getUGI().getUserName());
        } finally {
            if (previous == null) {
                System.clearProperty("doris.hadoop.config.dir");
            } else {
                System.setProperty("doris.hadoop.config.dir", previous);
            }
        }
    }
}
