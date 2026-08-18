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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.kerberos.HadoopAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for Hudi's separate storage and HMS authenticator resolution.
 *
 * <p>The load-bearing case is <b>HMS-metastore Kerberos with simple (non-Kerberos) storage</b> (e.g. a
 * Kerberized Hive Metastore over S3). After the catalog flip the hudi sibling shares the hms gateway's
 * FE-injected {@code ConnectorContext}, whose {@code executeAuthenticated} resolves to NOOP (SIMPLE) auth, so a
 * Kerberos HMS would be silently downgraded unless the connector owns the login itself. A hudi sibling runs in
 * its OWN classloader, so it must build its OWN authenticator (sharing the gateway's hive-loader authenticator
 * would split the UGI copy across loaders). SIMPLE cases pin the user sent by HMS {@code set_ugi}.
 *
 * <p>The actual keytab login is lazy (on first {@code doAs}), so these assertions never touch a KDC.
 */
public class HudiConnectorPluginAuthenticatorTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /** Storage-level Kerberos (raw hadoop.security.authentication) — the prior behavior, still honored. */
    @Test
    public void storageKerberosBuildsAuthenticator() {
        HadoopAuthenticator auth = HudiConnector.buildPluginAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hadoop.security.authentication", "kerberos",
                        "hadoop.kerberos.principal", "doris@EXAMPLE.COM",
                        "hadoop.kerberos.keytab", "/etc/security/doris.keytab"));
        Assertions.assertNotNull(auth, "storage kerberos must yield a plugin authenticator");
    }

    /**
     * THE BLOCKER CASE: a Kerberized HMS whose data storage is simple. Storage auth is unset, so the storage
     * gate is off; the connector must fall back to the HMS client-principal/keytab facts and still build a
     * plugin authenticator (mirroring the fe-core HMS authenticator it replaces). Without this a Kerberized
     * hudi-on-HMS table would silently downgrade to SIMPLE at the flip.
     */
    @Test
    public void hmsMetastoreKerberosWithSimpleStorageBuildsAuthenticator() {
        HadoopAuthenticator auth = HudiConnector.buildHmsAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "kerberos",
                        "hive.metastore.client.principal", "doris@EXAMPLE.COM",
                        "hive.metastore.client.keytab", "/etc/security/doris.keytab"));
        Assertions.assertNotNull(auth,
                "HMS-metastore kerberos with simple storage must yield a plugin authenticator");
    }

    /** A simple-auth HMS needs a UGI because HiveMetaStoreClient sends the current user in set_ugi. */
    @Test
    public void hmsSimpleAuthUsesConfiguredUser() throws Exception {
        HadoopAuthenticator auth = HudiConnector.buildHmsAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple",
                        "hive.metastore.username", "hudi-hms-user"));
        Assertions.assertEquals("hudi-hms-user", auth.getUGI().getUserName());
    }

    /** A plain HMS preserves the legacy simple-auth default. */
    @Test
    public void plainHmsUsesLegacyDefaultUser() throws Exception {
        HadoopAuthenticator auth = HudiConnector.buildHmsAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083"));
        Assertions.assertEquals("hadoop", auth.getUGI().getUserName());
    }

    @Test
    public void explicitSimpleHmsWinsOverKerberosStorage() throws Exception {
        HadoopAuthenticator auth = HudiConnector.buildHmsAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple",
                        "hive.metastore.username", "hudi-hms-user",
                        "hadoop.security.authentication", "kerberos",
                        "hadoop.kerberos.principal", "storage@EXAMPLE.COM",
                        "hadoop.kerberos.keytab", "/etc/security/storage.keytab"));
        Assertions.assertEquals("hudi-hms-user", auth.getUGI().getUserName());
    }

    /**
     * HMS declares kerberos auth-type but the client principal/keytab are blank — the {@code hasCredentials}
     * guard must reject it (an authenticator with no login pair would fail obscurely at first doAs).
     */
    @Test
    public void hmsKerberosWithBlankCredsReturnsNull() {
        HadoopAuthenticator auth = HudiConnector.buildHmsAuthenticator(
                props("hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "kerberos"));
        Assertions.assertNull(auth, "kerberos HMS without a client principal/keytab pair must not build one");
    }

    @Test
    public void hmsClientUsesFeConfiguredSocketTimeout() {
        ConnectorContext context = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return Map.of("hive_metastore_client_timeout_second", "47");
            }
        };
        HudiConnector connector = new HudiConnector(HudiTestProperties.minimalMap(), context);
        Assertions.assertEquals("47", connector.buildHmsClientConfig().getProperties()
                .get("hive.metastore.client.socket.timeout"));
    }
}
