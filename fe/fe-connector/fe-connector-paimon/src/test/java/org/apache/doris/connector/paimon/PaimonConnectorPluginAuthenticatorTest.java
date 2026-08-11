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

package org.apache.doris.connector.paimon;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link PaimonConnector#buildPluginAuthenticator(Map, Map)} — the connector-owned plugin-side
 * Kerberos authenticator resolution (design S6, ported from {@code IcebergConnector.buildPluginAuthenticator}).
 *
 * <p>The load-bearing NEW case is <b>HMS-metastore Kerberos with simple (non-Kerberos) storage</b> (e.g. a
 * Kerberized Hive Metastore over S3). Before design S6 retires the fe-core pre-execution authenticator, that
 * login was served fe-core-side by {@code PaimonHMSMetaStoreProperties} and delivered via
 * {@code DefaultConnectorContext}; the paimon connector must own it once that handle is a no-op — otherwise
 * S6 would silently drop Kerberos for a paimon secured-HMS-with-simple-storage catalog. These tests pin that
 * the connector builds a plugin authenticator from the HMS client principal/keytab facts, and does NOT build
 * one when the metastore is simple-auth (which would force needless SIMPLE-vs-Kerberos churn).
 *
 * <p>The actual keytab login is lazy (on first {@code doAs}), so these assertions never touch a KDC.
 */
public class PaimonConnectorPluginAuthenticatorTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /** Storage-level Kerberos (raw hadoop.security.authentication) — unchanged prior behavior, any flavor. */
    @Test
    public void storageKerberosBuildsAuthenticator() {
        HadoopAuthenticator auth = PaimonConnector.buildPluginAuthenticator(
                props("paimon.catalog.type", "filesystem",
                        "warehouse", "hdfs://ns/warehouse",
                        "hadoop.security.authentication", "kerberos",
                        "hadoop.kerberos.principal", "doris@EXAMPLE.COM",
                        "hadoop.kerberos.keytab", "/etc/security/doris.keytab"),
                new HashMap<>());
        Assertions.assertNotNull(auth, "storage kerberos must yield a plugin authenticator");
    }

    /**
     * THE S6 GAP: a Kerberized HMS whose data storage is simple. Storage auth is unset, so the storage gate is
     * off; the connector must fall back to the HMS client-principal/keytab facts and still build a plugin
     * authenticator (mirroring the fe-core HMS authenticator it replaces). Without this, retiring the fe-core
     * handle silently drops Kerberos for this catalog.
     */
    @Test
    public void hmsMetastoreKerberosWithSimpleStorageBuildsAuthenticator() {
        HadoopAuthenticator auth = PaimonConnector.buildPluginAuthenticator(
                props("paimon.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "kerberos",
                        "hive.metastore.client.principal", "doris@EXAMPLE.COM",
                        "hive.metastore.client.keytab", "/etc/security/doris.keytab"),
                new HashMap<>());
        Assertions.assertNotNull(auth,
                "HMS-metastore kerberos with simple storage must yield a plugin authenticator");
    }

    /** A simple-auth HMS builds no authenticator (a spurious one would force needless SIMPLE-vs-Kerberos churn). */
    @Test
    public void hmsSimpleAuthReturnsNull() {
        HadoopAuthenticator auth = PaimonConnector.buildPluginAuthenticator(
                props("paimon.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple"),
                new HashMap<>());
        Assertions.assertNull(auth, "simple-auth HMS must not build a plugin authenticator");
    }

    /** A non-HMS flavor with no storage Kerberos builds no authenticator. */
    @Test
    public void nonHmsFlavorWithoutStorageKerberosReturnsNull() {
        HadoopAuthenticator auth = PaimonConnector.buildPluginAuthenticator(
                props("paimon.catalog.type", "filesystem",
                        "warehouse", "s3://bucket/warehouse"),
                new HashMap<>());
        Assertions.assertNull(auth, "filesystem flavor without storage kerberos must not build an authenticator");
    }

    /**
     * HMS declares kerberos auth-type but the client principal/keytab are blank — the {@code hasCredentials}
     * guard must reject it (an authenticator with no login pair would fail obscurely at first doAs).
     */
    @Test
    public void hmsKerberosWithBlankCredsReturnsNull() {
        HadoopAuthenticator auth = PaimonConnector.buildPluginAuthenticator(
                props("paimon.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "kerberos"),
                new HashMap<>());
        Assertions.assertNull(auth, "kerberos HMS without a client principal/keytab pair must not build one");
    }
}
