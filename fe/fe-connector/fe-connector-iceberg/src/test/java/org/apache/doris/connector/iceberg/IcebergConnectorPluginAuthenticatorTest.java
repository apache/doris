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

package org.apache.doris.connector.iceberg;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link IcebergConnector#buildPluginAuthenticator(Map, Map)} — the connector-owned
 * plugin-side Kerberos authenticator resolution.
 *
 * <p>The load-bearing case is <b>HMS-metastore Kerberos with simple (non-Kerberos) storage</b>
 * (e.g. a Kerberized Hive Metastore over S3). Before the fe-core iceberg property cluster is deleted
 * that login was served fe-core-side by
 * {@code IcebergHMSMetaStoreProperties -> HadoopExecutionAuthenticator(hmsBaseProperties.getHmsAuthenticator())}
 * and delivered via {@code DefaultConnectorContext}; the connector must own it once that cluster is gone.
 * These tests pin that the connector builds a plugin authenticator from the HMS client principal/keytab
 * facts, and does NOT build one when the metastore is simple-auth (which would silently force SIMPLE auth).
 *
 * <p>The actual keytab login is lazy (on first {@code doAs}), so these assertions never touch a KDC.
 */
public class IcebergConnectorPluginAuthenticatorTest {

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
        HadoopAuthenticator auth = IcebergConnector.buildPluginAuthenticator(
                props("iceberg.catalog.type", "hadoop",
                        "hadoop.security.authentication", "kerberos",
                        "hadoop.kerberos.principal", "doris@EXAMPLE.COM",
                        "hadoop.kerberos.keytab", "/etc/security/doris.keytab"),
                new HashMap<>());
        Assertions.assertNotNull(auth, "storage kerberos must yield a plugin authenticator");
    }

    /**
     * THE CUT-1 GAP: a Kerberized HMS whose data storage is simple. Storage auth is unset, so the storage
     * gate is off; the connector must fall back to the HMS client-principal/keytab facts and still build a
     * plugin authenticator (mirroring the fe-core HMS authenticator it replaces).
     */
    @Test
    public void hmsMetastoreKerberosWithSimpleStorageBuildsAuthenticator() {
        HadoopAuthenticator auth = IcebergConnector.buildPluginAuthenticator(
                props("iceberg.catalog.type", "hms",
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
        HadoopAuthenticator auth = IcebergConnector.buildPluginAuthenticator(
                props("iceberg.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple"),
                new HashMap<>());
        Assertions.assertNull(auth, "simple-auth HMS must not build a plugin authenticator");
    }

    /** A non-HMS flavor with no storage Kerberos builds no authenticator. */
    @Test
    public void nonHmsFlavorWithoutStorageKerberosReturnsNull() {
        HadoopAuthenticator auth = IcebergConnector.buildPluginAuthenticator(
                props("iceberg.catalog.type", "rest",
                        "uri", "http://rest:8181"),
                new HashMap<>());
        Assertions.assertNull(auth, "rest flavor without storage kerberos must not build an authenticator");
    }

    /**
     * HMS declares kerberos auth-type but the client principal/keytab are blank — the {@code hasCredentials}
     * guard must reject it (an authenticator with no login pair would fail obscurely at first doAs).
     */
    @Test
    public void hmsKerberosWithBlankCredsReturnsNull() {
        HadoopAuthenticator auth = IcebergConnector.buildPluginAuthenticator(
                props("iceberg.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "kerberos"),
                new HashMap<>());
        Assertions.assertNull(auth, "kerberos HMS without a client principal/keytab pair must not build one");
    }
}
