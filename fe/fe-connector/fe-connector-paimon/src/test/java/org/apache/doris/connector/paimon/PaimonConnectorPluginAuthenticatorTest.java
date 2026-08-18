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

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.hive.pool.CachedClientPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for Paimon's separate storage and HMS authenticator resolution.
 *
 * <p>The load-bearing NEW case is <b>HMS-metastore Kerberos with simple (non-Kerberos) storage</b> (e.g. a
 * Kerberized Hive Metastore over S3). Before design S6 retires the fe-core pre-execution authenticator, that
 * login was served fe-core-side by {@code PaimonHMSMetaStoreProperties} and delivered via
 * {@code DefaultConnectorContext}; the paimon connector must own it once that handle is a no-op — otherwise
 * S6 would silently drop Kerberos for a paimon secured-HMS-with-simple-storage catalog. These tests also pin
 * SIMPLE HMS identity and client-pool cache isolation without replacing the storage UGI used by FileIO.
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
        HadoopAuthenticator auth = PaimonConnector.buildHmsAuthenticator(
                props("paimon.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "kerberos",
                        "hive.metastore.client.principal", "doris@EXAMPLE.COM",
                        "hive.metastore.client.keytab", "/etc/security/doris.keytab"),
                new HashMap<>());
        Assertions.assertNotNull(auth,
                "HMS-metastore kerberos with simple storage must yield a plugin authenticator");
    }

    /** HMS auth is resolved separately so it can be applied only at the HMS client-pool boundary. */
    @Test
    public void hmsSimpleAuthUsesConfiguredUser() throws Exception {
        HadoopAuthenticator auth = PaimonConnector.buildHmsAuthenticator(
                props("paimon.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple",
                        "hive.metastore.username", "paimon-hms-user"),
                new HashMap<>());
        Assertions.assertEquals("paimon-hms-user", auth.getUGI().getUserName());
    }

    @Test
    public void explicitSimpleHmsKeepsItsUserWithKerberosStorage() throws Exception {
        HadoopAuthenticator auth = PaimonConnector.buildHmsAuthenticator(
                props("paimon.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "simple",
                        "hive.metastore.username", "paimon-hms-user",
                        "hadoop.security.authentication", "kerberos",
                        "hadoop.kerberos.principal", "storage@EXAMPLE.COM",
                        "hadoop.kerberos.keytab", "/etc/security/storage.keytab"),
                new HashMap<>());
        Assertions.assertEquals("paimon-hms-user", auth.getUGI().getUserName());
    }

    @Test
    public void hmsIdentitiesAreIncludedInClientPoolCacheKey() {
        Assertions.assertEquals(
                "ugi,conf:hadoop.username,conf:hive.metastore.client.principal,"
                        + "conf:hive.metastore.kerberos.principal,conf:hadoop.kerberos.principal,"
                        + "conf:hive.metastore.sasl.enabled",
                PaimonConnector.appendHmsCacheKeys("ugi"));
        Assertions.assertEquals(
                "conf:hadoop.username,conf:hive.metastore.client.principal,"
                        + "conf:hive.metastore.kerberos.principal,conf:hadoop.kerberos.principal,"
                        + "conf:hive.metastore.sasl.enabled",
                PaimonConnector.appendHmsCacheKeys("conf:hadoop.username"));
    }

    @Test
    public void hmsCacheKeyPreservesCaseSensitiveConfigurationNames() {
        Assertions.assertEquals(
                "conf:HADOOP.USERNAME,conf:hadoop.username,conf:hive.metastore.client.principal,"
                        + "conf:hive.metastore.kerberos.principal,conf:hadoop.kerberos.principal,"
                        + "conf:hive.metastore.sasl.enabled",
                PaimonConnector.appendHmsCacheKeys("conf:HADOOP.USERNAME"));
    }

    @Test
    public void transportChangesProduceDifferentSdkPoolKeys() throws Exception {
        Configuration first = poolConf("service-a/_HOST@REALM", false);
        Assertions.assertNotEquals(paimonPoolKey(first),
                paimonPoolKey(poolConf("service-b/_HOST@REALM", false)));
        Assertions.assertNotEquals(paimonPoolKey(first),
                paimonPoolKey(poolConf("service-a/_HOST@REALM", true)));
    }

    private static Configuration poolConf(String servicePrincipal, boolean sasl) {
        Configuration conf = new Configuration(false);
        conf.set("hive.metastore.uris", "thrift://hms:9083");
        conf.set("hive.metastore.kerberos.principal", servicePrincipal);
        conf.setBoolean("hive.metastore.sasl.enabled", sasl);
        return conf;
    }

    private static Object paimonPoolKey(Configuration conf) throws Exception {
        Method extractKey = CachedClientPool.class.getDeclaredMethod(
                "extractKey", String.class, String.class, Configuration.class);
        extractKey.setAccessible(true);
        return extractKey.invoke(null, "test-client", PaimonConnector.appendHmsCacheKeys(null), conf);
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
        HadoopAuthenticator auth = PaimonConnector.buildHmsAuthenticator(
                props("paimon.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms:9083",
                        "hive.metastore.authentication.type", "kerberos"),
                new HashMap<>());
        Assertions.assertNull(auth, "kerberos HMS without a client principal/keytab pair must not build one");
    }
}
