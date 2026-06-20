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

package org.apache.doris.connector.metastore.spi.hms;

import org.apache.doris.kerberos.AuthType;
import org.apache.doris.kerberos.KerberosAuthSpec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** T2 parity for the HMS backend (legacy {@code HMSBaseProperties}/{@code buildHmsHiveConf}). */
public class HmsMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static HmsMetaStorePropertiesImpl of(Map<String, String> raw) {
        return HmsMetaStorePropertiesImpl.of(raw, Collections.emptyMap());
    }

    @Test
    public void simpleEmitsUriAndSocketTimeoutOnly() {
        HmsMetaStorePropertiesImpl props = of(raw("hive.metastore.uris", "thrift://h:9083", "warehouse", "wh"));

        Assertions.assertEquals("HMS", props.providerName());
        Assertions.assertTrue(props.needsStorage());
        Assertions.assertEquals("thrift://h:9083", props.getUri());
        Assertions.assertEquals(AuthType.SIMPLE, props.getAuthType());
        Assertions.assertFalse(props.kerberos().isPresent());

        Map<String, String> conf = props.toHiveConfOverrides("10");
        Assertions.assertEquals("thrift://h:9083", conf.get("hive.metastore.uris"));
        Assertions.assertEquals("10", conf.get("hive.metastore.client.socket.timeout"));
        // No kerberos leakage on a simple catalog.
        Assertions.assertFalse(conf.containsKey("hadoop.security.authentication"));
        Assertions.assertFalse(conf.containsKey("hive.metastore.sasl.enabled"));
        Assertions.assertEquals(2, conf.size());
    }

    @Test
    public void kerberosEmitsServicePrincipalSaslAndCarriesClientFacts() {
        HmsMetaStorePropertiesImpl props = of(raw(
                "hive.metastore.uris", "thrift://h:9083",
                "hive.metastore.authentication.type", "kerberos",
                "hive.metastore.client.principal", "doris@REALM",
                "hive.metastore.client.keytab", "/etc/doris.keytab",
                "hive.metastore.service.principal", "hive/_HOST@REALM",
                "hadoop.security.auth_to_local", "RULE:[1:$1]",
                "warehouse", "wh"));

        Map<String, String> conf = props.toHiveConfOverrides("10");
        Assertions.assertEquals("kerberos", conf.get("hive.metastore.authentication.type"));
        Assertions.assertEquals("doris@REALM", conf.get("hive.metastore.client.principal"));
        Assertions.assertEquals("/etc/doris.keytab", conf.get("hive.metastore.client.keytab"));
        Assertions.assertEquals("hive/_HOST@REALM", conf.get("hive.metastore.kerberos.principal"));
        Assertions.assertEquals("RULE:[1:$1]", conf.get("hadoop.security.auth_to_local"));
        Assertions.assertEquals("kerberos", conf.get("hadoop.security.authentication"));
        Assertions.assertEquals("true", conf.get("hive.metastore.sasl.enabled"));

        Assertions.assertEquals(AuthType.KERBEROS, props.getAuthType());
        Optional<KerberosAuthSpec> krb = props.kerberos();
        Assertions.assertTrue(krb.isPresent());
        Assertions.assertEquals("doris@REALM", krb.get().getPrincipal());
        Assertions.assertEquals("/etc/doris.keytab", krb.get().getKeytab());
        Assertions.assertTrue(krb.get().hasCredentials());
    }

    @Test
    public void kerberosBlockRunsAfterStorageOverlaySoItIsNotClobbered() {
        // User sets a raw hadoop.security.authentication=simple (storage passthrough); the kerberos
        // block must run LAST and force it back to kerberos (legacy ordering invariant).
        HmsMetaStorePropertiesImpl props = of(raw(
                "hive.metastore.uris", "thrift://h:9083",
                "hive.metastore.authentication.type", "kerberos",
                "hive.metastore.client.principal", "p", "hive.metastore.client.keytab", "k",
                "hadoop.security.authentication", "simple",
                "warehouse", "wh"));
        Map<String, String> conf = props.toHiveConfOverrides("10");
        Assertions.assertEquals("kerberos", conf.get("hadoop.security.authentication"));
        Assertions.assertEquals("true", conf.get("hive.metastore.sasl.enabled"));
    }

    @Test
    public void hdfsKerberosFallbackWhenMetastoreAuthIsNotSet() {
        HmsMetaStorePropertiesImpl props = of(raw(
                "hive.metastore.uris", "thrift://h:9083",
                "hadoop.security.authentication", "kerberos",
                "hadoop.kerberos.principal", "hdfs@REALM",
                "hadoop.kerberos.keytab", "/etc/hdfs.keytab",
                "warehouse", "wh"));
        Map<String, String> conf = props.toHiveConfOverrides("10");
        Assertions.assertEquals("kerberos", conf.get("hadoop.security.authentication"));
        Assertions.assertEquals("true", conf.get("hive.metastore.sasl.enabled"));
        // Metastore auth type itself is unset -> SIMPLE, but the effective kerberos facts come from HDFS.
        Assertions.assertEquals(AuthType.SIMPLE, props.getAuthType());
        Optional<KerberosAuthSpec> krb = props.kerberos();
        Assertions.assertTrue(krb.isPresent());
        Assertions.assertEquals("hdfs@REALM", krb.get().getPrincipal());
        Assertions.assertEquals("/etc/hdfs.keytab", krb.get().getKeytab());
    }

    @Test
    public void usernameAliasResolvesToHadoopUsername() {
        Map<String, String> conf = of(raw(
                "hive.metastore.uris", "thrift://h", "hive.metastore.username", "bob", "warehouse", "wh"))
                .toHiveConfOverrides("10");
        Assertions.assertEquals("bob", conf.get("hadoop.username"));
    }

    @Test
    public void validateChecksWarehouseThenUriThenAuthRules() {
        Assertions.assertEquals("Property warehouse is required.",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> of(raw("hive.metastore.uris", "thrift://h")).validate()).getMessage());
        Assertions.assertEquals("hive.metastore.uris or uri is required",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> of(raw("warehouse", "wh")).validate()).getMessage());
        // forbidIf simple
        Assertions.assertEquals("hive.metastore.client.principal and hive.metastore.client.keytab cannot be set when "
                        + "hive.metastore.authentication.type is simple",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> of(raw("warehouse", "wh", "hive.metastore.uris", "thrift://h",
                                "hive.metastore.authentication.type", "simple",
                                "hive.metastore.client.principal", "p")).validate()).getMessage());
        // requireIf kerberos (missing keytab)
        Assertions.assertEquals("hive.metastore.client.principal and hive.metastore.client.keytab are required when "
                        + "hive.metastore.authentication.type is kerberos",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> of(raw("warehouse", "wh", "hive.metastore.uris", "thrift://h",
                                "hive.metastore.authentication.type", "kerberos",
                                "hive.metastore.client.principal", "p")).validate()).getMessage());
        // valid simple (no client creds)
        of(raw("warehouse", "wh", "hive.metastore.uris", "thrift://h",
                "hive.metastore.authentication.type", "simple")).validate();
    }

    @Test
    public void authTypeMatchIsCaseSensitiveMirroringParamRules() {
        // ParamRules.forbidIf uses Objects.equals (case-sensitive): "Simple" != "simple", so the
        // forbid rule must NOT fire even though client.principal is set. (A mutation to equalsIgnoreCase
        // would make this throw.)
        of(raw("warehouse", "wh", "hive.metastore.uris", "thrift://h",
                "hive.metastore.authentication.type", "Simple",
                "hive.metastore.client.principal", "p")).validate();
    }

    @Test
    public void storageOverlayRunsBeforeKerberosBlockViaStorageMapChannel() {
        // The clobber candidate arrives ONLY via the storageHadoopConfig map (not raw), so this pins the
        // DV-007 invariant through the actual storage channel: step-5 overlay (which writes simple) MUST
        // run before step-6 kerberos (which forces kerberos). The marker proves the overlay ran at all.
        Map<String, String> storage = new HashMap<>();
        storage.put("hadoop.security.authentication", "simple");
        storage.put("fs.s3a.marker", "ran");
        Map<String, String> conf = HmsMetaStorePropertiesImpl.of(raw(
                "hive.metastore.uris", "thrift://h",
                "hive.metastore.authentication.type", "kerberos",
                "hive.metastore.client.principal", "p", "hive.metastore.client.keytab", "k",
                "warehouse", "wh"), storage).toHiveConfOverrides("10");
        Assertions.assertEquals("ran", conf.get("fs.s3a.marker"));
        Assertions.assertEquals("kerberos", conf.get("hadoop.security.authentication"));
    }

    @Test
    public void uriPrefersFirstAlias() {
        // names = {"hive.metastore.uris", "uri"} -> first alias wins when both are set.
        Assertions.assertEquals("thrift://first",
                of(raw("hive.metastore.uris", "thrift://first", "uri", "thrift://second", "warehouse", "wh"))
                        .getUri());
    }

    @Test
    public void usernameAliasOverwritesStorageHadoopUsername() {
        // Storage overlay (step 5) writes hadoop.username from the storage map; step 7 (after the overlay)
        // must overwrite it with the resolved username alias.
        Map<String, String> storage = new HashMap<>();
        storage.put("hadoop.username", "from-storage");
        Map<String, String> conf = HmsMetaStorePropertiesImpl.of(raw(
                "hive.metastore.uris", "thrift://h", "hive.metastore.username", "bob", "warehouse", "wh"),
                storage).toHiveConfOverrides("10");
        Assertions.assertEquals("bob", conf.get("hadoop.username"));
    }

    @Test
    public void hdfsKerberosFallbackSuppressedWhenMetastoreAuthIsSimple() {
        // auth type explicitly "simple" -> the HDFS-kerberos fallback must NOT fire (the !simple guard).
        HmsMetaStorePropertiesImpl props = of(raw(
                "hive.metastore.uris", "thrift://h",
                "hive.metastore.authentication.type", "simple",
                "hadoop.security.authentication", "kerberos",
                "hadoop.kerberos.principal", "hdfs@REALM", "hadoop.kerberos.keytab", "/k",
                "warehouse", "wh"));
        Assertions.assertFalse(props.kerberos().isPresent());
        Assertions.assertFalse(props.toHiveConfOverrides("10").containsKey("hive.metastore.sasl.enabled"));
    }

    @Test
    public void userSuppliedSocketTimeoutSurvivesTheDefault() {
        Map<String, String> conf = of(raw(
                "hive.metastore.uris", "thrift://h", "hive.metastore.client.socket.timeout", "30",
                "warehouse", "wh")).toHiveConfOverrides("10");
        Assertions.assertEquals("30", conf.get("hive.metastore.client.socket.timeout"));
    }

    @Test
    public void threadedSocketTimeoutDefaultFlowsThrough() {
        // C4: the FE-configured hive_metastore_client_timeout_second (threaded as the default arg) is applied
        // instead of the hardcoded 10 when the user did not set hive.metastore.client.socket.timeout.
        Map<String, String> conf = of(raw("hive.metastore.uris", "thrift://h", "warehouse", "wh"))
                .toHiveConfOverrides("60");
        Assertions.assertEquals("60", conf.get("hive.metastore.client.socket.timeout"));
    }

    @Test
    public void userSocketTimeoutOverridesThreadedDefault() {
        // C4: a per-catalog hive.metastore.client.socket.timeout still wins over the threaded FE default.
        Map<String, String> conf = of(raw(
                "hive.metastore.uris", "thrift://h", "hive.metastore.client.socket.timeout", "30",
                "warehouse", "wh")).toHiveConfOverrides("60");
        Assertions.assertEquals("30", conf.get("hive.metastore.client.socket.timeout"));
    }

    @Test
    public void blankThreadedSocketTimeoutFallsBackToTen() {
        // C4: defensive fallback — a blank threaded default keeps the historical 10s (legacy parity when unset).
        Map<String, String> conf = of(raw("hive.metastore.uris", "thrift://h", "warehouse", "wh"))
                .toHiveConfOverrides("");
        Assertions.assertEquals("10", conf.get("hive.metastore.client.socket.timeout"));
    }

    @Test
    public void matchedPropertiesIncludesMatchedAliasesAndExcludesUnmatched() {
        Map<String, String> matched = of(raw(
                "hive.metastore.uris", "thrift://h", "warehouse", "wh", "some.random.key", "v"))
                .matchedProperties();
        Assertions.assertEquals("thrift://h", matched.get("hive.metastore.uris"));
        Assertions.assertEquals("wh", matched.get("warehouse"));
        Assertions.assertFalse(matched.containsKey("some.random.key"));
    }
}
