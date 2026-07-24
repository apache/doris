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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * CREATE-CATALOG property validation, exercised through the production entry point
 * {@link PaimonConnectorProvider#validateProperties(Map)} (called by fe-core
 * {@code PluginDrivenExternalCatalog.checkProperties}).
 *
 * <p>P2-T03: validation moved from the hand-rolled {@code PaimonCatalogFactory.validate} to the shared
 * {@code MetaStoreProviders.bind(props, {}).validate()}. The shared parsers restore the TRUE-legacy
 * rules the paimon hand-copy had dropped, so CREATE CATALOG is now STRICTER (user decision Q1 =
 * adopt the legacy-faithful validate): HMS {@code forbidIf(simple)}/{@code requireIf(kerberos)} on
 * client principal+keytab, the DLF OSS-storage requirement enforced at CREATE (not catalog build),
 * and REST case-sensitive {@code "dlf".equals(token.provider)}. These three are the net-new RED tests
 * here; the rest pin the required-key rules that already existed.
 */
public class PaimonConnectorValidatePropertiesTest {

    private static final PaimonConnectorProvider PROVIDER = new PaimonConnectorProvider();

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static void validate(Map<String, String> props) {
        PROVIDER.validateProperties(props);
    }

    // ---------------------------------------------------------------------
    // Required-key rules (pre-existing; retargeted to the provider entry point)
    // ---------------------------------------------------------------------

    @Test
    public void rejectsUnknownFlavor() {
        // WHY: an unknown paimon.catalog.type must fail at CREATE CATALOG, not silently fall back to
        // filesystem. Post-cutover the rejection is MetaStoreProviders.bind throwing (no provider
        // supports it) rather than the old "Unknown paimon.catalog.type value: X" message; we assert
        // only that CREATE fails (IllegalArgumentException), not the exact wording.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props("paimon.catalog.type", "bogus", "warehouse", "/wh")));
    }

    @Test
    public void requiresWarehouseForFilesystem() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props("paimon.catalog.type", "filesystem")));
    }

    @Test
    public void rejectsMalformedMetaCacheKnob() {
        // Legacy parity restored (user decision, 2026-07-01): meta.cache.paimon.table.{enable,ttl-second,
        // capacity} are validated again at CREATE/ALTER via the shared CacheSpec, so a malformed value is
        // REJECTED (this reverses the earlier warn-only behavior for dead knobs — enable/capacity stay unwired
        // on the plugin path, but an out-of-range/garbage value is still rejected, matching the deleted
        // PaimonExternalCatalog.checkProperties). The catalog is otherwise well-formed, so the knob is the
        // only variable.
        IllegalArgumentException capacity = Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "filesystem", "warehouse", "/wh",
                        "meta.cache.paimon.table.capacity", "-5")));
        Assertions.assertTrue(capacity.getMessage().contains("is wrong"));

        IllegalArgumentException ttl = Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "filesystem", "warehouse", "/wh",
                        "meta.cache.paimon.table.ttl-second", "-2")));
        Assertions.assertEquals(
                "The parameter meta.cache.paimon.table.ttl-second is wrong, value is -2", ttl.getMessage());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "filesystem", "warehouse", "/wh",
                        "meta.cache.paimon.table.enable", "maybe")));
    }

    @Test
    public void acceptsValidMetaCacheKnobs() {
        // Valid values must pass: ttl-second=-1 is the "no expiration" sentinel (min is -1), 0 disables,
        // capacity=0 disables, enable is boolean. enable/capacity remain unwired (warn-only) but are NOT
        // rejected when well-formed.
        Assertions.assertDoesNotThrow(() -> validate(props(
                "paimon.catalog.type", "filesystem", "warehouse", "/wh",
                "meta.cache.paimon.table.enable", "false",
                "meta.cache.paimon.table.ttl-second", "-1",
                "meta.cache.paimon.table.capacity", "0")));
        Assertions.assertDoesNotThrow(() -> validate(props(
                "paimon.catalog.type", "filesystem", "warehouse", "/wh",
                "meta.cache.paimon.table.ttl-second", "0")));
    }

    @Test
    public void requiresWarehouseForRest() {
        // Legacy parity: AbstractPaimonProperties requires warehouse and PaimonRestMetaStoreProperties
        // does NOT override it, so a REST catalog without warehouse is rejected.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "rest",
                        "paimon.rest.uri", "http://rest:8080")));
    }

    @Test
    public void restDlfTokenProviderRequiresAkSk() {
        // requireIf: token provider "dlf" (lower-case, the legacy case-sensitive value) needs the dlf
        // access-key-id AND access-key-secret. warehouse supplied so this exercises the requireIf.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "rest",
                        "warehouse", "/wh",
                        "paimon.rest.uri", "http://rest:8080",
                        "paimon.rest.token.provider", "dlf")));
    }

    @Test
    public void jdbcDriverUrlWithoutDriverClassFails() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "jdbc",
                        "warehouse", "/wh",
                        "uri", "jdbc:mysql://db:3306/meta",
                        "paimon.jdbc.driver_url", "mysql.jar")));
    }

    @Test
    public void hmsRequiresUri() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "hms",
                        "warehouse", "/wh")));
    }

    @Test
    public void acceptsEachWellFormedFlavor() {
        Assertions.assertDoesNotThrow(() -> validate(
                props("paimon.catalog.type", "filesystem", "warehouse", "/wh")));
        Assertions.assertDoesNotThrow(() -> validate(props(
                "paimon.catalog.type", "hms", "warehouse", "/wh", "hive.metastore.uris", "thrift://nn:9083")));
        Assertions.assertDoesNotThrow(() -> validate(props(
                "paimon.catalog.type", "rest", "warehouse", "/wh", "paimon.rest.uri", "http://rest:8080")));
        Assertions.assertDoesNotThrow(() -> validate(props(
                "paimon.catalog.type", "jdbc", "warehouse", "/wh", "uri", "jdbc:mysql://db:3306/meta")));
    }

    @Test
    public void defaultsToFilesystemWhenTypeAbsent() {
        Assertions.assertDoesNotThrow(() -> validate(props("warehouse", "/wh")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props("not-a-type", "x")));
    }

    // ---------------------------------------------------------------------
    // Net-new legacy-faithful tightening (Q1) — RED against the old loose validate
    // ---------------------------------------------------------------------

    @Test
    public void hmsKerberosRequiresPrincipalAndKeytab() {
        // requireIf(kerberos): legacy HMSBaseProperties.buildRules mandates the client principal AND
        // keytab when the HMS auth type is kerberos. The paimon hand-copy dropped this rule; the shared
        // parser restores it (HmsMetaStorePropertiesImpl.validate). MUTATION: dropping requireIf -> green
        // (no throw) -> test red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "hms",
                        "warehouse", "/wh",
                        "hive.metastore.uris", "thrift://nn:9083",
                        "hive.metastore.authentication.type", "kerberos")));
    }

    @Test
    public void hmsSimpleForbidsPrincipalAndKeytab() {
        // forbidIf(simple): legacy forbids a client principal/keytab when the auth type is simple
        // (case-SENSITIVE Objects.equals). Restored by the shared parser. RED against the old validate.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "hms",
                        "warehouse", "/wh",
                        "hive.metastore.uris", "thrift://nn:9083",
                        "hive.metastore.authentication.type", "simple",
                        "hive.metastore.client.principal", "hive/_HOST@REALM")));
    }


    @Test
    public void removedDlfCatalogTypeNoLongerValidates() {
        // WHY: paimon.catalog.type=dlf (DLF 1.0 over the vendored thrift ProxyMetaStoreClient) was removed. It
        // must now fail at CREATE CATALOG like any unknown type — never fall through to a backend whose client
        // no longer ships. MUTATION: re-registering the dlf provider -> this passes validate -> red.
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> validate(props(
                        "paimon.catalog.type", "dlf",
                        "warehouse", "/wh",
                        "dlf.access_key", "ak",
                        "dlf.secret_key", "sk",
                        "dlf.endpoint", "dlf.cn.aliyuncs.com")));
        Assertions.assertTrue(ex.getMessage().contains("No MetaStoreProvider supports"),
                "removed dlf must be unsupported, got: " + ex.getMessage());
    }
}
