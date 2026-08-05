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

import org.apache.doris.connector.metastore.paimon.hms.PaimonHmsMetaStoreProperties;
import org.apache.doris.connector.metastore.paimon.jdbc.PaimonJdbcMetaStoreProperties;
import org.apache.doris.connector.metastore.paimon.rest.PaimonRestMetaStoreProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Proves that folding the Paimon catalog ASSEMBLY onto the bound metastore holders resolves the same
 * value the hand-written {@link PaimonCatalogFactory#firstNonBlank} resolves today.
 *
 * <p><b>Why this test exists.</b> Every alias-bearing paimon property currently has two readers: the
 * per-flavor {@code *MetaStoreProperties} holder binds it via {@code @ConnectorProperty(names = ...)}
 * for VALIDATION, and {@link PaimonCatalogFactory} re-scans the raw map with {@code firstNonBlank} and
 * a parallel {@code String[]} alias array for ASSEMBLY. Retiring the second reader is only safe if the
 * two resolve identically, and nothing in the compiler enforces that — a divergence would just make a
 * catalog validate against one value and connect with another.
 *
 * <p>Each case below drives the SAME map through both readers and asserts they agree. The one
 * deliberate exception is {@link #trimIsTheOneDeliberateDivergence()}: the binder trims, the helper
 * does not. That divergence is accepted (and is already live for the hms flavor, where the HiveConf is
 * built from the bound holder while the paimon {@code Options} come from the raw scan), so the
 * convergence normalizes the two rather than preserving the split.
 *
 * <p>This test is transitional: once {@code firstNonBlank} is gone from the connector it has nothing
 * left to compare against. The lasting guard is the alias-semantics block in fe-foundation's
 * {@code ConnectorPropertiesUtilsTest} plus the option snapshots in {@link PaimonCatalogFactoryTest}.
 */
public class PaimonAliasResolutionParityTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String bindHms(Map<String, String> raw) {
        return PaimonHmsMetaStoreProperties.of(raw, Collections.emptyMap()).getUri();
    }

    private static String bindRestUri(Map<String, String> raw) {
        return PaimonRestMetaStoreProperties.of(raw).getUri();
    }

    /**
     * Runs the four alias shapes that matter -- only the primary set, only the fallback set, both set
     * (priority), primary blank (blank counts as absent) -- through both readers and asserts equality.
     */
    private static void assertAgreesOnAllShapes(String[] aliases, Function<Map<String, String>, String> bind) {
        String primary = aliases[0];
        String fallback = aliases[1];

        Map<String, String> onlyPrimary = props(primary, "v-primary");
        Assertions.assertEquals(PaimonCatalogFactory.firstNonBlank(onlyPrimary, aliases), bind.apply(onlyPrimary),
                "primary-only disagrees for " + primary);

        Map<String, String> onlyFallback = props(fallback, "v-fallback");
        Assertions.assertEquals(PaimonCatalogFactory.firstNonBlank(onlyFallback, aliases), bind.apply(onlyFallback),
                "fallback-only disagrees for " + fallback);

        Map<String, String> both = props(primary, "v-primary", fallback, "v-fallback");
        Assertions.assertEquals(PaimonCatalogFactory.firstNonBlank(both, aliases), bind.apply(both),
                "alias priority disagrees for " + primary + " vs " + fallback);
        Assertions.assertEquals("v-primary", bind.apply(both), "declaration order is priority order");

        Map<String, String> blankPrimary = props(primary, "   ", fallback, "v-fallback");
        Assertions.assertEquals(PaimonCatalogFactory.firstNonBlank(blankPrimary, aliases), bind.apply(blankPrimary),
                "blank-primary fallthrough disagrees for " + primary);
        Assertions.assertEquals("v-fallback", bind.apply(blankPrimary), "a blank value must count as absent");
    }

    @Test
    public void hmsUriAliasesAgree() {
        assertAgreesOnAllShapes(PaimonConnectorProperties.HMS_URI, PaimonAliasResolutionParityTest::bindHms);
    }

    @Test
    public void restUriAliasesAgree() {
        assertAgreesOnAllShapes(PaimonConnectorProperties.REST_URI, PaimonAliasResolutionParityTest::bindRestUri);
    }

    @Test
    public void jdbcAliasesAgree() {
        assertAgreesOnAllShapes(PaimonConnectorProperties.JDBC_URI,
                raw -> PaimonJdbcMetaStoreProperties.of(raw).getUri());
        assertAgreesOnAllShapes(PaimonConnectorProperties.JDBC_USER,
                raw -> PaimonJdbcMetaStoreProperties.of(raw).getUser());
        assertAgreesOnAllShapes(PaimonConnectorProperties.JDBC_PASSWORD,
                raw -> PaimonJdbcMetaStoreProperties.of(raw).getPassword());
        assertAgreesOnAllShapes(PaimonConnectorProperties.JDBC_DRIVER_URL,
                raw -> PaimonJdbcMetaStoreProperties.of(raw).getDriverUrl());
        assertAgreesOnAllShapes(PaimonConnectorProperties.JDBC_DRIVER_CLASS,
                raw -> PaimonJdbcMetaStoreProperties.of(raw).getDriverClass());
    }

    /**
     * The single accepted divergence: a padded value binds trimmed but scans verbatim. Pinned rather
     * than hidden, because the assembly switches to the trimmed value -- and because the hms flavor
     * ALREADY splits this way today (HiveConf gets "thrift://nn:9083", the paimon Options get
     * "thrift://nn:9083 "), which is exactly the inconsistency the convergence removes.
     */
    @Test
    public void trimIsTheOneDeliberateDivergence() {
        Map<String, String> padded = props("hive.metastore.uris", " thrift://nn:9083 ");

        Assertions.assertEquals(" thrift://nn:9083 ",
                PaimonCatalogFactory.firstNonBlank(padded, PaimonConnectorProperties.HMS_URI));
        Assertions.assertEquals("thrift://nn:9083", bindHms(padded));
    }

    /**
     * Absence renders differently: the helper answers {@code null}, the holder answers {@code ""}.
     * Both are only reachable on a catalog that never passed validation (every flavor requires its
     * uri), but the assembly must not start emitting a literal "null" for one of them.
     */
    @Test
    public void absentAliasIsNullFromTheHelperAndEmptyFromTheHolder() {
        Map<String, String> none = props("warehouse", "/wh");

        Assertions.assertNull(PaimonCatalogFactory.firstNonBlank(none, PaimonConnectorProperties.HMS_URI));
        Assertions.assertEquals("", bindHms(none));
    }
}
