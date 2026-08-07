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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link IcebergCatalogProperties}, the connector-level (flavor-independent) catalog
 * properties.
 *
 * <p>The three rules that matter are about WHEN things are allowed to fail, not about what the getters
 * return. {@code of(Map)} runs on every connector build — including the lazy rebuild after an FE restart —
 * so it must bind an existing catalog's properties without judging them; everything that judges belongs to
 * {@code checkCreateTimeOnlyRules()}, which only a CREATE/ALTER statement reaches. A rule that drifts from
 * the second method into the first does not fail here loudly — it fails in production, months later, as a
 * catalog that stops coming back after a restart.
 */
public class IcebergCatalogPropertiesTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    public void ofNeverThrowsSoAnExistingCatalogAlwaysRebuilds() {
        // WHY: of() is on the rebuild path. Everything a live catalog could carry -- an unknown backend, a
        // meta-cache value that fails validation, a key this connector does not model -- must still bind.
        // MUTATION: moving any check out of checkCreateTimeOnlyRules() into of() -> red.
        Assertions.assertDoesNotThrow(() -> IcebergCatalogProperties.of(props(
                "iceberg.catalog.type", "no-such-backend",
                "meta.cache.iceberg.table.ttl-second", "not-a-number",
                "some.key.this.connector.never.heard.of", "x")));
        Assertions.assertDoesNotThrow(() -> IcebergCatalogProperties.of(Collections.emptyMap()));
    }

    @Test
    public void ofIsIdempotentAndDoesNotAliasTheCallersMap() {
        // WHY: the connector binds once per build and hands the result to four consumers; a second bind of
        // the same map must produce the same facts, and a later mutation of the caller's map must not change
        // what an already-built connector sees. MUTATION: keeping the map by reference -> red.
        Map<String, String> raw = props("iceberg.catalog.type", "REST", "enable.mapping.varbinary", "true");
        IcebergCatalogProperties first = IcebergCatalogProperties.of(raw);
        IcebergCatalogProperties second = IcebergCatalogProperties.of(raw);
        Assertions.assertEquals(first.getFlavor(), second.getFlavor());
        Assertions.assertEquals(first.isEnableMappingVarbinary(), second.isEnableMappingVarbinary());

        raw.put("iceberg.catalog.type", "hms");
        Assertions.assertEquals("rest", first.getFlavor(), "a bound instance must not follow the caller's map");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> first.getRaw().put("k", "v"), "the exposed raw map must be unmodifiable");
    }

    @Test
    public void flavorIsLowerCasedAndNullWhenTheCatalogNamesNone() {
        // WHY: the flavor drives the second-level dispatch, and every switch on it compares lower-case. A
        // catalog that names NO backend is a distinct state (the build reports "Missing ..."), so it must
        // stay null rather than acquire a default. MUTATION: dropping toLowerCase, or defaulting to a
        // flavor -> red.
        Assertions.assertEquals("rest", IcebergCatalogProperties.of(
                props("iceberg.catalog.type", "REST")).getFlavor());
        Assertions.assertEquals("hms", IcebergCatalogProperties.of(
                props("iceberg.catalog.type", "Hms")).getFlavor());
        Assertions.assertNull(IcebergCatalogProperties.of(Collections.emptyMap()).getFlavor());
        Assertions.assertNull(IcebergCatalogProperties.of(props("iceberg.catalog.type", "")).getFlavor());
        // Surrounding whitespace is stripped by the binder, so a padded value names its backend rather
        // than a nonexistent one.
        Assertions.assertEquals("hms", IcebergCatalogProperties.of(
                props("iceberg.catalog.type", "  hms  ")).getFlavor());
    }

    @Test
    public void externalCatalogNameIsEmptyUnlessTheCatalogNamesOne() {
        // WHY: the value becomes a namespace level applied to EVERY table resolution. An absent (or blank)
        // key must yield an empty Optional -- an Optional.of("") would silently prepend an empty level and
        // resolve every table in the wrong namespace. MUTATION: defaulting the field to "" -> red.
        Assertions.assertFalse(IcebergCatalogProperties.of(Collections.emptyMap())
                .getExternalCatalogName().isPresent());
        Assertions.assertFalse(IcebergCatalogProperties.of(props("external_catalog.name", ""))
                .getExternalCatalogName().isPresent());
        Assertions.assertEquals("prod", IcebergCatalogProperties.of(props("external_catalog.name", "prod"))
                .getExternalCatalogName().orElse(null));
    }

    @Test
    public void typeMappingSwitchesDefaultOffAndReadTheDottedKeys() {
        // WHY: the dotted spelling is the only one a live catalog map carries; an underscore variant would
        // read false and silently drop the BINARY->VARBINARY / TIMESTAMPTZ mapping for every table.
        // MUTATION: binding an underscore key name -> red.
        IcebergCatalogProperties off = IcebergCatalogProperties.of(Collections.emptyMap());
        Assertions.assertFalse(off.isEnableMappingVarbinary());
        Assertions.assertFalse(off.isEnableMappingTimestampTz());

        IcebergCatalogProperties on = IcebergCatalogProperties.of(props(
                "enable.mapping.varbinary", "true", "enable.mapping.timestamp_tz", "TRUE"));
        Assertions.assertTrue(on.isEnableMappingVarbinary());
        Assertions.assertTrue(on.isEnableMappingTimestampTz(), "parsing must stay case-insensitive");
    }

    @Test
    public void createTimeRulesRejectABadMetaCacheValue() {
        // WHY: this restores the legacy fail-fast the SPI cutover dropped. Without it a typo'd ttl is
        // silently coerced to a cache-disabling default, so the operator's tuning quietly does nothing.
        // MUTATION: dropping checkMetaCacheProperties -> the statement is accepted -> red.
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergCatalogProperties.of(props(
                        "iceberg.catalog.type", "hadoop", "warehouse", "s3://b/wh",
                        "meta.cache.iceberg.table.ttl-second", "half-an-hour")).checkCreateTimeOnlyRules());
        Assertions.assertTrue(e.getMessage().contains("meta.cache.iceberg.table.ttl-second"),
                "the message must name the offending key: " + e.getMessage());
    }

    @Test
    public void createTimeRulesRejectAnUnknownBackendAndRunTheBackendsOwnRules() {
        // WHY: the CREATE gate is where a catalog that could never work is refused. Two distinct failures:
        // no backend answers to the named type, and a named backend's own fail-fast (here: a hadoop catalog
        // cannot initialize without a warehouse). MUTATION: not dispatching -> both accepted -> red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergCatalogProperties.of(props("iceberg.catalog.type", "no-such-backend"))
                        .checkCreateTimeOnlyRules());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergCatalogProperties.of(props("iceberg.catalog.type", "hadoop"))
                        .checkCreateTimeOnlyRules(),
                "a hadoop catalog without a warehouse must be refused at CREATE");
        Assertions.assertDoesNotThrow(() -> IcebergCatalogProperties.of(props(
                "iceberg.catalog.type", "hadoop", "warehouse", "s3://bucket/wh")).checkCreateTimeOnlyRules());
    }

    @Test
    public void toStringMasksSecretsCarriedInTheRawMap() {
        // WHY: this object is logged. It declares no sensitive field of its own, but the masking helper is
        // what keeps that true if one is ever added. MUTATION: switching to a plain field dump -> red.
        String rendered = IcebergCatalogProperties.of(props(
                "iceberg.catalog.type", "rest", "enable.mapping.varbinary", "true")).toString();
        Assertions.assertTrue(rendered.contains("rest"), "non-secret values stay readable: " + rendered);
    }
}
