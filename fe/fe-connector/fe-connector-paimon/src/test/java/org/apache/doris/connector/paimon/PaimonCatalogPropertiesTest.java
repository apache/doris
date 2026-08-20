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
 * The connector-level catalog properties: the backend flavor and the two type-mapping switches.
 *
 * <p>The split between {@link PaimonCatalogProperties#of(Map)} and
 * {@link PaimonCatalogProperties#checkCreateTimeOnlyRules()} is the thing to hold on to. {@code of()}
 * runs on EVERY connector build, including the lazy rebuild after an FE restart, so anything that
 * throws there turns a catalog that works today into one that cannot be reconstructed tomorrow. The
 * rules that only ever ran at CREATE/ALTER stay behind the second call, which only the provider makes.
 */
public class PaimonCatalogPropertiesTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    public void bindsFlavorAndBothMappingSwitches() {
        PaimonCatalogProperties p = PaimonCatalogProperties.of(props(
                PaimonCatalogProperties.PAIMON_CATALOG_TYPE, "hms",
                PaimonCatalogProperties.ENABLE_MAPPING_VARBINARY, "true",
                PaimonCatalogProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "true"));

        Assertions.assertEquals(PaimonCatalogProperties.HMS, p.getFlavor());
        Assertions.assertTrue(p.isEnableMappingVarbinary());
        Assertions.assertTrue(p.isEnableMappingTimestampTz());
    }

    @Test
    public void defaultsToFilesystemWithBothMappingsOff() {
        PaimonCatalogProperties p = PaimonCatalogProperties.of(props("warehouse", "/wh"));

        Assertions.assertEquals(PaimonCatalogProperties.FILESYSTEM, p.getFlavor());
        Assertions.assertFalse(p.isEnableMappingVarbinary());
        Assertions.assertFalse(p.isEnableMappingTimestampTz());
    }

    /** The flavor drives a switch in four classes, so it is lower-cased once, here. */
    @Test
    public void flavorIsLowerCased() {
        Assertions.assertEquals(PaimonCatalogProperties.JDBC,
                PaimonCatalogProperties.of(props(PaimonCatalogProperties.PAIMON_CATALOG_TYPE, "JDBC")).getFlavor());
    }

    /**
     * WHY: fe-core writes only the dotted spelling of the mapping keys, and an underscore variant would
     * read false and silently map the column the other way. Pinned so a "helpful" alias cannot be added
     * without someone deciding what a catalog carrying both should do.
     */
    @Test
    public void mappingSwitchesReadOnlyTheDottedSpelling() {
        PaimonCatalogProperties p = PaimonCatalogProperties.of(props(
                "enable_mapping_varbinary", "true",
                "enable_mapping_timestamp_tz", "true"));

        Assertions.assertFalse(p.isEnableMappingVarbinary());
        Assertions.assertFalse(p.isEnableMappingTimestampTz());
    }

    /**
     * WHY (convention rule D3.2): ALTER CATALOG merges properties -- it can overwrite a key but never
     * remove one -- and the same map carries engine keys and storage keys. Rejecting an unknown key
     * would build a catalog nobody can repair.
     */
    @Test
    public void unknownKeysAreTolerated() {
        Map<String, String> m = props(
                "type", "paimon",
                "warehouse", "/wh",
                "s3.endpoint", "http://minio:9000",
                "some_future_key", "x");

        Assertions.assertDoesNotThrow(() -> PaimonCatalogProperties.of(m));
    }

    /**
     * WHY: of() runs on every connector build. A catalog created before a validation rule existed still
     * has to come back after an FE restart, so of() must not enforce anything the connector can run
     * without. Every rule that fails below is reachable only through checkCreateTimeOnlyRules().
     */
    @Test
    public void ofAcceptsWhatOnlyTheCreateGateRejects() {
        // No warehouse (every flavor's validate() requires one), a garbage meta-cache ttl, and an
        // unparseable table option -- three separate CREATE-time rules.
        Map<String, String> m = props(
                PaimonCatalogProperties.PAIMON_CATALOG_TYPE, "filesystem",
                "meta.cache.paimon.table.ttl-second", "not-a-number",
                "paimon.table-option.", "orphan");

        PaimonCatalogProperties p = Assertions.assertDoesNotThrow(() -> PaimonCatalogProperties.of(m));
        Assertions.assertThrows(IllegalArgumentException.class, p::checkCreateTimeOnlyRules);
    }

    @Test
    public void createGateRejectsAnUnknownFlavor() {
        PaimonCatalogProperties p = PaimonCatalogProperties.of(props(
                PaimonCatalogProperties.PAIMON_CATALOG_TYPE, "dlf", "warehouse", "/wh"));

        Assertions.assertThrows(IllegalArgumentException.class, p::checkCreateTimeOnlyRules);
    }

    @Test
    public void createGateAcceptsAValidCatalog() {
        PaimonCatalogProperties p = PaimonCatalogProperties.of(props(
                PaimonCatalogProperties.PAIMON_CATALOG_TYPE, "hms",
                "warehouse", "/wh",
                "hive.metastore.uris", "thrift://nn:9083"));

        Assertions.assertDoesNotThrow(p::checkCreateTimeOnlyRules);
    }

    /**
     * WHY (convention rule D3.1): of() runs at CREATE, at ALTER validation and on every rebuild, so it
     * has to be pure and idempotent. Re-binding the same map must give the same answer.
     */
    @Test
    public void ofIsIdempotent() {
        Map<String, String> m = props(
                PaimonCatalogProperties.PAIMON_CATALOG_TYPE, "rest",
                PaimonCatalogProperties.ENABLE_MAPPING_VARBINARY, "true");

        Assertions.assertEquals(PaimonCatalogProperties.of(m).getFlavor(),
                PaimonCatalogProperties.of(m).getFlavor());
        Assertions.assertEquals(PaimonCatalogProperties.of(m).isEnableMappingVarbinary(),
                PaimonCatalogProperties.of(m).isEnableMappingVarbinary());
        Assertions.assertEquals(m, PaimonCatalogProperties.of(m).getRaw());
    }

    /** The raw map stays available for the wildcard passthroughs, but must not be writable through it. */
    @Test
    public void rawMapIsNotWritable() {
        PaimonCatalogProperties p = PaimonCatalogProperties.of(props("warehouse", "/wh"));

        Assertions.assertThrows(UnsupportedOperationException.class, () -> p.getRaw().put("k", "v"));
    }
}
