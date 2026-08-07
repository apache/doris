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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.fluss.FlussCatalogProperties.UnionReadMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Pins the catalog-property contract of a fluss catalog: what CREATE CATALOG must reject, and exactly
 * which properties reach the fluss client.
 */
public class FlussCatalogPropertiesTest {

    private static Map<String, String> props(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    /**
     * A map that a real catalog could have been created from. Binding is validating, so every fixture
     * that is not itself testing the required key has to carry one — reading any other property off an
     * instance is only meaningful for an instance that could exist.
     */
    private static FlussCatalogProperties bound(String... keyValues) {
        Map<String, String> map = props(keyValues);
        map.putIfAbsent(FlussCatalogProperties.BOOTSTRAP_SERVERS, "localhost:9123");
        return FlussCatalogProperties.of(map);
    }

    @Test
    public void bootstrapServersIsRequired() {
        // A catalog with no bootstrap servers can never answer a query, so it must fail at CREATE
        // CATALOG rather than at the user's first SELECT.
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlussCatalogProperties.of(props()));
        Assertions.assertTrue(e.getMessage().contains(FlussCatalogProperties.BOOTSTRAP_SERVERS),
                "message should name the missing property, was: " + e.getMessage());

        // Blank is absent, not present-and-empty: the binder skips a blank value, so the field keeps its
        // default and the same "is missing" is reported.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> FlussCatalogProperties.of(props(FlussCatalogProperties.BOOTSTRAP_SERVERS, "   ")));
    }

    @Test
    public void bootstrapServersMustBeHostPortPairs() {
        // Each rejected form is one a user actually writes: a bare host, a non-numeric port, a port out
        // of range, and a trailing empty element from a stray comma.
        for (String bad : new String[] {"localhost", "localhost:", "localhost:abc", "localhost:0",
                "localhost:65536", "host1:9123,"}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> FlussCatalogProperties.of(
                            props(FlussCatalogProperties.BOOTSTRAP_SERVERS, bad)),
                    "expected '" + bad + "' to be rejected");
        }
    }

    @Test
    public void bootstrapServersAcceptsListsAndIpv6() {
        // The IPv6 case is why the port is split at the LAST colon, not the first.
        for (String good : new String[] {"localhost:9123", " host1:9123 , host2:9124 ", "[::1]:9123"}) {
            FlussCatalogProperties.of(props(FlussCatalogProperties.BOOTSTRAP_SERVERS, good));
        }
    }

    /**
     * The convention's load-bearing rule: bad VALUES are refused, unrecognized NAMES are not. The same
     * map carries engine keys and storage keys, and ALTER CATALOG can only overwrite a key, never remove
     * one — so a catalog that had been refused for an unknown key could not be repaired by any statement.
     */
    @Test
    public void unknownKeysAreAcceptedSoAlterCanAlwaysRepairACatalog() {
        FlussCatalogProperties p = bound(
                "type", "fluss",
                "meta.cache.ttl-second", "60",
                "s3.endpoint", "https://example",
                "fluss.some.option.a.future.release.adds", "1",
                "not.even.a.namespace.we.know", "x");
        Assertions.assertEquals("localhost:9123", p.getBootstrapServers());
    }

    @Test
    public void unionReadModeDefaultsToAutoAndIsCaseInsensitive() {
        Assertions.assertEquals(UnionReadMode.AUTO, bound().getUnionReadMode());
        Assertions.assertEquals(UnionReadMode.REQUIRED,
                bound(FlussCatalogProperties.UNION_READ_MODE, "ReQuIrEd").getUnionReadMode());
        Assertions.assertEquals(UnionReadMode.DISABLED,
                bound(FlussCatalogProperties.UNION_READ_MODE, " disabled ").getUnionReadMode());
    }

    @Test
    public void unionReadModeRejectsUnknownValueAtCreateCatalog() {
        // A typo here would otherwise degrade silently to whatever the default is, and the difference
        // between auto and required is only visible as "the query returned fewer rows than it should".
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> bound(FlussCatalogProperties.UNION_READ_MODE, "enabled"));
        Assertions.assertTrue(e.getMessage().contains("auto, required, disabled"),
                "message should list the accepted values, was: " + e.getMessage());
    }

    /**
     * The ceiling on how much log tail BE may hold while reading a primary-key table together with its
     * lake. Zero and negative are rejected rather than read as "no limit": that limit exists because BE
     * is a long-lived process shared by every other query, and there is deliberately no way to say
     * "unbounded".
     */
    @Test
    public void theTailCeilingDefaultsToTwoMillionRowsAndMustBePositive() {
        Assertions.assertEquals(2_000_000L, bound().getMaxTailRows());
        Assertions.assertEquals(500L,
                bound(FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS, " 500 ").getMaxTailRows());

        // "lots" fails in the binder (which names the key), 0 and -1 in the positivity rule.
        for (String bad : new String[] {"0", "-1", "lots"}) {
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> bound(FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS, bad),
                    "accepted '" + bad + "' as a row ceiling");
            Assertions.assertTrue(
                    e.getMessage().contains(FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS),
                    e.getMessage());
        }

        // A blank value is absent, framework-wide, so it reads as the default rather than as an error.
        // Uniform with every other connector's every other key, which is the point of the shared binder:
        // a connector that made blank mean something else here would be the one place a user has to
        // remember a local rule.
        Assertions.assertEquals(2_000_000L,
                bound(FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS, "").getMaxTailRows());
    }

    /**
     * The scan-wide companion of the per-bucket ceiling. It exists because the per-bucket one cannot see
     * the case that actually costs the memory: BE keeps the keys of every tail it has touched until the
     * scan ends, so many buckets each well inside their own ceiling still add up. A value below the
     * per-bucket ceiling describes a scan no read could satisfy, and is rejected rather than silently
     * making every union read impossible.
     */
    @Test
    public void theScanWideTailCeilingDefaultsToTenTailsAndCannotSitBelowThePerBucketOne() {
        Assertions.assertEquals(20_000_000L, bound().getMaxTotalTailRows());
        // Both, because lowering the scan-wide ceiling under the per-bucket default is itself the
        // contradiction this rule refuses -- see the cross-check below.
        Assertions.assertEquals(500L,
                bound(FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS, "100",
                        FlussCatalogProperties.UNION_READ_MAX_TOTAL_TAIL_ROWS, " 500 ")
                        .getMaxTotalTailRows());

        for (String bad : new String[] {"0", "-1", "lots"}) {
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> bound(FlussCatalogProperties.UNION_READ_MAX_TOTAL_TAIL_ROWS, bad),
                    "accepted '" + bad + "' as a scan-wide row ceiling");
            Assertions.assertTrue(
                    e.getMessage().contains(FlussCatalogProperties.UNION_READ_MAX_TOTAL_TAIL_ROWS),
                    e.getMessage());
        }

        IllegalArgumentException below = Assertions.assertThrows(IllegalArgumentException.class,
                () -> bound(FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS, "100",
                        FlussCatalogProperties.UNION_READ_MAX_TOTAL_TAIL_ROWS, "99"));
        Assertions.assertTrue(below.getMessage().contains("no union read could satisfy both"),
                below.getMessage());

        // Equal is the legitimate edge: "one maximal tail, and no more".
        Assertions.assertEquals(100L,
                bound(FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS, "100",
                        FlussCatalogProperties.UNION_READ_MAX_TOTAL_TAIL_ROWS, "100")
                        .getMaxTotalTailRows());
    }

    @Test
    public void clientConfigIsThePrefixedPropertiesMinusTheDorisOnlyOnes() {
        FlussCatalogProperties p = bound(
                "fluss.client.security.protocol", "sasl",
                FlussCatalogProperties.UNION_READ_MODE, "required",
                FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS, "10",
                FlussCatalogProperties.UNION_READ_MAX_TOTAL_TAIL_ROWS, "20",
                FlussCatalogProperties.ENABLE_MAPPING_VARBINARY, "true",
                FlussCatalogProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "true",
                "type", "fluss",
                "warehouse", "s3://ignored");

        // bootstrap.servers and client.* arrive under fluss's own names; the Doris-only union-read
        // switch and every non-fluss catalog property stay behind. The engine's own keys ("type") and
        // other connectors' keys ("warehouse") are not fluss options and must not be handed over as if
        // they were — the fluss config is not a place to dump whatever the catalog happened to carry.
        // The two type-mapping switches are in the input for the same reason: they steer Doris's own
        // schema rendering and fluss has no idea what they mean.
        Map<String, String> expected = new HashMap<>();
        expected.put("bootstrap.servers", "localhost:9123");
        expected.put("client.security.protocol", "sasl");
        Assertions.assertEquals(expected, p.getFlussClientConfig());
    }

    /**
     * The bootstrap servers are the one key that is both bound AND forwarded to the client under the
     * same name, so the two could disagree. The bound (trimmed) value has to win: it is the one every
     * other reader here sees and the one the error messages quote, and handing the client a different
     * string than the catalog validated is how a connection failure ends up naming an address nobody
     * configured.
     */
    @Test
    public void theClientGetsTheSameBootstrapServersEveryOtherReaderSees() {
        FlussCatalogProperties p = FlussCatalogProperties.of(
                props(FlussCatalogProperties.BOOTSTRAP_SERVERS, "  host1:9123,host2:9124  "));

        Assertions.assertEquals("host1:9123,host2:9124", p.getBootstrapServers());
        Assertions.assertEquals(p.getBootstrapServers(),
                p.getFlussClientConfig().get("bootstrap.servers"));
    }

    @Test
    public void typeMappingSwitchesDefaultToOffAndUseTheEngineWideNames() {
        // The names are deliberately the unprefixed, engine-wide ones the hive/paimon/iceberg catalogs
        // already answer to: a user who knows enable.mapping.varbinary must not have to discover a
        // fluss-specific spelling, and a misspelling here degrades silently to "switch is off".
        FlussTypeMapping.Options off = bound().getTypeMappingOptions();
        Assertions.assertFalse(off.isMapBinaryToVarbinary());
        Assertions.assertFalse(off.isMapTimestampTz());

        FlussTypeMapping.Options on = bound(
                "enable.mapping.varbinary", "true",
                "enable.mapping.timestamp_tz", "TRUE").getTypeMappingOptions();
        Assertions.assertTrue(on.isMapBinaryToVarbinary());
        Assertions.assertTrue(on.isMapTimestampTz());

        // Anything that is not "true" is off, matching how every other catalog reads these.
        FlussTypeMapping.Options garbage =
                bound(FlussCatalogProperties.ENABLE_MAPPING_VARBINARY, "yes").getTypeMappingOptions();
        Assertions.assertFalse(garbage.isMapBinaryToVarbinary());
    }
}
