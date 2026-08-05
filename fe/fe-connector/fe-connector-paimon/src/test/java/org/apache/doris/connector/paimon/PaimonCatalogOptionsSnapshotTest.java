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

import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * WHOLE-MAP snapshots of {@link PaimonCatalogFactory#buildCatalogOptions}, one per flavor.
 *
 * <p><b>Why a snapshot and not more per-key assertions.</b> {@link PaimonCatalogFactoryTest} asserts
 * that the keys it names are right; nothing there notices a key that appears, disappears, or is spelled
 * differently. The catalog Options ARE the connection: paimon silently ignores an option it does not
 * recognize, so a dropped or misspelled key does not throw — it produces a catalog that connects with
 * different settings than the operator asked for. These tests assert the ENTIRE map, so any drift shows
 * up as a diff.
 *
 * <p>They exist because the per-flavor assembly is being folded onto the bound
 * {@code *MetaStoreProperties} holders, retiring the parallel raw-map scan. Once the old path is gone
 * there is nothing left to compare against, so the reference is captured HERE, before the change, and
 * kept afterwards as the permanent guard that holder and assembly stay in agreement.
 *
 * <p>Each input map deliberately carries the awkward keys as well as the ordinary ones: an alias form,
 * a generic {@code paimon.*} passthrough, and the three namespaces the passthrough must exclude
 * ({@code paimon.s3.*} storage, {@code paimon.table-option.*} per-table, {@code paimon.jni.*} BE knob).
 *
 * <p><b>A leak these snapshots caught and deliberately keep.</b> {@code paimon.catalog.type} is a
 * Doris-side key, but it matches the generic {@code paimon.} passthrough like any other, so every
 * catalog emits a {@code catalog.type} option paimon does not define. It is inert (paimon's
 * {@code Options} is an open bag) and dropping it would change what a live catalog is built with, so it
 * is pinned here rather than quietly removed while the assembly is being reworked. Cleaning it up is a
 * separate, deliberate change.
 */
public class PaimonCatalogOptionsSnapshotTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /** Sorted so an assertion failure renders as a readable diff of the whole option map. */
    private static void assertOptions(Map<String, String> expected, Map<String, String> input) {
        Options actual = PaimonCatalogFactory.buildCatalogOptions(PaimonCatalogProperties.of(input));
        Assertions.assertEquals(new TreeMap<>(expected), new TreeMap<>(actual.toMap()));
    }

    /**
     * The three excluded namespaces, appended to every input map. None of them may reach the catalog
     * Options: storage keys belong in the Hadoop Configuration, table options are applied per table on
     * load, and the jni namespace is a BE scanner knob forwarded by the scan-plan provider.
     */
    private static Map<String, String> withExcludedNamespaces(Map<String, String> base) {
        base.put("paimon.s3.access-key", "must-not-leak");
        base.put("paimon.table-option.scan.snapshot-id", "7");
        base.put("paimon.jni.batch-size", "2048");
        return base;
    }

    @Test
    public void filesystemSnapshot() {
        assertOptions(
                props("metastore", "filesystem",
                        "catalog.type", "filesystem",
                        "warehouse", "/wh",
                        "read.batch-size", "4096"),
                withExcludedNamespaces(props(
                        "paimon.catalog.type", "filesystem",
                        "warehouse", "/wh",
                        "paimon.read.batch-size", "4096",
                        // Hadoop-recognized keys ride the Configuration, never the catalog Options.
                        "fs.defaultFS", "hdfs://nn:8020")));
    }

    /** No flavor key at all: the default flavor is filesystem. */
    @Test
    public void defaultFlavorSnapshotIsFilesystem() {
        assertOptions(
                props("metastore", "filesystem",
                        "warehouse", "/wh"),
                props("warehouse", "/wh"));
    }

    @Test
    public void hmsSnapshotWithDefaults() {
        assertOptions(
                props("metastore", "hive",
                        "catalog.type", "hms",
                        "warehouse", "/wh",
                        "uri", "thrift://nn:9083",
                        // Both are emitted unconditionally, at their legacy defaults when unset.
                        "client-pool-cache.eviction-interval-ms", "300000",
                        "location-in-properties", "false",
                        "read.batch-size", "4096"),
                withExcludedNamespaces(props(
                        "paimon.catalog.type", "hms",
                        "warehouse", "/wh",
                        "hive.metastore.uris", "thrift://nn:9083",
                        "paimon.read.batch-size", "4096",
                        // hive.* keys feed the HiveConf, not the catalog Options.
                        "hive.conf.resources", "hive-site.xml")));
    }

    /** The bare {@code uri} alias plus explicit overrides of both hms defaults. */
    @Test
    public void hmsSnapshotWithUriAliasAndOverrides() {
        assertOptions(
                props("metastore", "hive",
                        "catalog.type", "hms",
                        "warehouse", "/wh",
                        "uri", "thrift://alias:9083",
                        "client-pool-cache.eviction-interval-ms", "60000",
                        "location-in-properties", "true"),
                props("paimon.catalog.type", "hms",
                        "warehouse", "/wh",
                        "uri", "thrift://alias:9083",
                        "client-pool-cache.eviction-interval-ms", "60000",
                        "location-in-properties", "true"));
    }

    /**
     * A padded metastore uri reaches the catalog Options TRIMMED, because the option now comes from the
     * bound hms properties instead of a second scan of the raw map.
     *
     * <p>This is a deliberate behaviour change, and it removes an inconsistency rather than introducing
     * one: the HiveConf this same catalog connects with has always been built from the bound value, so
     * before this change one catalog would talk to the metastore as "thrift://nn:9083" while its paimon
     * Options claimed "thrift://nn:9083 ".
     */
    @Test
    public void hmsSnapshotTrimsThePaddedUriLikeTheHiveConfAlwaysDid() {
        assertOptions(
                props("metastore", "hive",
                        "catalog.type", "hms",
                        "warehouse", "/wh",
                        "uri", "thrift://nn:9083",
                        "client-pool-cache.eviction-interval-ms", "300000",
                        "location-in-properties", "false"),
                props("paimon.catalog.type", "hms",
                        "warehouse", " /wh ",
                        "hive.metastore.uris", " thrift://nn:9083 "));
    }

    /**
     * REST emits every {@code paimon.rest.*} key TWICE: once as {@code rest.<x>} (the generic
     * {@code paimon.} passthrough in the common appender) and once as {@code <x>} (the rest appender's
     * own prefix strip). Pinned because it is surprising, not because it is desirable — the rest
     * appender's strip is the one paimon actually reads.
     */
    @Test
    public void restSnapshotEmitsBothStrippedForms() {
        assertOptions(
                props("metastore", "rest",
                        "catalog.type", "rest",
                        "warehouse", "/wh",
                        "uri", "http://rest:8080",
                        "token.provider", "dlf",
                        "dlf.access-key-id", "ak",
                        "rest.uri", "http://rest:8080",
                        "rest.token.provider", "dlf",
                        "rest.dlf.access-key-id", "ak",
                        "read.batch-size", "4096"),
                withExcludedNamespaces(props(
                        "paimon.catalog.type", "rest",
                        "warehouse", "/wh",
                        "paimon.rest.uri", "http://rest:8080",
                        "paimon.rest.token.provider", "dlf",
                        "paimon.rest.dlf.access-key-id", "ak",
                        "paimon.read.batch-size", "4096")));
    }

    /** The rest flavor also accepts the bare {@code uri} alias, which carries no prefix to strip. */
    @Test
    public void restSnapshotWithUriAlias() {
        assertOptions(
                props("metastore", "rest",
                        "catalog.type", "rest",
                        "warehouse", "/wh",
                        "uri", "http://rest:8080"),
                props("paimon.catalog.type", "rest",
                        "warehouse", "/wh",
                        "uri", "http://rest:8080"));
    }

    /**
     * The rest flavor's {@code uri} comes from the BOUND value, so it wins over the same key arriving
     * through the {@code paimon.rest.} prefix strip.
     *
     * <p>Both write to {@code uri}: {@code paimon.rest.uri} strips down onto it verbatim, and the
     * alias-resolved value is written last. Without that ordering a padded {@code paimon.rest.uri} would
     * connect with a value {@code validate()} never saw. The generic passthrough's own {@code rest.uri}
     * copy stays verbatim -- it is a wildcard forward of keys this connector does not interpret.
     */
    @Test
    public void restSnapshotUriComesFromTheBoundValue() {
        assertOptions(
                props("metastore", "rest",
                        "catalog.type", "rest",
                        "warehouse", "/wh",
                        "uri", "http://rest:8080",
                        "rest.uri", " http://rest:8080 "),
                props("paimon.catalog.type", "rest",
                        "warehouse", " /wh ",
                        "paimon.rest.uri", " http://rest:8080 "));
    }

    /**
     * JDBC mixes all three sources: the {@code paimon.} passthrough, the alias-resolved
     * user/password, and the raw {@code jdbc.*} passthrough that fills in whatever the first two left
     * unset. Both alias spellings are exercised (user via {@code paimon.jdbc.}, password via bare
     * {@code jdbc.}).
     */
    @Test
    public void jdbcSnapshot() {
        assertOptions(
                props("metastore", "jdbc",
                        "catalog.type", "jdbc",
                        "warehouse", "/wh",
                        "uri", "jdbc:mysql://db:3306/meta",
                        "jdbc.user", "alice",
                        "jdbc.password", "secret",
                        "jdbc.driver_url", "mysql.jar",
                        "jdbc.driver_class", "com.mysql.cj.jdbc.Driver",
                        "jdbc.foo", "bar",
                        "read.batch-size", "4096"),
                withExcludedNamespaces(props(
                        "paimon.catalog.type", "jdbc",
                        "warehouse", "/wh",
                        "uri", "jdbc:mysql://db:3306/meta",
                        "paimon.jdbc.user", "alice",
                        "jdbc.password", "secret",
                        "paimon.jdbc.driver_url", "mysql.jar",
                        "paimon.jdbc.driver_class", "com.mysql.cj.jdbc.Driver",
                        "jdbc.foo", "bar",
                        "paimon.read.batch-size", "4096")));
    }

    /** JDBC with only the {@code paimon.jdbc.uri} alias and no optional key at all. */
    @Test
    public void jdbcSnapshotMinimal() {
        assertOptions(
                props("metastore", "jdbc",
                        "catalog.type", "jdbc",
                        "warehouse", "/wh",
                        "uri", "jdbc:mysql://db:3306/meta",
                        "jdbc.uri", "jdbc:mysql://db:3306/meta"),
                props("paimon.catalog.type", "jdbc",
                        "warehouse", "/wh",
                        "paimon.jdbc.uri", "jdbc:mysql://db:3306/meta"));
    }
}
