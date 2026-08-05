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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.cache.CacheSpec;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * What the catalog remembers between statements, and what each REFRESH is supposed to forget.
 *
 * <p>Every test counts how often the source was asked, because the point of this cache is not that the
 * answers are right -- they were right before it existed -- but that the remote calls stop happening. A test
 * that only compared returned values would pass against a cache that never caches.
 */
class AdbcMetadataCacheTest {

    private static final AdbcNamespace MAIN = new AdbcNamespace("main", "");
    private static final AdbcNamespace OTHER = new AdbcNamespace("other", "");
    private static final AdbcTableHandle MAIN_T1 = new AdbcTableHandle(MAIN, "t1");
    private static final AdbcTableHandle MAIN_T2 = new AdbcTableHandle(MAIN, "t2");
    private static final AdbcTableHandle OTHER_T1 = new AdbcTableHandle(OTHER, "t1");

    private static final Schema SCHEMA = new Schema(Collections.emptyList());

    private static AdbcMetadataCache cacheWith(String... keysAndValues) {
        Map<String, String> properties = new java.util.HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            properties.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return new AdbcMetadataCache(properties);
    }

    // ========= what is remembered =========

    @Test
    void theDatabaseListingIsReadFromTheSourceOnce() {
        AdbcMetadataCache cache = cacheWith();
        Counting<List<AdbcNamespace>> source = new Counting<>(List.of(MAIN));

        Assertions.assertEquals(List.of(MAIN), cache.namespaces(source));
        Assertions.assertEquals(List.of(MAIN), cache.namespaces(source));

        Assertions.assertEquals(1, source.calls);
    }

    @Test
    void tableListingsAreRememberedPerDatabase() {
        AdbcMetadataCache cache = cacheWith();
        Counting<List<String>> main = new Counting<>(List.of("t1"));
        Counting<List<String>> other = new Counting<>(List.of("t9"));

        Assertions.assertEquals(List.of("t1"), cache.tableNames(MAIN, main));
        Assertions.assertEquals(List.of("t9"), cache.tableNames(OTHER, other));
        cache.tableNames(MAIN, main);
        cache.tableNames(OTHER, other);

        // One key per database: a shared key would serve one database's tables under another's name.
        Assertions.assertEquals(1, main.calls);
        Assertions.assertEquals(1, other.calls);
    }

    @Test
    void schemasAreRememberedPerTable() {
        AdbcMetadataCache cache = cacheWith();
        Counting<Schema> t1 = new Counting<>(SCHEMA);
        Counting<Schema> t2 = new Counting<>(SCHEMA);

        cache.tableSchema(MAIN_T1, t1);
        cache.tableSchema(MAIN_T2, t2);
        cache.tableSchema(MAIN_T1, t1);
        cache.tableSchema(MAIN_T2, t2);

        Assertions.assertEquals(1, t1.calls);
        Assertions.assertEquals(1, t2.calls);
    }

    @Test
    void twoTablesOfTheSameNameInDifferentDatabasesAreNotConfused() {
        AdbcMetadataCache cache = cacheWith();
        Counting<Schema> main = new Counting<>(SCHEMA);
        Counting<Schema> other = new Counting<>(SCHEMA);

        cache.tableSchema(MAIN_T1, main);
        cache.tableSchema(OTHER_T1, other);

        // Keying on the bare table name would serve main.t1's columns for other.t1.
        Assertions.assertEquals(1, main.calls);
        Assertions.assertEquals(1, other.calls);
    }

    // ========= what a REFRESH forgets =========

    @Test
    void refreshTableForgetsThatTablesSchema() {
        AdbcMetadataCache cache = cacheWith();
        Counting<Schema> t1 = new Counting<>(SCHEMA);
        cache.tableSchema(MAIN_T1, t1);

        cache.invalidateTable("main", "t1");
        cache.tableSchema(MAIN_T1, t1);

        Assertions.assertEquals(2, t1.calls);
    }

    /**
     * Decision D. A user typing REFRESH TABLE means "get to know this table again", and a table that was
     * created remotely after the listing was cached cannot be got to know at all while the listing that
     * omits it survives -- REFRESH TABLE would have no way to ever make it appear.
     */
    @Test
    void refreshTableAlsoForgetsItsDatabaseTableListing() {
        AdbcMetadataCache cache = cacheWith();
        Counting<List<String>> listing = new Counting<>(List.of("t1"));
        cache.tableNames(MAIN, listing);

        cache.invalidateTable("main", "t1");
        cache.tableNames(MAIN, listing);

        Assertions.assertEquals(2, listing.calls);
    }

    @Test
    void refreshTableLeavesTheRestOfTheCatalogAlone() {
        AdbcMetadataCache cache = cacheWith();
        Counting<Schema> sibling = new Counting<>(SCHEMA);
        Counting<Schema> otherDb = new Counting<>(SCHEMA);
        Counting<List<String>> otherListing = new Counting<>(List.of("t9"));
        Counting<List<AdbcNamespace>> databases = new Counting<>(List.of(MAIN, OTHER));
        cache.tableSchema(MAIN_T2, sibling);
        cache.tableSchema(OTHER_T1, otherDb);
        cache.tableNames(OTHER, otherListing);
        cache.namespaces(databases);

        cache.invalidateTable("main", "t1");

        cache.tableSchema(MAIN_T2, sibling);
        cache.tableSchema(OTHER_T1, otherDb);
        cache.tableNames(OTHER, otherListing);
        cache.namespaces(databases);
        Assertions.assertEquals(1, sibling.calls);
        Assertions.assertEquals(1, otherDb.calls);
        Assertions.assertEquals(1, otherListing.calls);
        Assertions.assertEquals(1, databases.calls);
    }

    @Test
    void refreshDatabaseForgetsItsListingAndEverySchemaInIt() {
        AdbcMetadataCache cache = cacheWith();
        Counting<List<String>> listing = new Counting<>(List.of("t1", "t2"));
        Counting<Schema> t1 = new Counting<>(SCHEMA);
        Counting<Schema> t2 = new Counting<>(SCHEMA);
        cache.tableNames(MAIN, listing);
        cache.tableSchema(MAIN_T1, t1);
        cache.tableSchema(MAIN_T2, t2);

        cache.invalidateDb("main");

        cache.tableNames(MAIN, listing);
        cache.tableSchema(MAIN_T1, t1);
        cache.tableSchema(MAIN_T2, t2);
        Assertions.assertEquals(2, listing.calls);
        Assertions.assertEquals(2, t1.calls);
        Assertions.assertEquals(2, t2.calls);
    }

    @Test
    void refreshDatabaseLeavesAnotherDatabaseAlone() {
        AdbcMetadataCache cache = cacheWith();
        Counting<Schema> otherDb = new Counting<>(SCHEMA);
        Counting<List<String>> otherListing = new Counting<>(List.of("t9"));
        cache.tableSchema(OTHER_T1, otherDb);
        cache.tableNames(OTHER, otherListing);

        cache.invalidateDb("main");

        cache.tableSchema(OTHER_T1, otherDb);
        cache.tableNames(OTHER, otherListing);
        Assertions.assertEquals(1, otherDb.calls);
        Assertions.assertEquals(1, otherListing.calls);
    }

    /**
     * Only REFRESH CATALOG forgets which databases exist: it is the one statement a user reaches for when
     * the shape of the catalog itself changed, and it is also the only one that names no database to
     * invalidate.
     */
    @Test
    void refreshCatalogForgetsEverythingIncludingWhichDatabasesExist() {
        AdbcMetadataCache cache = cacheWith();
        Counting<List<AdbcNamespace>> databases = new Counting<>(List.of(MAIN));
        Counting<List<String>> listing = new Counting<>(List.of("t1"));
        Counting<Schema> t1 = new Counting<>(SCHEMA);
        cache.namespaces(databases);
        cache.tableNames(MAIN, listing);
        cache.tableSchema(MAIN_T1, t1);

        cache.invalidateAll();

        cache.namespaces(databases);
        cache.tableNames(MAIN, listing);
        cache.tableSchema(MAIN_T1, t1);
        Assertions.assertEquals(2, databases.calls);
        Assertions.assertEquals(2, listing.calls);
        Assertions.assertEquals(2, t1.calls);
    }

    // ========= never answering "no such object" from memory (decision C) =========

    @Test
    void tableListingCanBeReReadOnDemand() {
        AdbcMetadataCache cache = cacheWith();
        Counting<List<String>> listing = new Counting<>(List.of("t1"));
        cache.tableNames(MAIN, listing);

        cache.reloadTableNames(MAIN, listing);

        // The caller reaches for this having failed to find a name, so a cached answer is exactly what it
        // must not get; the fresh answer then replaces the stale one for everybody else.
        Assertions.assertEquals(2, listing.calls);
        cache.tableNames(MAIN, listing);
        Assertions.assertEquals(2, listing.calls);
    }

    @Test
    void theDatabaseListingCanBeReReadOnDemand() {
        AdbcMetadataCache cache = cacheWith();
        Counting<List<AdbcNamespace>> databases = new Counting<>(List.of(MAIN));
        cache.namespaces(databases);

        cache.reloadNamespaces(databases);

        Assertions.assertEquals(2, databases.calls);
        cache.namespaces(databases);
        Assertions.assertEquals(2, databases.calls);
    }

    // ========= configuration =========

    /**
     * Decision B, and the one number here that differs from every other connector: the framework default is
     * 24 hours, ADBC uses 10 minutes. An ADBC source is another live database rather than a warehouse of
     * files, so its tables change under Doris far more often, and this bounds how long a user who forgot to
     * REFRESH stays wrong. It is a decision, not an oversight -- do not "fix" it back to the framework value.
     */
    @Test
    void metadataIsForgottenAfterTenMinutesByDefault() {
        CacheSpec spec = AdbcMetadataCache.cacheSpec(Map.of());

        Assertions.assertTrue(spec.isEnable());
        Assertions.assertEquals(600L, spec.getTtlSecond());
        Assertions.assertEquals(1000L, spec.getCapacity());
    }

    @Test
    void everyCacheKnobIsReadFromTheCatalogProperties() {
        CacheSpec spec = AdbcMetadataCache.cacheSpec(Map.of(
                "meta.cache.adbc.metadata.enable", "false",
                "meta.cache.adbc.metadata.ttl-second", "42",
                "meta.cache.adbc.metadata.capacity", "7"));

        Assertions.assertFalse(spec.isEnable());
        Assertions.assertEquals(42L, spec.getTtlSecond());
        Assertions.assertEquals(7L, spec.getCapacity());
    }

    @Test
    void turningTheCacheOffSendsEveryReadBackToTheSource() {
        AdbcMetadataCache cache = cacheWith("meta.cache.adbc.metadata.enable", "false");
        Counting<List<AdbcNamespace>> databases = new Counting<>(List.of(MAIN));
        Counting<List<String>> listing = new Counting<>(List.of("t1"));
        Counting<Schema> t1 = new Counting<>(SCHEMA);

        for (int i = 0; i < 2; i++) {
            cache.namespaces(databases);
            cache.tableNames(MAIN, listing);
            cache.tableSchema(MAIN_T1, t1);
        }

        Assertions.assertEquals(2, databases.calls);
        Assertions.assertEquals(2, listing.calls);
        Assertions.assertEquals(2, t1.calls);
    }

    @Test
    void zeroTtlAlsoMeansNoCaching() {
        AdbcMetadataCache cache = cacheWith("meta.cache.adbc.metadata.ttl-second", "0");
        Counting<Schema> t1 = new Counting<>(SCHEMA);

        cache.tableSchema(MAIN_T1, t1);
        cache.tableSchema(MAIN_T1, t1);

        Assertions.assertEquals(2, t1.calls);
    }

    /** A supplier that answers the same thing every time and records how often it was asked. */
    private static final class Counting<T> implements Supplier<T> {

        private final T value;
        private int calls;

        private Counting(T value) {
            this.value = value;
        }

        @Override
        public T get() {
            calls++;
            return value;
        }
    }
}
