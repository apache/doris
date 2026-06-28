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

import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Parity tests for the listing internals of {@link CatalogBackedIcebergCatalogOps} (P6-T09): nested
 * namespace recursion, view filtering, and dotted-namespace / external-catalog-name construction —
 * mirroring legacy {@code IcebergMetadataOps}. Drives the real seam against the fail-loud
 * {@link FakeIcebergCatalog} hierarchy (no Mockito), asserting the EXACT names returned AND the exact
 * namespaces the seam constructed.
 */
public class CatalogBackedIcebergCatalogOpsTest {

    private static IcebergCatalogOps ops(Catalog catalog, boolean restFlavor, boolean nestedNamespaceEnabled,
            boolean viewEnabled, Optional<String> externalCatalogName) {
        return new CatalogBackedIcebergCatalogOps(
                catalog, restFlavor, nestedNamespaceEnabled, viewEnabled, externalCatalogName);
    }

    // ---------------------------------------------------------------------
    // listDatabaseNames — nested-namespace recursion (G3)
    // ---------------------------------------------------------------------

    @Test
    public void listDatabaseNamesReturnsLastLevelWhenNotNested() {
        // WHY: in the default (non-nested) mode legacy maps each listed namespace to its LAST level
        // (n.level(n.length()-1)). A nested namespace [a,b] surfaces only as "b". MUTATION: emitting the
        // full dotted name, or recursing when nested is off -> red.
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();
        catalog.childNamespaces.put(Namespace.empty(),
                Arrays.asList(Namespace.of("db_a"), Namespace.of("a", "b")));

        List<String> result = ops(catalog, false, false, true, Optional.empty()).listDatabaseNames();

        Assertions.assertEquals(Arrays.asList("db_a", "b"), result,
                "non-nested mode must return each namespace's last level only");
    }

    @Test
    public void listDatabaseNamesRecursesDottedWhenRestAndNestedEnabled() {
        // WHY: legacy recurses ONLY for a REST catalog with iceberg.rest.nested-namespace-enabled=true,
        // emitting each child's dotted toString() then flat-mapping its descendants. Root -> [a]; a ->
        // [a.b]; a.b -> []. Result must be ["a", "a.b"] in that order. MUTATION: last-level-only, or
        // wrong recursion order -> red.
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();
        catalog.childNamespaces.put(Namespace.empty(), Collections.singletonList(Namespace.of("a")));
        catalog.childNamespaces.put(Namespace.of("a"), Collections.singletonList(Namespace.of("a", "b")));
        catalog.childNamespaces.put(Namespace.of("a", "b"), Collections.emptyList());

        List<String> result = ops(catalog, true, true, true, Optional.empty()).listDatabaseNames();

        Assertions.assertEquals(Arrays.asList("a", "a.b"), result,
                "REST + nested-enabled must recurse and emit dotted namespace names depth-first");
    }

    @Test
    public void listDatabaseNamesDoesNotRecurseWhenNestedFlagButNotRest() {
        // WHY: legacy gates recursion on `dorisCatalog instanceof IcebergRestExternalCatalog` AS WELL AS
        // the flag, so a non-REST flavor with the nested flag set must STILL fall back to last-level.
        // MUTATION: recursing on the flag alone (ignoring the REST gate) -> red.
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();
        catalog.childNamespaces.put(Namespace.empty(), Collections.singletonList(Namespace.of("a", "b")));

        List<String> result = ops(catalog, false, true, true, Optional.empty()).listDatabaseNames();

        Assertions.assertEquals(Collections.singletonList("b"), result,
                "nested flag without REST flavor must not recurse");
    }

    @Test
    public void listDatabaseNamesUsesExternalCatalogNameAsRoot() {
        // WHY: legacy roots the listing at Namespace.of(externalCatalogName) when present (3-level REST
        // catalogs), else Namespace.empty(). MUTATION: always rooting at Namespace.empty() -> the seam
        // lists the wrong parent -> empty result -> red.
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();
        catalog.childNamespaces.put(Namespace.of("cat"),
                Collections.singletonList(Namespace.of("cat", "db1")));

        List<String> result = ops(catalog, false, false, true, Optional.of("cat")).listDatabaseNames();

        Assertions.assertEquals(Collections.singletonList("db1"), result,
                "external-catalog-name must root the namespace listing");
        Assertions.assertTrue(catalog.log.contains("listNamespaces:cat"),
                "listing must start from the external-catalog-name namespace");
    }

    @Test
    public void listDatabaseNamesEmptyWhenCatalogHasNoNamespaceSupport() {
        // WHY: the guard for a catalog without SupportsNamespaces must be preserved — return empty, never
        // throw. MUTATION: dropping the instanceof guard -> ClassCastException -> red (error).
        List<String> result =
                ops(new PlainIcebergCatalog(), false, false, true, Optional.empty()).listDatabaseNames();

        Assertions.assertTrue(result.isEmpty(),
                "a catalog without namespace support must yield an empty database list");
    }

    // ---------------------------------------------------------------------
    // listTableNames — view filtering (G4)
    // ---------------------------------------------------------------------

    @Test
    public void listTableNamesFiltersOutViewsWhenViewCatalogEnabled() {
        // WHY: iceberg's listTables returns views too, so legacy subtracts the names returned by
        // listViews when the catalog is a (view-enabled) ViewCatalog. MUTATION: not filtering, or
        // filtering the wrong set -> red.
        FakeIcebergViewCatalog catalog = new FakeIcebergViewCatalog();
        catalog.tablesByNs.put(Namespace.of("db1"), Arrays.asList("t1", "v1", "t2"));
        catalog.viewsByNs.put(Namespace.of("db1"), Collections.singletonList("v1"));

        List<String> result = ops(catalog, true, false, true, Optional.empty()).listTableNames("db1");

        Assertions.assertEquals(Arrays.asList("t1", "t2"), result,
                "view names returned by listTables must be filtered out");
    }

    @Test
    public void listTableNamesDoesNotFilterWhenNotViewCatalog() {
        // WHY: a plain Catalog (not a ViewCatalog) cannot list views, so legacy returns the table list
        // unfiltered. MUTATION: attempting to cast to ViewCatalog / filtering anyway -> red.
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();
        catalog.tablesByNs.put(Namespace.of("db1"), Arrays.asList("t1", "v1"));

        List<String> result = ops(catalog, true, false, true, Optional.empty()).listTableNames("db1");

        Assertions.assertEquals(Arrays.asList("t1", "v1"), result,
                "a non-ViewCatalog must return the table list unfiltered");
    }

    @Test
    public void listTableNamesDoesNotFilterWhenRestViewDisabled() {
        // WHY: even a ViewCatalog must skip view filtering when iceberg.rest.view-enabled=false on a REST
        // catalog (isViewCatalogEnabled() returns false), and listViews must NOT be called at all.
        // MUTATION: ignoring the view-enabled flag, or calling listViews regardless -> red.
        FakeIcebergViewCatalog catalog = new FakeIcebergViewCatalog();
        catalog.tablesByNs.put(Namespace.of("db1"), Arrays.asList("t1", "v1"));
        catalog.viewsByNs.put(Namespace.of("db1"), Collections.singletonList("v1"));

        List<String> result = ops(catalog, true, false, false, Optional.empty()).listTableNames("db1");

        Assertions.assertEquals(Arrays.asList("t1", "v1"), result,
                "REST view-disabled must return the table list unfiltered");
        Assertions.assertFalse(catalog.log.contains("listViews:db1"),
                "listViews must not be called when view filtering is disabled");
    }

    // ---------------------------------------------------------------------
    // listViewNames / viewExists — the inverse of listTableNames' view subtraction (B0 view SPI)
    // ---------------------------------------------------------------------

    @Test
    public void listViewNamesReturnsViewsWhenViewCatalogEnabled() {
        // WHY: the catalog re-merges these into SHOW TABLES (listTableNames subtracts them). The real impl
        // must surface the ViewCatalog's listViews names. MUTATION: returning empty / not casting to
        // ViewCatalog -> views vanish from SHOW TABLES -> red.
        FakeIcebergViewCatalog catalog = new FakeIcebergViewCatalog();
        catalog.viewsByNs.put(Namespace.of("db1"), Arrays.asList("v1", "v2"));

        List<String> result = ops(catalog, true, false, true, Optional.empty()).listViewNames("db1");

        Assertions.assertEquals(Arrays.asList("v1", "v2"), result);
    }

    @Test
    public void listViewNamesEmptyWhenNotViewCatalog() {
        // WHY: a plain Catalog (not a ViewCatalog) has no views. MUTATION: dropping the isViewCatalogEnabled
        // gate -> a ClassCastException on the non-ViewCatalog -> red.
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();

        List<String> result = ops(catalog, true, false, true, Optional.empty()).listViewNames("db1");

        Assertions.assertTrue(result.isEmpty(), "a non-ViewCatalog must report no views");
    }

    @Test
    public void listViewNamesEmptyWhenRestViewDisabled() {
        // WHY: even a ViewCatalog reports no views when iceberg.rest.view-enabled=false, and listViews must
        // NOT be called. MUTATION: ignoring the view-enabled flag -> listViews called / views surface -> red.
        FakeIcebergViewCatalog catalog = new FakeIcebergViewCatalog();
        catalog.viewsByNs.put(Namespace.of("db1"), Collections.singletonList("v1"));

        List<String> result = ops(catalog, true, false, false, Optional.empty()).listViewNames("db1");

        Assertions.assertTrue(result.isEmpty(), "REST view-disabled must report no views");
        Assertions.assertFalse(catalog.log.contains("listViews:db1"),
                "listViews must not be called when view filtering is disabled");
    }

    @Test
    public void viewExistsTrueOnlyForKnownViewWhenViewCatalogEnabled() {
        // WHY: PluginDrivenExternalTable.isView() resolves from this; it must report true exactly for a view
        // name. MUTATION: hard-coding true/false, or checking the wrong name -> red on one of the two cases.
        FakeIcebergViewCatalog catalog = new FakeIcebergViewCatalog();
        catalog.viewsByNs.put(Namespace.of("db1"), Collections.singletonList("v1"));

        Assertions.assertTrue(ops(catalog, true, false, true, Optional.empty()).viewExists("db1", "v1"));
        Assertions.assertFalse(ops(catalog, true, false, true, Optional.empty()).viewExists("db1", "t1"),
                "a non-view name must not report as a view");
    }

    @Test
    public void viewExistsFalseWhenNotViewCatalogOrRestDisabled() {
        // WHY: the isViewCatalogEnabled gate must short-circuit viewExists to false for a plain Catalog and
        // for a view-disabled REST catalog (never casting / calling viewExists on them). MUTATION: dropping
        // the gate -> ClassCastException on the plain catalog, or a true on the disabled one -> red.
        FakeIcebergCatalog plain = new FakeIcebergCatalog();
        Assertions.assertFalse(ops(plain, true, false, true, Optional.empty()).viewExists("db1", "v1"),
                "a non-ViewCatalog reports no views");

        FakeIcebergViewCatalog disabled = new FakeIcebergViewCatalog();
        disabled.viewsByNs.put(Namespace.of("db1"), Collections.singletonList("v1"));
        Assertions.assertFalse(ops(disabled, true, false, false, Optional.empty()).viewExists("db1", "v1"),
                "REST view-disabled gates viewExists to false");
    }

    // ---------------------------------------------------------------------
    // namespace construction — dotted split + external-catalog-name append
    // ---------------------------------------------------------------------

    @Test
    public void listTableNamesSplitsDottedDbAndAppendsExternalCatalogName() {
        // WHY: legacy getNamespace splits the db name on '.' (omit empties / trim) and appends the
        // external-catalog-name at the end. "a.b" + cat -> Namespace.of(a, b, cat). MUTATION: treating
        // "a.b" as a single level, or prepending the catalog name -> red.
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();

        ops(catalog, false, false, true, Optional.of("cat")).listTableNames("a.b");

        Assertions.assertEquals(Namespace.of("a", "b", "cat"), catalog.lastListTablesNs,
                "dotted db name must split into levels with external-catalog-name appended last");
    }

    @Test
    public void listTableNamesTrimsUnicodeWhitespaceLikeGuava() {
        // WHY: legacy getNamespace splits via Guava Splitter.trimResults(), whose trimming is
        // CharMatcher.whitespace() -- it strips the Unicode whitespace chars above U+0020 (e.g. the
        // ideographic space U+3000), which plain String.trim() (only <= U+0020) does NOT. A db name
        // with U+3000 edges must therefore trim to the same namespace legacy produces. MUTATION: plain
        // String.trim() leaves the U+3000 -> a different Iceberg namespace -> red. (NBSP U+00A0 is
        // intentionally avoided: Guava whitespace() excludes it, so it is not a divergence.)
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();

        ops(catalog, false, false, true, Optional.empty()).listTableNames("\u3000db1\u3000");

        Assertions.assertEquals(Namespace.of("db1"), catalog.lastListTablesNs,
                "U+3000 whitespace edges must be trimmed, matching legacy Guava trimResults()");
    }

    @Test
    public void databaseExistsSplitsDottedNamespace() {
        // WHY: databaseExists must check the SPLIT multi-level namespace, not the dotted string as a
        // single level. MUTATION: Namespace.of("a.b") (one level) -> miss -> red.
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();
        catalog.existingNamespaces.add(Namespace.of("a", "b"));

        boolean exists = ops(catalog, false, false, true, Optional.empty()).databaseExists("a.b");

        Assertions.assertTrue(exists, "a dotted db name must resolve to the multi-level namespace");
        Assertions.assertEquals(Namespace.of("a", "b"), catalog.lastNamespaceExistsNs,
                "databaseExists must build the multi-level namespace from the dotted name");
    }

    @Test
    public void databaseExistsFalseWhenNoNamespaceSupport() {
        // WHY: the no-namespace-support guard returns false (never throws). MUTATION: dropping the guard
        // -> ClassCastException -> red.
        boolean exists =
                ops(new PlainIcebergCatalog(), false, false, true, Optional.empty()).databaseExists("db1");

        Assertions.assertFalse(exists, "a catalog without namespace support reports no databases");
    }

    @Test
    public void tableExistsSplitsDottedNamespace() {
        // WHY: tableExists must build TableIdentifier.of(<split namespace>, table). MUTATION: single-level
        // namespace from a dotted db -> wrong identifier -> miss -> red.
        FakeIcebergCatalog catalog = new FakeIcebergCatalog();
        catalog.existingTables.add(TableIdentifier.of(Namespace.of("a", "b"), "t1"));

        boolean exists = ops(catalog, false, false, true, Optional.empty()).tableExists("a.b", "t1");

        Assertions.assertTrue(exists, "a dotted db name must resolve to the multi-level table identifier");
        Assertions.assertEquals(TableIdentifier.of(Namespace.of("a", "b"), "t1"), catalog.lastTableExistsId,
                "tableExists must build the identifier from the split namespace");
    }
}
