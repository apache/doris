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
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Catalog-backed tests preserving namespace identity between discovery and metadata operations. */
public class IcebergRootNamespaceTest {
    private InMemoryCatalog catalog;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("root-namespace", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("root"));
        catalog.createNamespace(Namespace.of("root", "db"));
        catalog.createTable(TableIdentifier.of(Namespace.of("root", "db"), "t"),
                new Schema(Types.NestedField.optional(1, "original", Types.LongType.get())));
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalog.close();
    }

    private CatalogBackedIcebergCatalogOps ops(boolean nested) {
        return new CatalogBackedIcebergCatalogOps(catalog, true, nested, false, Optional.of("root"));
    }

    @Test
    public void listedDatabaseResolvesToItsOriginalNamespace() {
        CatalogBackedIcebergCatalogOps ops = ops(false);
        List<String> listed = ops.listDatabaseNames();
        Assertions.assertEquals(Collections.singletonList("db"), listed);
        Assertions.assertTrue(ops.databaseExists(listed.get(0)),
                "A discovered namespace must round-trip through the existence check");
    }

    @Test
    public void listedDatabaseDoesNotReadTheReversedNamespace() {
        catalog.createNamespace(Namespace.of("db"));
        catalog.createNamespace(Namespace.of("db", "root"));
        catalog.createTable(TableIdentifier.of(Namespace.of("db", "root"), "t"),
                new Schema(Types.NestedField.optional(1, "other", Types.LongType.get())));
        CatalogBackedIcebergCatalogOps ops = ops(false);
        String db = ops.listDatabaseNames().get(0);
        Assertions.assertNotNull(ops.loadTable(db, "t").schema().findField("original"),
                "Discovery and table load must address the same remote namespace");
    }

    @Test
    public void nestedListedDatabaseRoundTripsWithoutDuplicatingTheRoot() {
        CatalogBackedIcebergCatalogOps ops = ops(true);
        List<String> listed = ops.listDatabaseNames();
        Assertions.assertEquals(1, listed.size());
        Assertions.assertTrue(ops.databaseExists(listed.get(0)),
                "A nested discovered namespace must not append the root a second time");
    }

    @Test
    public void unrootedNestedDatabaseResolvesToItsOriginalTable() {
        CatalogBackedIcebergCatalogOps ops = new CatalogBackedIcebergCatalogOps(
                catalog, true, true, false, Optional.empty());
        Assertions.assertTrue(ops.listDatabaseNames().contains("root.db"));
        Assertions.assertTrue(ops.databaseExists("root.db"));
        Assertions.assertNotNull(ops.loadTable("root.db", "t").schema().findField("original"));
    }

    @Test
    public void unrootedSingleLevelDatabaseResolvesToItsOriginalTable() {
        catalog.createNamespace(Namespace.of("plain"));
        catalog.createTable(TableIdentifier.of(Namespace.of("plain"), "t"),
                new Schema(Types.NestedField.optional(1, "original", Types.LongType.get())));
        CatalogBackedIcebergCatalogOps ops = new CatalogBackedIcebergCatalogOps(
                catalog, true, false, false, Optional.empty());
        Assertions.assertTrue(ops.listDatabaseNames().contains("plain"));
        Assertions.assertTrue(ops.databaseExists("plain"));
        Assertions.assertNotNull(ops.loadTable("plain", "t").schema().findField("original"));
    }

    @Test
    public void nestedDatabaseAndDescendantsResolveToTheirOriginalTables() {
        catalog.createNamespace(Namespace.of("root", "db", "deep"));
        catalog.createTable(TableIdentifier.of(Namespace.of("root", "db", "deep"), "t"),
                new Schema(Types.NestedField.optional(1, "deep_field", Types.LongType.get())));
        CatalogBackedIcebergCatalogOps ops = ops(true);
        Assertions.assertEquals(Arrays.asList("root.db", "root.db.deep"), ops.listDatabaseNames());
        Assertions.assertTrue(ops.databaseExists("root.db.deep"));
        Assertions.assertNotNull(ops.loadTable("root.db.deep", "t").schema().findField("deep_field"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void rootIsOneLevelEvenWhenItContainsDots(boolean nested) {
        catalog.createNamespace(Namespace.of("root.with.dot"));
        catalog.createNamespace(Namespace.of("root.with.dot", "child"));
        catalog.createTable(TableIdentifier.of(Namespace.of("root.with.dot", "child"), "t"),
                new Schema(Types.NestedField.optional(1, "original", Types.LongType.get())));
        CatalogBackedIcebergCatalogOps ops = new CatalogBackedIcebergCatalogOps(
                catalog, true, nested, false, Optional.of("root.with.dot"));
        String db = nested ? "root.with.dot.child" : "child";
        Assertions.assertEquals(Collections.singletonList(db), ops.listDatabaseNames());
        Assertions.assertTrue(ops.databaseExists(db));
        Assertions.assertNotNull(ops.loadTable(db, "t").schema().findField("original"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void childNamedAfterRootResolvesWithoutCollapsingLevels(boolean nested) {
        catalog.createNamespace(Namespace.of("root", "root"));
        catalog.createTable(TableIdentifier.of(Namespace.of("root", "root"), "t"),
                new Schema(Types.NestedField.optional(1, "child_field", Types.LongType.get())));
        CatalogBackedIcebergCatalogOps ops = ops(nested);
        String db = nested ? "root.root" : "root";
        Assertions.assertTrue(ops.listDatabaseNames().contains(db));
        Assertions.assertNotNull(ops.loadTable(db, "t").schema().findField("child_field"));
    }

    @Test
    public void nonNestedDottedChildIsOneLevel() {
        catalog.createNamespace(Namespace.of("root", "a.b"));
        catalog.createTable(TableIdentifier.of(Namespace.of("root", "a.b"), "t"),
                new Schema(Types.NestedField.optional(1, "literal_field", Types.LongType.get())));
        catalog.createNamespace(Namespace.of("root", "a"));
        catalog.createNamespace(Namespace.of("root", "a", "b"));
        catalog.createTable(TableIdentifier.of(Namespace.of("root", "a", "b"), "t"),
                new Schema(Types.NestedField.optional(1, "nested_field", Types.LongType.get())));
        CatalogBackedIcebergCatalogOps ops = ops(false);
        Assertions.assertTrue(ops.listDatabaseNames().contains("a.b"));
        Assertions.assertTrue(ops.databaseExists("a.b"));
        Assertions.assertNotNull(ops.loadTable("a.b", "t").schema().findField("literal_field"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void rootedChildWhitespaceIsPreserved(boolean nested) {
        String child = "\u3000child\u3000";
        catalog.createNamespace(Namespace.of("root", child));
        catalog.createTable(TableIdentifier.of(Namespace.of("root", child), "t"),
                new Schema(Types.NestedField.optional(1, "original", Types.LongType.get())));
        CatalogBackedIcebergCatalogOps ops = ops(nested);
        String db = nested ? "root." + child : child;
        Assertions.assertTrue(ops.listDatabaseNames().contains(db));
        Assertions.assertTrue(ops.databaseExists(db));
        Assertions.assertNotNull(ops.loadTable(db, "t").schema().findField("original"));
    }

    @Test
    public void nestedNamesOutsideConfiguredRootAreRejected() {
        CatalogBackedIcebergCatalogOps ops = ops(true);
        for (String db : Arrays.asList("db", "other.db", "rooted.db")) {
            DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                    () -> ops.databaseExists(db));
            Assertions.assertTrue(failure.getMessage().contains("root"));
        }
    }

    @Test
    public void nestedDottedChildIsRejectedInsteadOfAliasingAnotherNamespace() {
        FakeIcebergCatalog names = new FakeIcebergCatalog();
        names.childNamespaces.put(Namespace.of("root"),
                Arrays.asList(Namespace.of("root", "a.b"), Namespace.of("root", "a")));
        names.childNamespaces.put(Namespace.of("root", "a"),
                Collections.singletonList(Namespace.of("root", "a", "b")));
        CatalogBackedIcebergCatalogOps ops = new CatalogBackedIcebergCatalogOps(
                names, true, true, false, Optional.of("root"));
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                ops::listDatabaseNames);
        Assertions.assertTrue(failure.getMessage().contains("a.b"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void databaseAndTableDdlUseTheDiscoveredNamespace(boolean nested) {
        CatalogBackedIcebergCatalogOps ops = ops(nested);
        String db = nested ? "root.created" : "created";
        Namespace ns = Namespace.of("root", "created");
        ops.createDatabase(db, Collections.singletonMap("location", "memory-location"));
        Assertions.assertTrue(catalog.namespaceExists(ns));
        Assertions.assertTrue(ops.listDatabaseNames().contains(db));
        Assertions.assertEquals(Optional.of("memory-location"), ops.loadNamespaceLocation(db));
        ops.createTable(db, "created_table",
                new Schema(Types.NestedField.optional(1, "created_field", Types.LongType.get())),
                PartitionSpec.unpartitioned(), SortOrder.unsorted(), Collections.emptyMap());
        Assertions.assertTrue(catalog.tableExists(TableIdentifier.of(ns, "created_table")));
        Assertions.assertEquals(Collections.singletonList("created_table"), ops.listTableNames(db));
        Table loaded = ops.loadTable(db, "created_table");
        Assertions.assertNotNull(loaded.schema().findField("created_field"));
        ops.renameTable(db, "created_table", "renamed_table");
        Assertions.assertTrue(catalog.tableExists(TableIdentifier.of(ns, "renamed_table")));
        Assertions.assertFalse(catalog.tableExists(TableIdentifier.of(ns, "created_table")));
        ops.dropTable(db, "renamed_table", false);
        Assertions.assertFalse(catalog.tableExists(TableIdentifier.of(ns, "renamed_table")));
        ops.dropDatabase(db);
        Assertions.assertFalse(catalog.namespaceExists(ns));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void missingOriginalTableDoesNotLoadTheReversedTable(boolean nested) {
        catalog.createNamespace(Namespace.of("db"));
        catalog.createNamespace(Namespace.of("db", "root"));
        catalog.createTable(TableIdentifier.of(Namespace.of("db", "root"), "t"),
                new Schema(Types.NestedField.optional(1, "other", Types.LongType.get())));
        catalog.dropTable(TableIdentifier.of(Namespace.of("root", "db"), "t"), false);
        Assertions.assertThrows(NoSuchTableException.class,
                () -> ops(nested).loadTable(nested ? "root.db" : "db", "t"));
        Assertions.assertTrue(catalog.tableExists(TableIdentifier.of(Namespace.of("db", "root"), "t")));
    }
}
