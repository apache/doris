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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Offline fail-loud double for an iceberg {@link Catalog} + {@link SupportsNamespaces}, mirroring
 * {@link FakeIcebergTable}. Only the read accessors the {@code CatalogBackedIcebergCatalogOps} listing
 * path exercises return controlled values; every other method throws {@link UnsupportedOperationException}
 * so a future change that starts depending on (say) {@code createNamespace} blows up loudly instead of
 * silently passing.
 *
 * <p>Records the namespaces / identifiers it receives so tests can assert the EXACT namespace the seam
 * constructed (dotted-name split + external-catalog-name append), not just the returned names.
 *
 * <p>See also {@link FakeIcebergViewCatalog} (adds {@code ViewCatalog}) and {@link PlainIcebergCatalog}
 * (a bare {@code Catalog} with no namespace support).
 */
class FakeIcebergCatalog implements Catalog, SupportsNamespaces {

    /** Ordered record of the calls made, e.g. {@code "listNamespaces:a"}, {@code "listViews:db1"}. */
    final List<String> log = new ArrayList<>();
    /** parent namespace -> its immediate child namespaces (for listNamespaces / nested recursion). */
    final Map<Namespace, List<Namespace>> childNamespaces = new HashMap<>();
    /** namespace -> table names returned by listTables. */
    final Map<Namespace, List<String>> tablesByNs = new HashMap<>();
    final Set<Namespace> existingNamespaces = new HashSet<>();
    final Set<TableIdentifier> existingTables = new HashSet<>();

    /** The exact namespace/identifier last received — lets tests pin namespace CONSTRUCTION. */
    Namespace lastListTablesNs;
    Namespace lastNamespaceExistsNs;
    TableIdentifier lastTableExistsId;

    @Override
    public List<Namespace> listNamespaces(Namespace ns) {
        log.add("listNamespaces:" + ns);
        return childNamespaces.getOrDefault(ns, Collections.emptyList());
    }

    @Override
    public boolean namespaceExists(Namespace ns) {
        lastNamespaceExistsNs = ns;
        log.add("namespaceExists:" + ns);
        return existingNamespaces.contains(ns);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace ns) {
        lastListTablesNs = ns;
        log.add("listTables:" + ns);
        return tablesByNs.getOrDefault(ns, Collections.<String>emptyList()).stream()
                .map(n -> TableIdentifier.of(ns, n))
                .collect(Collectors.toList());
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
        lastTableExistsId = identifier;
        log.add("tableExists:" + identifier);
        return existingTables.contains(identifier);
    }

    // ---- outside the listing read path: fail loud if ever called ----

    @Override
    public Table loadTable(TableIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropNamespace(Namespace namespace) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        throw new UnsupportedOperationException();
    }
}
