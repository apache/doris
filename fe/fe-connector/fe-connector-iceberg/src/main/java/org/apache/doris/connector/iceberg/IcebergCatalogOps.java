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

import com.google.common.base.Splitter;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Injection seam over the remote Iceberg {@link Catalog} calls.
 *
 * <p>The default {@link CatalogBackedIcebergCatalogOps} simply delegates to a real {@code Catalog},
 * which requires a live remote catalog (REST / HMS / Glue / Hadoop / JDBC / S3Tables / DLF). By
 * depending on this interface instead of {@code Catalog} directly, {@link IcebergConnectorMetadata}
 * becomes unit-testable offline with a hand-written recording fake (no Mockito) — mirroring the paimon
 * connector's {@link org.apache.doris.connector.iceberg.IcebergCatalogFactory} sibling seam pattern
 * {@code PaimonCatalogOps}.
 *
 * <p>P6.1 (this task) declares only the read subset the metadata layer needs. Write / DDL / MVCC
 * methods land in later phases, with signatures mirroring the real Iceberg {@code Catalog}. The
 * {@code SupportsNamespaces}-vs-plain-{@code Catalog} branch (which the skeleton leaked into the
 * metadata layer) is kept INTERNAL to the default impl.
 */
public interface IcebergCatalogOps {

    /**
     * Lists the top-level database (namespace) names, or an empty list when the catalog does not
     * support namespaces. Returns the LAST level of each namespace (mirrors the legacy skeleton).
     */
    List<String> listDatabaseNames();

    /** Returns {@code true} iff the database (namespace) exists; {@code false} when no namespace support. */
    boolean databaseExists(String dbName);

    /** Lists the table names in {@code dbName}. */
    List<String> listTableNames(String dbName);

    /** Returns {@code true} iff {@code dbName.tableName} exists. */
    boolean tableExists(String dbName, String tableName);

    /** Loads the Iceberg {@link Table} for {@code dbName.tableName}. */
    Table loadTable(String dbName, String tableName);

    void close() throws IOException;

    /**
     * Default implementation backing the seam with a real Iceberg {@link Catalog}. Each method is a
     * thin delegation; the {@code Catalog} is the only state. Keeps the {@code SupportsNamespaces}
     * branch internal.
     */
    class CatalogBackedIcebergCatalogOps implements IcebergCatalogOps {

        private static final Logger LOG = LogManager.getLogger(CatalogBackedIcebergCatalogOps.class);

        private final Catalog catalog;
        // Listing-parity gating mirrored from legacy IcebergMetadataOps (threaded from IcebergConnector):
        private final boolean restFlavor;
        private final boolean nestedNamespaceEnabled;
        private final boolean viewEnabled;
        private final Optional<String> externalCatalogName;

        public CatalogBackedIcebergCatalogOps(Catalog catalog) {
            this(catalog, false, false, true, Optional.empty());
        }

        public CatalogBackedIcebergCatalogOps(Catalog catalog, boolean restFlavor,
                boolean nestedNamespaceEnabled, boolean viewEnabled, Optional<String> externalCatalogName) {
            this.catalog = catalog;
            this.restFlavor = restFlavor;
            this.nestedNamespaceEnabled = nestedNamespaceEnabled;
            this.viewEnabled = viewEnabled;
            this.externalCatalogName = externalCatalogName;
        }

        @Override
        public List<String> listDatabaseNames() {
            if (!(catalog instanceof SupportsNamespaces)) {
                LOG.warn("Iceberg catalog does not support namespaces");
                return Collections.emptyList();
            }
            return listNestedNamespaces(rootNamespace());
        }

        /**
         * Lists databases under {@code parentNs}, mirroring legacy {@code IcebergMetadataOps}: for a REST
         * flavor with {@code iceberg.rest.nested-namespace-enabled=true} it RECURSES, emitting each child's
         * dotted {@code toString()} followed by its descendants; otherwise it returns each child's last
         * level only. Assumes the catalog is a {@link SupportsNamespaces} (guarded by the callers).
         */
        private List<String> listNestedNamespaces(Namespace parentNs) {
            SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            if (restFlavor && nestedNamespaceEnabled) {
                return nsCatalog.listNamespaces(parentNs).stream()
                        .flatMap(childNs -> Stream.concat(
                                Stream.of(childNs.toString()),
                                listNestedNamespaces(childNs).stream()))
                        .collect(Collectors.toList());
            }
            return nsCatalog.listNamespaces(parentNs).stream()
                    .map(ns -> ns.level(ns.length() - 1))
                    .collect(Collectors.toList());
        }

        @Override
        public boolean databaseExists(String dbName) {
            if (!(catalog instanceof SupportsNamespaces)) {
                return false;
            }
            return ((SupportsNamespaces) catalog).namespaceExists(toNamespace(dbName));
        }

        @Override
        public List<String> listTableNames(String dbName) {
            Namespace ns = toNamespace(dbName);
            List<String> tableNames = catalog.listTables(ns).stream()
                    .map(TableIdentifier::name)
                    .collect(Collectors.toList());
            // iceberg's listTables also returns views, so subtract the view names when the catalog is a
            // (view-enabled) ViewCatalog — mirrors legacy IcebergMetadataOps.listTableNames.
            if (!isViewCatalogEnabled()) {
                return tableNames;
            }
            List<String> views = ((ViewCatalog) catalog).listViews(ns).stream()
                    .map(TableIdentifier::name)
                    .collect(Collectors.toList());
            if (views.isEmpty()) {
                return tableNames;
            }
            return tableNames.stream()
                    .filter(name -> !views.contains(name))
                    .collect(Collectors.toList());
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return catalog.tableExists(toTableIdentifier(dbName, tableName));
        }

        @Override
        public Table loadTable(String dbName, String tableName) {
            return catalog.loadTable(toTableIdentifier(dbName, tableName));
        }

        /** View filtering is on iff the catalog is a {@link ViewCatalog} and (for REST) views are enabled. */
        private boolean isViewCatalogEnabled() {
            if (!(catalog instanceof ViewCatalog)) {
                return false;
            }
            return !restFlavor || viewEnabled;
        }

        /** The root namespace to start database listing from: the external-catalog level, else empty. */
        private Namespace rootNamespace() {
            return externalCatalogName.map(Namespace::of).orElseGet(Namespace::empty);
        }

        /**
         * Builds the multi-level namespace for {@code dbName}, mirroring legacy {@code getNamespace}: split
         * on {@code '.'} (omit empties / trim), then append the external-catalog level last when present.
         */
        private Namespace toNamespace(String dbName) {
            // Use the SAME Guava splitter as legacy IcebergMetadataOps.getNamespace so splitting is
            // byte-faithful — including trimResults()==CharMatcher.whitespace(), which trims Unicode
            // whitespace above U+0020 (e.g. U+3000) that String.trim() would leave behind.
            List<String> levels = new ArrayList<>(
                    Splitter.on('.').omitEmptyStrings().trimResults().splitToList(dbName));
            externalCatalogName.ifPresent(levels::add);
            return Namespace.of(levels.toArray(new String[0]));
        }

        private TableIdentifier toTableIdentifier(String dbName, String tableName) {
            return TableIdentifier.of(toNamespace(dbName), tableName);
        }

        @Override
        public void close() throws IOException {
            if (catalog instanceof java.io.Closeable) {
                ((java.io.Closeable) catalog).close();
            }
        }
    }
}
