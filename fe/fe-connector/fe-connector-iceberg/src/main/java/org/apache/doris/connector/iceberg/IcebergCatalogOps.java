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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

        public CatalogBackedIcebergCatalogOps(Catalog catalog) {
            this.catalog = catalog;
        }

        @Override
        public List<String> listDatabaseNames() {
            if (!(catalog instanceof SupportsNamespaces)) {
                LOG.warn("Iceberg catalog does not support namespaces");
                return Collections.emptyList();
            }
            SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            return nsCatalog.listNamespaces(Namespace.empty()).stream()
                    .map(ns -> ns.level(ns.length() - 1))
                    .collect(Collectors.toList());
        }

        @Override
        public boolean databaseExists(String dbName) {
            if (!(catalog instanceof SupportsNamespaces)) {
                return false;
            }
            return ((SupportsNamespaces) catalog).namespaceExists(Namespace.of(dbName));
        }

        @Override
        public List<String> listTableNames(String dbName) {
            Namespace ns = Namespace.of(dbName);
            return catalog.listTables(ns).stream()
                    .map(TableIdentifier::name)
                    .collect(Collectors.toList());
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return catalog.tableExists(TableIdentifier.of(dbName, tableName));
        }

        @Override
        public Table loadTable(String dbName, String tableName) {
            return catalog.loadTable(TableIdentifier.of(dbName, tableName));
        }

        @Override
        public void close() throws IOException {
            if (catalog instanceof java.io.Closeable) {
                ((java.io.Closeable) catalog).close();
            }
        }
    }
}
