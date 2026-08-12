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

package org.apache.doris.authorization;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * The object an authorization decision is about.
 *
 * <p>Every variant is nested here and can only be built through the factory methods, so the set of things
 * that can be asked about is closed: an implementation may switch on {@link #getKind()} and treat an
 * unknown kind as a defect rather than as something to guess about. Names are carried as given - no
 * lower-casing, no qualification - because whoever asks already resolved them and a second normalization
 * here would decide access on a name nobody used.</p>
 */
public abstract class AuthorizedResource {

    private final ResourceKind kind;

    private AuthorizedResource(ResourceKind kind) {
        this.kind = kind;
    }

    public final ResourceKind getKind() {
        return kind;
    }

    /** The cluster as a whole. */
    public static Global global() {
        return Global.INSTANCE;
    }

    public static Catalog catalog(String catalog) {
        return new Catalog(catalog);
    }

    public static Database database(String catalog, String database) {
        return new Database(catalog, database);
    }

    public static Table table(String catalog, String database, String table) {
        return new Table(catalog, database, table);
    }

    public static Columns columns(String catalog, String database, String table, Set<String> columns) {
        return new Columns(catalog, database, table, columns);
    }

    public static Named resource(String name) {
        return new Named(ResourceKind.RESOURCE, name);
    }

    public static Named workloadGroup(String name) {
        return new Named(ResourceKind.WORKLOAD_GROUP, name);
    }

    public static Named storageVault(String name) {
        return new Named(ResourceKind.STORAGE_VAULT, name);
    }

    /**
     * A cloud object, reached through the cloud privilege path.
     *
     * @param kind one of the {@code CLOUD_*} kinds
     */
    public static Named cloud(ResourceKind kind, String name) {
        Objects.requireNonNull(kind, "kind is required");
        switch (kind) {
            case CLOUD_GENERAL:
            case CLOUD_COMPUTE_GROUP:
            case CLOUD_STAGE:
            case CLOUD_STORAGE_VAULT:
                return new Named(kind, name);
            default:
                throw new IllegalArgumentException(kind + " is not a cloud resource kind");
        }
    }

    /** The cluster as a whole; global privileges are the ones held on it. */
    public static final class Global extends AuthorizedResource {
        private static final Global INSTANCE = new Global();

        private Global() {
            super(ResourceKind.GLOBAL);
        }

        @Override
        public String toString() {
            return "global";
        }
    }

    /** One catalog. */
    public static final class Catalog extends AuthorizedResource {
        private final String catalog;

        private Catalog(String catalog) {
            super(ResourceKind.CATALOG);
            this.catalog = Objects.requireNonNull(catalog, "catalog is required");
        }

        public String getCatalog() {
            return catalog;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Catalog)) {
                return false;
            }
            return catalog.equals(((Catalog) o).catalog);
        }

        @Override
        public int hashCode() {
            return catalog.hashCode();
        }

        @Override
        public String toString() {
            return "catalog " + catalog;
        }
    }

    /** One database of one catalog. */
    public static final class Database extends AuthorizedResource {
        private final String catalog;
        private final String database;

        private Database(String catalog, String database) {
            super(ResourceKind.DATABASE);
            this.catalog = Objects.requireNonNull(catalog, "catalog is required");
            this.database = Objects.requireNonNull(database, "database is required");
        }

        public String getCatalog() {
            return catalog;
        }

        public String getDatabase() {
            return database;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Database)) {
                return false;
            }
            Database that = (Database) o;
            return catalog.equals(that.catalog) && database.equals(that.database);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalog, database);
        }

        @Override
        public String toString() {
            return "database " + catalog + "." + database;
        }
    }

    /** One table, view or any other relation. */
    public static final class Table extends AuthorizedResource {
        private final String catalog;
        private final String database;
        private final String table;

        private Table(String catalog, String database, String table) {
            super(ResourceKind.TABLE);
            this.catalog = Objects.requireNonNull(catalog, "catalog is required");
            this.database = Objects.requireNonNull(database, "database is required");
            this.table = Objects.requireNonNull(table, "table is required");
        }

        public String getCatalog() {
            return catalog;
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Table)) {
                return false;
            }
            Table that = (Table) o;
            return catalog.equals(that.catalog) && database.equals(that.database) && table.equals(that.table);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalog, database, table);
        }

        @Override
        public String toString() {
            return "table " + catalog + "." + database + "." + table;
        }
    }

    /** Named columns of one table, asked about together. */
    public static final class Columns extends AuthorizedResource {
        private final String catalog;
        private final String database;
        private final String table;
        private final Set<String> columns;

        private Columns(String catalog, String database, String table, Set<String> columns) {
            super(ResourceKind.COLUMNS);
            this.catalog = Objects.requireNonNull(catalog, "catalog is required");
            this.database = Objects.requireNonNull(database, "database is required");
            this.table = Objects.requireNonNull(table, "table is required");
            Objects.requireNonNull(columns, "columns is required");
            // Insertion order is kept: a denial names the first column that failed, and a set that reorders
            // would make which column is reported depend on hashing.
            this.columns = Collections.unmodifiableSet(new LinkedHashSet<>(columns));
        }

        public String getCatalog() {
            return catalog;
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        public Set<String> getColumns() {
            return columns;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Columns)) {
                return false;
            }
            Columns that = (Columns) o;
            return catalog.equals(that.catalog) && database.equals(that.database)
                    && table.equals(that.table) && columns.equals(that.columns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalog, database, table, columns);
        }

        @Override
        public String toString() {
            return "columns " + columns + " of " + catalog + "." + database + "." + table;
        }
    }

    /** A system-wide object addressed by a single name: a resource, a workload group, a vault, a stage. */
    public static final class Named extends AuthorizedResource {
        private final String name;

        private Named(ResourceKind kind, String name) {
            super(kind);
            this.name = Objects.requireNonNull(name, "name is required");
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Named)) {
                return false;
            }
            Named that = (Named) o;
            return getKind() == that.getKind() && name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getKind(), name);
        }

        @Override
        public String toString() {
            return getKind() + " " + name;
        }
    }
}
