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
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class IcebergJdbcViewProbeTest {
    private static final Namespace NAMESPACE = Namespace.of("db");
    private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "table");
    private static final TableIdentifier VIEW = TableIdentifier.of(NAMESPACE, "view");

    private IcebergCatalogOps ops(Catalog catalog) {
        return new CatalogBackedIcebergCatalogOps(catalog, false, false, true, Optional.empty());
    }

    @Test
    public void v0ListsTablesWithoutViewSupport() {
        Assertions.assertEquals(Collections.singletonList("table"), ops(new V0JdbcCatalog()).listTableNames("db"));
    }

    @Test
    public void v0HasNoViews() {
        IcebergCatalogOps ops = ops(new V0JdbcCatalog());
        Assertions.assertEquals(Collections.emptyList(), ops.listViewNames("db"));
        Assertions.assertFalse(ops.viewExists("db", "table"));
        Assertions.assertFalse(ops.viewExists("db", "missing"));
    }

    @Test
    public void explicitV0ViewOperationsStillFail() {
        IcebergCatalogOps ops = ops(new V0JdbcCatalog());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ops.loadView("db", "view"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ops.dropView("db", "view"));
    }

    @Test
    public void jdbcViewSupportIsNotDisabledByCatalogType() {
        IcebergCatalogOps ops = ops(new V0JdbcCatalog() {
            @Override
            public List<TableIdentifier> listTables(Namespace namespace) {
                return Arrays.asList(TABLE, VIEW);
            }

            @Override
            public List<TableIdentifier> listViews(Namespace namespace) {
                return Collections.singletonList(VIEW);
            }

            @Override
            public boolean viewExists(TableIdentifier identifier) {
                return VIEW.equals(identifier);
            }
        });
        Assertions.assertEquals(Collections.singletonList("table"), ops.listTableNames("db"));
        Assertions.assertEquals(Collections.singletonList("view"), ops.listViewNames("db"));
        Assertions.assertTrue(ops.viewExists("db", "view"));
        Assertions.assertFalse(ops.viewExists("db", "table"));
    }

    @Test
    public void jdbcProbeFailuresAreNotHidden() {
        for (RuntimeException failure : Arrays.asList(
                new ForbiddenException("Access denied"), new UncheckedSQLException("Connection failed"))) {
            IcebergCatalogOps ops = ops(new V0JdbcCatalog() {
                @Override
                public List<TableIdentifier> listViews(Namespace namespace) {
                    throw failure;
                }

                @Override
                public boolean viewExists(TableIdentifier identifier) {
                    throw failure;
                }
            });
            Assertions.assertSame(failure, Assertions.assertThrows(failure.getClass(),
                    () -> ops.listTableNames("db")));
            Assertions.assertSame(failure, Assertions.assertThrows(failure.getClass(),
                    () -> ops.listViewNames("db")));
            Assertions.assertSame(failure, Assertions.assertThrows(failure.getClass(),
                    () -> ops.viewExists("db", "view")));
        }
    }

    @Test
    public void unsupportedViewsInOtherCatalogsAreNotHidden() {
        UnsupportedOperationException failure = new UnsupportedOperationException("Unsupported view operation");
        FakeIcebergViewCatalog catalog = new FakeIcebergViewCatalog() {
            @Override
            public List<TableIdentifier> listViews(Namespace namespace) {
                throw failure;
            }

            @Override
            public boolean viewExists(TableIdentifier identifier) {
                throw failure;
            }
        };
        IcebergCatalogOps ops = ops(catalog);
        Assertions.assertSame(failure, Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.listTableNames("db")));
        Assertions.assertSame(failure, Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.listViewNames("db")));
        Assertions.assertSame(failure, Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.viewExists("db", "view")));
    }

    // Keep the SDK's actual V0 listViews/viewExists guards; only table listing avoids a live JDBC connection.
    private static class V0JdbcCatalog extends JdbcCatalog {
        @Override
        public List<TableIdentifier> listTables(Namespace namespace) {
            return Collections.singletonList(TABLE);
        }
    }
}
