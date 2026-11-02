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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalCatalog;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IcebergJdbcViewProbeTest {
    private static final Namespace NAMESPACE = Namespace.of("db");
    private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "table");
    private static final TableIdentifier VIEW = TableIdentifier.of(NAMESPACE, "view");

    private IcebergMetadataOps ops(Catalog catalog) {
        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        return new IcebergMetadataOps(dorisCatalog, catalog);
    }

    @Test
    public void v0ListsTablesWithoutViewSupport() {
        Assertions.assertEquals(Collections.singletonList("table"), ops(new V0JdbcCatalog()).listTableNames("db"));
    }

    @Test
    public void v0HasNoViews() {
        IcebergMetadataOps ops = ops(new V0JdbcCatalog());
        Assertions.assertEquals(Collections.emptyList(), ops.listViewNames("db"));
        Assertions.assertFalse(ops.viewExists("db", "table"));
        Assertions.assertFalse(ops.viewExists("db", "missing"));
    }

    @Test
    public void explicitV0ViewLoadStillFails() {
        IcebergMetadataOps ops = ops(new V0JdbcCatalog());
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class, () -> ops.loadView("db", "view"));
        Assertions.assertInstanceOf(UnsupportedOperationException.class, failure.getCause());
    }

    @Test
    public void jdbcViewSupportIsNotDisabledByCatalogType() {
        IcebergMetadataOps ops = ops(new V0JdbcCatalog() {
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
            IcebergMetadataOps ops = ops(new V0JdbcCatalog() {
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
            Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                    () -> ops.viewExists("db", "view")).getCause());
        }
    }

    @Test
    public void unsupportedViewsInOtherCatalogsAreNotHidden() {
        UnsupportedOperationException failure = new UnsupportedOperationException("Unsupported view operation");
        Catalog catalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class, ViewCatalog.class));
        Mockito.when(catalog.listTables(NAMESPACE)).thenReturn(Collections.singletonList(TABLE));
        Mockito.when(((ViewCatalog) catalog).listViews(NAMESPACE)).thenThrow(failure);
        Mockito.when(((ViewCatalog) catalog).viewExists(VIEW)).thenThrow(failure);
        IcebergMetadataOps ops = ops(catalog);
        Assertions.assertSame(failure, Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.listTableNames("db")));
        Assertions.assertSame(failure, Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.listViewNames("db")));
        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                () -> ops.viewExists("db", "view")).getCause());
    }

    // Keep the SDK's actual V0 listViews/viewExists guards; only table listing avoids a live JDBC connection.
    private static class V0JdbcCatalog extends JdbcCatalog {
        @Override
        public List<TableIdentifier> listTables(Namespace namespace) {
            return Collections.singletonList(TABLE);
        }
    }
}
