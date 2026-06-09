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
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.MemoryFileSystem;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergMetadataOpTest {

    @Test
    public void testGetNamespaces() {
        Namespace ns = IcebergMetadataOps.getNamespace(Optional.empty(), "db1");
        Assert.assertEquals(1, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.empty(), "db1.db2.db3");
        Assert.assertEquals(3, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.empty(), "db1..db2");
        Assert.assertEquals(2, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.of("p1"), "db1");
        Assert.assertEquals(2, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.of("p1"), "");
        Assert.assertEquals(1, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.empty(), "");
        Assert.assertEquals(0, ns.length());
    }

    @Test
    public void testListTableNamesSkipsViewsWhenRestViewDisabled() {
        IcebergRestExternalCatalog dorisCatalog = Mockito.mock(IcebergRestExternalCatalog.class);
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class, ViewCatalog.class));

        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "http://localhost:8181");
        props.put("iceberg.rest.view-enabled", "false");

        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(dorisCatalog.getCatalogProperty()).thenReturn(new CatalogProperty(null, props));

        Namespace namespace = Namespace.of("PUBLIC");
        TableIdentifier table = TableIdentifier.of(namespace, "DORIS_HORIZON_T");
        Mockito.when(icebergCatalog.listTables(namespace)).thenReturn(Collections.singletonList(table));

        IcebergMetadataOps ops = new IcebergMetadataOps(dorisCatalog, icebergCatalog);
        List<String> tableNames = ops.listTableNames("PUBLIC");

        Assert.assertEquals(Collections.singletonList("DORIS_HORIZON_T"), tableNames);
        Mockito.verify((ViewCatalog) icebergCatalog, Mockito.never()).listViews(Mockito.any());
    }

    @Test
    public void testListTableNamesFiltersViewsWhenRestViewEnabled() {
        IcebergRestExternalCatalog dorisCatalog = Mockito.mock(IcebergRestExternalCatalog.class);
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class, ViewCatalog.class));

        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "http://localhost:8181");

        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(dorisCatalog.getCatalogProperty()).thenReturn(new CatalogProperty(null, props));

        Namespace namespace = Namespace.of("PUBLIC");
        TableIdentifier table = TableIdentifier.of(namespace, "DORIS_HORIZON_T");
        TableIdentifier view = TableIdentifier.of(namespace, "DORIS_HORIZON_V");
        Mockito.when(icebergCatalog.listTables(namespace)).thenReturn(Arrays.asList(table, view));
        Mockito.when(((ViewCatalog) icebergCatalog).listViews(namespace)).thenReturn(Collections.singletonList(view));

        IcebergMetadataOps ops = new IcebergMetadataOps(dorisCatalog, icebergCatalog);
        List<String> tableNames = ops.listTableNames("PUBLIC");

        Assert.assertEquals(Collections.singletonList("DORIS_HORIZON_T"), tableNames);
    }

    @Test
    public void testDropTableCleansEmptyTableLocation() throws Exception {
        MemoryFileSystem fs = new MemoryFileSystem();
        Location tableLocation = Location.of("hdfs://nn/warehouse/db/t1");
        fs.mkdirs(tableLocation);
        fs.mkdirs(tableLocation.resolve("data"));
        fs.mkdirs(tableLocation.resolve("metadata"));

        IcebergExternalCatalog dorisCatalog = mockHmsCatalog();
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class));
        IcebergMetadataOps ops = newOpsWithCleanupFileSystem(dorisCatalog, icebergCatalog, fs);

        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("db"), "t1");
        org.apache.iceberg.Table icebergTable = Mockito.mock(org.apache.iceberg.Table.class);
        Mockito.when(icebergTable.location()).thenReturn(tableLocation.uri());
        Mockito.when(icebergCatalog.tableExists(tableIdentifier)).thenReturn(true);
        Mockito.when(icebergCatalog.loadTable(tableIdentifier)).thenReturn(icebergTable);
        Mockito.when(icebergCatalog.dropTable(tableIdentifier, true)).thenReturn(true);

        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn("db");
        Mockito.when(dorisTable.getRemoteName()).thenReturn("t1");
        Mockito.when(dorisTable.getName()).thenReturn("t1");
        ops.dropTableImpl(dorisTable, false);

        Assert.assertFalse(fs.exists(tableLocation));
        Mockito.verify(icebergCatalog).dropTable(tableIdentifier, true);
    }

    @Test
    public void testDropDbCleansEmptyNamespaceLocation() throws Exception {
        MemoryFileSystem fs = new MemoryFileSystem();
        Location namespaceLocation = Location.of("hdfs://nn/warehouse/db.db");
        fs.mkdirs(namespaceLocation);

        IcebergExternalCatalog dorisCatalog = mockHmsCatalog();
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class));
        IcebergMetadataOps ops = newOpsWithCleanupFileSystem(dorisCatalog, icebergCatalog, fs);

        ExternalDatabase<?> dorisDb = Mockito.mock(ExternalDatabase.class);
        Mockito.when(dorisDb.getRemoteName()).thenReturn("db");
        Mockito.when(dorisCatalog.getDbNullable("db")).thenReturn(dorisDb);

        SupportsNamespaces nsCatalog = (SupportsNamespaces) icebergCatalog;
        Namespace namespace = Namespace.of("db");
        Mockito.when(nsCatalog.loadNamespaceMetadata(namespace))
                .thenReturn(Collections.singletonMap("location", namespaceLocation.uri()));
        Mockito.when(nsCatalog.dropNamespace(namespace)).thenReturn(true);
        ops.dropDbImpl("db", false, false);

        Assert.assertFalse(fs.exists(namespaceLocation));
        Mockito.verify(nsCatalog).dropNamespace(namespace);
    }

    @Test
    public void testDeleteEmptyDirectoryKeepsDirectoryWithExternalFile() throws Exception {
        MemoryFileSystem fs = new MemoryFileSystem();
        Location tableLocation = Location.of("hdfs://nn/warehouse/db/t2");
        fs.mkdirs(tableLocation);
        fs.mkdirs(tableLocation.resolve("data"));
        Location externalFile = tableLocation.resolve("external-file");
        fs.put(externalFile, new byte[] {1});

        Assert.assertFalse(IcebergMetadataOps.deleteEmptyDirectory(fs, tableLocation));
        Assert.assertTrue(fs.exists(tableLocation));
        Assert.assertTrue(fs.exists(externalFile));
        Assert.assertFalse(fs.exists(tableLocation.resolve("data")));
    }

    private IcebergExternalCatalog mockHmsCatalog() {
        IcebergExternalCatalog dorisCatalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(dorisCatalog.getIcebergCatalogType()).thenReturn(IcebergExternalCatalog.ICEBERG_HMS);
        Mockito.when(dorisCatalog.getCatalogProperty()).thenReturn(new CatalogProperty(null, Collections.emptyMap()));
        return dorisCatalog;
    }

    private IcebergMetadataOps newOpsWithCleanupFileSystem(
            IcebergExternalCatalog dorisCatalog, Catalog icebergCatalog, FileSystem fs) {
        IcebergMetadataOps ops = new IcebergMetadataOps(dorisCatalog, icebergCatalog) {
            @Override
            protected FileSystem createCleanupFileSystem() {
                return fs;
            }
        };
        Mockito.when(dorisCatalog.getMetadataOps()).thenReturn(ops);
        return ops;
    }
}
