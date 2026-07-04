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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.MemoryFileSystem;
import org.apache.doris.kerberos.ExecutionAuthenticator;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

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
    public void testListTableNamesFiltersViewsWhenRestViewEnabled() {
        IcebergExternalCatalog dorisCatalog = Mockito.mock(IcebergExternalCatalog.class);
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
    public void testPerformCreateTableRespectsCatalogDefaultFormatVersion() throws Exception {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.FORMAT_VERSION, "3");
        IcebergExternalCatalog dorisCatalog = mockHmsCatalog(catalogProps);
        Catalog icebergCatalog = Mockito.mock(Catalog.class,
                Mockito.withSettings().extraInterfaces(SupportsNamespaces.class));
        IcebergMetadataOps ops = new IcebergMetadataOps(dorisCatalog, icebergCatalog);

        ExternalDatabase<?> dorisDb = Mockito.mock(ExternalDatabase.class);
        Mockito.when(dorisDb.getRemoteName()).thenReturn("db");
        Mockito.when(dorisDb.getTableNullable("tbl")).thenReturn(null);
        Mockito.doReturn(dorisDb).when(dorisCatalog).getDbNullable("db");
        Mockito.when(dorisCatalog.getName()).thenReturn("iceberg_catalog");
        Mockito.when(icebergCatalog.tableExists(TableIdentifier.of("db", "tbl"))).thenReturn(false);

        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        Map<String, String> tableProps = new HashMap<>();
        Mockito.when(createTableInfo.getDbName()).thenReturn("db");
        Mockito.when(createTableInfo.getTableName()).thenReturn("tbl");
        Mockito.when(createTableInfo.isIfNotExists()).thenReturn(false);
        Mockito.when(createTableInfo.getColumns()).thenReturn(Collections.singletonList(
                new Column("id", Type.INT, true)));
        Mockito.when(createTableInfo.getProperties()).thenReturn(tableProps);

        ops.performCreateTable(createTableInfo);

        Mockito.verify(createTableInfo).validateIcebergRowLineageColumns(3);
        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(icebergCatalog).createTable(Mockito.eq(TableIdentifier.of("db", "tbl")),
                Mockito.any(Schema.class), Mockito.any(PartitionSpec.class), propsCaptor.capture());
        Assert.assertFalse(propsCaptor.getValue().containsKey(TableProperties.FORMAT_VERSION));
        Assert.assertEquals(3, IcebergUtils.getEffectiveIcebergFormatVersion(
                propsCaptor.getValue(), catalogProps));
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
        Mockito.doReturn(dorisDb).when(dorisCatalog).getDbNullable("db");

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
        Assert.assertTrue(fs.exists(tableLocation.resolve("data")));
    }

    @Test
    public void testDeleteEmptyTableLocationCleansFlatObjectStoreMarkers() throws Exception {
        FlatMarkerFileSystem fs = new FlatMarkerFileSystem();
        Location tableLocation = Location.of("s3://bucket/warehouse/db/t3");
        fs.mkdirs(tableLocation);
        fs.mkdirs(tableLocation.resolve("data"));
        fs.mkdirs(tableLocation.resolve("metadata"));

        Assert.assertTrue(fs.exists(tableLocation));
        Assert.assertTrue(IcebergMetadataOps.deleteEmptyTableLocation(fs, tableLocation));
        Assert.assertFalse(fs.exists(tableLocation));
    }

    private IcebergExternalCatalog mockHmsCatalog() {
        return mockHmsCatalog(Collections.emptyMap());
    }

    private IcebergExternalCatalog mockHmsCatalog(Map<String, String> catalogProperties) {
        IcebergExternalCatalog dorisCatalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(catalogProperties);
        Mockito.when(dorisCatalog.getIcebergCatalogType()).thenReturn(IcebergCatalogConstants.ICEBERG_HMS);
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

    private static class FlatMarkerFileSystem implements FileSystem {
        private final Set<String> markers = new HashSet<>();
        private final Set<String> files = new HashSet<>();

        @Override
        public boolean exists(Location location) {
            String uri = location.uri();
            String marker = withTrailingSlash(uri);
            if (markers.contains(uri) || markers.contains(marker) || files.contains(uri)) {
                return true;
            }
            String prefix = withTrailingSlash(uri);
            for (String file : files) {
                if (file.startsWith(prefix)) {
                    return true;
                }
            }
            for (String directoryMarker : markers) {
                if (directoryMarker.startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void mkdirs(Location location) {
            markers.add(withTrailingSlash(location.uri()));
        }

        @Override
        public void delete(Location location, boolean recursive) throws IOException {
            if (recursive) {
                throw new IOException("recursive delete is not fail-safe");
            }
            String marker = withTrailingSlash(location.uri());
            for (String file : files) {
                if (file.startsWith(marker) && !file.equals(marker)) {
                    throw new IOException("Directory not empty: " + location.uri());
                }
            }
            for (String directoryMarker : markers) {
                if (directoryMarker.startsWith(marker) && !directoryMarker.equals(marker)) {
                    throw new IOException("Directory not empty: " + location.uri());
                }
            }
            markers.remove(marker);
            files.remove(location.uri());
        }

        @Override
        public void rename(Location src, Location dst) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIterator list(Location location) {
            String prefix = withTrailingSlash(location.uri());
            List<FileEntry> entries = new ArrayList<>();
            for (String file : files) {
                if (file.startsWith(prefix)) {
                    entries.add(new FileEntry(Location.of(file), 1L, false, 0L, null));
                }
            }
            return iteratorOf(entries);
        }

        @Override
        public DorisInputFile newInputFile(Location location) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DorisOutputFile newOutputFile(Location location) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

        private static FileIterator iteratorOf(List<FileEntry> entries) {
            return new FileIterator() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < entries.size();
                }

                @Override
                public FileEntry next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return entries.get(index++);
                }

                @Override
                public void close() {
                }
            };
        }

        private static String withTrailingSlash(String uri) {
            return uri.endsWith("/") ? uri : uri + "/";
        }
    }
}
