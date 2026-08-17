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
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalDatabase;
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

    private IcebergExternalCatalog mockHmsCatalog(Map<String, String> catalogProperties) {
        IcebergExternalCatalog dorisCatalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        Mockito.when(dorisCatalog.getProperties()).thenReturn(catalogProperties);
        Mockito.when(dorisCatalog.getIcebergCatalogType()).thenReturn(IcebergExternalCatalog.ICEBERG_HMS);
        Mockito.when(dorisCatalog.getCatalogProperty()).thenReturn(new CatalogProperty(null, Collections.emptyMap()));
        return dorisCatalog;
    }
}
