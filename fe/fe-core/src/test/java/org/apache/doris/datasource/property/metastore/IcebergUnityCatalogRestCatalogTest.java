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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergRestExternalCatalog;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@Disabled("set your databricks token and uri before running the test")
public class IcebergUnityCatalogRestCatalogTest {

    private String oauthToken = "";
    private String uri = "https://dbc-59918a85-6c3a.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest/";

    @Test
    public void testIcebergUnityCatalogRestCatalog() {
        try {
            Catalog bricksRestCatalog = initIcebergUnityCatalogRestCatalog();
            SupportsNamespaces nsCatalog = (SupportsNamespaces) bricksRestCatalog;
            // List namespaces and assert
            nsCatalog.listNamespaces(Namespace.empty()).forEach(namespace1 -> {
                System.out.println("Namespace: " + namespace1);
                Assertions.assertNotNull(namespace1, "Namespace should not be null");

                bricksRestCatalog.listTables(namespace1).forEach(tableIdentifier -> {
                    System.out.println("Table: " + tableIdentifier.name());
                    Assertions.assertNotNull(tableIdentifier, "TableIdentifier should not be null");

                    // Load table history and assert
                    try {
                        Table iceTable = bricksRestCatalog.loadTable(tableIdentifier);
                        Map<String, String> ioProperties = iceTable.io().properties();
                        for (Map.Entry<String, String> entry : ioProperties.entrySet()) {
                            System.out.println("io prop: " + entry.getKey() + ": " + entry.getValue());
                        }

                        iceTable.history().forEach(snapshot -> {
                            System.out.println("Snapshot: " + snapshot);
                            Assertions.assertNotNull(snapshot, "Snapshot should not be null");
                        });

                        CloseableIterable<FileScanTask> tasks = iceTable.newScan().planFiles();
                        tasks.forEach(task -> {
                            System.out.println("FileScanTask: " + task);
                            Assertions.assertNotNull(task, "FileScanTask should not be null");
                        });
                    } catch (Exception e) {
                        // System.out.println("Failed to load table: " + tableIdentifier.name() + ", " + e.getMessage());
                    }
                });
            });
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private Catalog initIcebergUnityCatalogRestCatalog() {
        Map<String, String> options = Maps.newHashMap();
        options.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        options.put(CatalogProperties.URI, uri);
        options.put("warehouse", "yy_unity_catalog");
        options.put("rest.auth.type", "oauth2");
        // options.put(OAuth2Properties.OAUTH2_SERVER_URI, "https://dbc-59918a85-6c3a.cloud.databricks.com/oidc/v1/token");
        // options.put(OAuth2Properties.CREDENTIAL, "xxx");
        options.put(OAuth2Properties.TOKEN, oauthToken);
        options.put(OAuth2Properties.SCOPE, "all-apis");
        Configuration conf = new Configuration();
        return CatalogUtil.buildIcebergCatalog("databricks_test", options, conf);
    }


    @Test
    public void rawTest() {
        Map<String, String> options = Maps.newHashMap();
        options.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        options.put(CatalogProperties.URI, uri);
        options.put("warehouse", "yy_unity_catalog");
        options.put("rest.auth.type", "oauth2");
        // options.put(OAuth2Properties.OAUTH2_SERVER_URI, "https://dbc-59918a85-6c3a.cloud.databricks.com/oidc/v1/token");
        // options.put(OAuth2Properties.CREDENTIAL, "xxxx");
        options.put(OAuth2Properties.TOKEN, oauthToken);
        // options.put(OAuth2Properties.SCOPE, "all-apis");
        Configuration conf = new Configuration();

        RESTCatalog catalog = new RESTCatalog();
        CatalogUtil.configureHadoopConf(catalog, conf);
        try {
            catalog.initialize("databricks_test", options);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateRestCatalog() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("uri", uri);
        properties.put("type", "iceberg");
        properties.put("warehouse", "yy_unity_catalog");
        properties.put("iceberg.catalog.type", "rest");
        properties.put("iceberg.rest.security.type", "oauth2");
        properties.put("iceberg.rest.oauth2.token", oauthToken);
        // properties.put("iceberg.rest.oauth2.scope", "all-apis");
        IcebergRestExternalCatalog catalog = new IcebergRestExternalCatalog(
                1, "databricks_test", null, properties, "test");
        catalog.setDefaultPropsIfMissing(false);
        Collection<DatabaseIf<?>> dbs = catalog.getAllDbs();
        for (DatabaseIf db : dbs) {
            ExternalDatabase extDb = (ExternalDatabase) db;
            System.out.println(extDb.getFullName());
            List tables = extDb.getTables();
            for (Object table : tables) {
                IcebergExternalTable tbl = (IcebergExternalTable) table;
                System.out.println(tbl.getName());
                System.out.println(tbl.location());
            }
        }
    }
}
