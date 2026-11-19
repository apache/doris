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

package org.apache.doris.nereids.lineage;

import org.apache.doris.datasource.CatalogIf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Map;

class LineageCatalogPropertyMaskingTest {
    @Test
    void catalogMetadataPreservesNativeFilesystemLocations() throws Exception {
        Map<String, String> raw = Map.of("gs.uri", "gs://example_bucket/warehouse",
                "hdfs.uri", "hdfs:///data.csv", "fs.defaultFS", "viewfs:///warehouse",
                "viewfs.uri", "viewfs://mount_table/warehouse");
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getProperties()).thenReturn(raw);
        Method sanitize = LineageUtils.class.getDeclaredMethod("sanitizeCatalogProperties", CatalogIf.class);
        sanitize.setAccessible(true);
        Assertions.assertEquals(raw, sanitize.invoke(null, catalog));
    }

    @Test
    void catalogMetadataPreservesSupportedLocalAndViewfsLocations() throws Exception {
        Map<String, String> raw = Map.of("driver_url", "/opt/example/libdriver.so",
                "uri", "file:/data/example/catalog.db", "fs.defaultFS", "viewfs://my-cluster");
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getProperties()).thenReturn(raw);
        Method sanitize = LineageUtils.class.getDeclaredMethod("sanitizeCatalogProperties", CatalogIf.class);
        sanitize.setAccessible(true);
        Assertions.assertEquals(raw, sanitize.invoke(null, catalog));
    }

    @Test
    void catalogMetadataNeverExportsCredentialBearingValues() throws Exception {
        Map<String, String> raw = Map.of(
                "jdbc_url", "jdbc:fixture://host.example.com;password=fixture-jdbc",
                "custom.password", "fixture-custom",
                "fs.s3a.secret.key", "fixture-hadoop",
                "uri", "https://fixture:fixture-userinfo@api.example.com/?token=fixture-token&region=east",
                "custom.connection", "jdbc:fixture://host.example.com;password=fixture-nested",
                "hosts", "https://host1.example:9200/,https://fixture:fixture-list-password@host2.example:9200/",
                "warehouse", "abfss://warehouse@account.dfs.core.windows.net/tables?token=fixture-azure&region=east",
                "endpoint", "host.example.com:9000");
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getProperties()).thenReturn(raw);
        Method sanitize = LineageUtils.class.getDeclaredMethod("sanitizeCatalogProperties", CatalogIf.class);
        sanitize.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> exported = (Map<String, String>) sanitize.invoke(null, catalog);

        Assertions.assertFalse(exported.containsKey("jdbc_url"));
        Assertions.assertFalse(exported.containsKey("custom.password"));
        Assertions.assertFalse(exported.containsKey("fs.s3a.secret.key"));
        Assertions.assertEquals("https://api.example.com/?token=*XXX&region=east", exported.get("uri"));
        Assertions.assertEquals("*XXX", exported.get("custom.connection"));
        Assertions.assertEquals("*XXX", exported.get("hosts"));
        Assertions.assertEquals("abfss://warehouse@account.dfs.core.windows.net/tables?token=*XXX&region=east",
                exported.get("warehouse"));
        Assertions.assertEquals("host.example.com:9000", exported.get("endpoint"));
        Assertions.assertFalse(exported.toString().contains("fixture-"));
        Assertions.assertTrue(raw.get("uri").contains("fixture-token"));
    }
}
