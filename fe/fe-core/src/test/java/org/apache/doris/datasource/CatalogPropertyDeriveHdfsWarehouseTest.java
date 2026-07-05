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

package org.apache.doris.datasource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link CatalogProperty#deriveHdfsDefaultFsFromWarehouse(Map)} — the neutral, connector-agnostic
 * warehouse -> fs.defaultFS bridge that takes over for an SPI catalog with no fe-core MetastoreProperties
 * (e.g. a native iceberg catalog once its property cluster moved to the connector). Verbatim behavior parity
 * with the former {@code IcebergFileSystemMetaStoreProperties.getDerivedStorageProperties}: an HA-nameservice
 * {@code warehouse=hdfs://<ns>/path} configured with no inline {@code fs.defaultFS} must still bind HDFS with
 * that nameservice; non-hdfs and blank warehouses derive nothing; a blank nameservice fails loud.
 */
public class CatalogPropertyDeriveHdfsWarehouseTest {

    private static Map<String, String> warehouse(String value) {
        Map<String, String> m = new HashMap<>();
        if (value != null) {
            m.put("warehouse", value);
        }
        return m;
    }

    @Test
    public void haNameserviceWarehouseBridgesToDefaultFs() {
        Assertions.assertEquals(Collections.singletonMap("fs.defaultFS", "hdfs://myns"),
                CatalogProperty.deriveHdfsDefaultFsFromWarehouse(warehouse("hdfs://myns/warehouse")));
    }

    @Test
    public void hostPortWarehouseBridgesToDefaultFs() {
        Assertions.assertEquals(Collections.singletonMap("fs.defaultFS", "hdfs://nn-host:8020"),
                CatalogProperty.deriveHdfsDefaultFsFromWarehouse(warehouse("hdfs://nn-host:8020/warehouse")));
    }

    @Test
    public void nonHdfsWarehouseDerivesNothing() {
        // file:// and s3:// warehouses derive nothing: the bridge is hdfs-only (startsWith "hdfs:").
        Assertions.assertTrue(
                CatalogProperty.deriveHdfsDefaultFsFromWarehouse(warehouse("file:///tmp/wh")).isEmpty());
        Assertions.assertTrue(
                CatalogProperty.deriveHdfsDefaultFsFromWarehouse(warehouse("s3://bucket/wh")).isEmpty());
    }

    @Test
    public void blankOrAbsentWarehouseDerivesNothing() {
        Assertions.assertTrue(CatalogProperty.deriveHdfsDefaultFsFromWarehouse(warehouse(null)).isEmpty());
        Assertions.assertTrue(CatalogProperty.deriveHdfsDefaultFsFromWarehouse(warehouse("")).isEmpty());
        Assertions.assertTrue(CatalogProperty.deriveHdfsDefaultFsFromWarehouse(warehouse("   ")).isEmpty());
    }

    @Test
    public void blankNameserviceFailsLoud() {
        // hdfs:///path has no nameservice authority -> the bridge cannot pick one, so it must fail loud
        // rather than silently bind an empty fs.defaultFS.
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CatalogProperty.deriveHdfsDefaultFsFromWarehouse(warehouse("hdfs:///warehouse")));
        Assertions.assertTrue(e.getMessage().contains("name service is required"), e.getMessage());
    }
}
