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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Design S8: the iceberg connector owns the hadoop-catalog {@code warehouse -> fs.defaultFS} storage derivation
 * that used to live in fe-core's {@code IcebergFileSystemMetaStoreProperties.getDerivedStorageProperties}.
 * Verifies verbatim parity with the former bridge (HA-nameservice / host:port warehouse -> fs.defaultFS;
 * non-hdfs / blank warehouse derives nothing; a blank nameservice fails loud) AND — the parity-preserving
 * addition — that ONLY the hadoop flavor derives: rest/hms/glue/... contribute nothing even with an hdfs
 * warehouse (the legacy override lived only on the hadoop flavor).
 */
public class IcebergConnectorDeriveStoragePropertiesTest {

    private static Map<String, String> props(String catalogType, String warehouse) {
        Map<String, String> m = new HashMap<>();
        if (catalogType != null) {
            m.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, catalogType);
        }
        if (warehouse != null) {
            m.put(IcebergConnectorProperties.WAREHOUSE, warehouse);
        }
        return m;
    }

    @Test
    public void hadoopHaNameserviceWarehouseBridgesToDefaultFs() {
        Assertions.assertEquals(Collections.singletonMap("fs.defaultFS", "hdfs://myns"),
                IcebergConnector.deriveStorageDefaults(props("hadoop", "hdfs://myns/warehouse")));
    }

    @Test
    public void hadoopHostPortWarehouseBridgesToDefaultFs() {
        Assertions.assertEquals(Collections.singletonMap("fs.defaultFS", "hdfs://nn-host:8020"),
                IcebergConnector.deriveStorageDefaults(props("hadoop", "hdfs://nn-host:8020/warehouse")));
    }

    @Test
    public void hadoopNonHdfsWarehouseDerivesNothing() {
        // file:// and s3:// warehouses derive nothing: the bridge is hdfs-only (startsWith "hdfs:").
        Assertions.assertTrue(
                IcebergConnector.deriveStorageDefaults(props("hadoop", "file:///tmp/wh")).isEmpty());
        Assertions.assertTrue(
                IcebergConnector.deriveStorageDefaults(props("hadoop", "s3://bucket/wh")).isEmpty());
    }

    @Test
    public void hadoopBlankOrAbsentWarehouseDerivesNothing() {
        Assertions.assertTrue(IcebergConnector.deriveStorageDefaults(props("hadoop", null)).isEmpty());
        Assertions.assertTrue(IcebergConnector.deriveStorageDefaults(props("hadoop", "")).isEmpty());
        Assertions.assertTrue(IcebergConnector.deriveStorageDefaults(props("hadoop", "   ")).isEmpty());
    }

    @Test
    public void hadoopBlankNameserviceFailsLoud() {
        // hdfs:///path has no nameservice authority -> fail loud rather than bind an empty fs.defaultFS.
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergConnector.deriveStorageDefaults(props("hadoop", "hdfs:///warehouse")));
        Assertions.assertTrue(e.getMessage().contains("name service is required"), e.getMessage());
    }

    @Test
    public void nonHadoopFlavorNeverDerivesEvenWithHdfsWarehouse() {
        // Parity with the legacy override, which only the hadoop (filesystem) flavor carried. An hdfs warehouse
        // on a rest/hms/glue/dlf/jdbc/s3tables catalog must NOT synthesize fs.defaultFS.
        Assertions.assertTrue(
                IcebergConnector.deriveStorageDefaults(props("rest", "hdfs://myns/warehouse")).isEmpty());
        Assertions.assertTrue(
                IcebergConnector.deriveStorageDefaults(props("hms", "hdfs://myns/warehouse")).isEmpty());
        Assertions.assertTrue(
                IcebergConnector.deriveStorageDefaults(props("glue", "hdfs://myns/warehouse")).isEmpty());
    }

    @Test
    public void missingCatalogTypeDerivesNothing() {
        Assertions.assertTrue(
                IcebergConnector.deriveStorageDefaults(props(null, "hdfs://myns/warehouse")).isEmpty());
    }
}
