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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.metastore.FileSystemMetaStoreProperties;
import org.apache.doris.connector.metastore.HmsMetaStoreProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class DeltaLakeCatalogPropertiesTest {
    @Test
    void defaultsToFilesystemAndRequiresWarehouse() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DeltaLakeCatalogProperties.of(Collections.emptyMap()));
        DeltaLakeCatalogProperties properties =
                DeltaLakeCatalogProperties.of(Collections.singletonMap("warehouse", "s3://bucket/warehouse"));
        Assertions.assertEquals("filesystem", properties.getCatalogType());
        Assertions.assertInstanceOf(FileSystemMetaStoreProperties.class, properties.getMetastore());
    }

    @Test
    void bindsHmsWithoutRequiringWarehouse() {
        Map<String, String> raw = new HashMap<>();
        raw.put("deltalake.catalog.type", "HMS");
        raw.put("hive.metastore.uris", "thrift://localhost:9083");
        DeltaLakeCatalogProperties properties = DeltaLakeCatalogProperties.of(raw);
        Assertions.assertInstanceOf(HmsMetaStoreProperties.class, properties.getMetastore());
        Assertions.assertEquals("hms", properties.getCatalogType());
        raw.remove("hive.metastore.uris");
        Assertions.assertThrows(IllegalArgumentException.class, () -> DeltaLakeCatalogProperties.of(raw));
    }

    @Test
    void rejectsUnknownMetastore() {
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> DeltaLakeCatalogProperties.of(Collections.singletonMap("deltalake.catalog.type", "glue")));
        Assertions.assertTrue(error.getMessage().contains("glue"));
    }

    @Test
    void bindsMappingOptionsAndCopiesProperties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("warehouse", "/warehouse");
        raw.put("enable.mapping.timestamp_tz", "true");
        raw.put("enable.mapping.varbinary", "true");
        DeltaLakeCatalogProperties properties = DeltaLakeCatalogProperties.of(raw);
        Assertions.assertTrue(properties.isMapTimestampTz());
        Assertions.assertTrue(properties.isMapVarbinary());
        raw.clear();
        Assertions.assertEquals("/warehouse", properties.getRaw().get("warehouse"));
    }
}
