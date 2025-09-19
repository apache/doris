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

import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AbstractIcebergPropertiesTest {

    private static class TestIcebergProperties extends AbstractIcebergProperties {
        private final Catalog catalogToReturn;
        private Map<String, String> capturedCatalogProps;

        TestIcebergProperties(Map<String, String> props, Catalog catalogToReturn) {
            super(props);
            this.catalogToReturn = catalogToReturn;
        }

        @Override
        public String getIcebergCatalogType() {
            return "test";
        }

        @Override
        protected Catalog initCatalog(String catalogName,
                                      Map<String, String> catalogProps,
                                      List<StorageProperties> storagePropertiesList) {
            // Capture the catalogProps for verification
            this.capturedCatalogProps = new HashMap<>(catalogProps);
            return catalogToReturn;
        }

        Map<String, String> getCapturedCatalogProps() {
            return capturedCatalogProps;
        }
    }

    @Test
    void testInitializeCatalogWithWarehouse() {
        Catalog mockCatalog = Mockito.mock(Catalog.class);
        Mockito.when(mockCatalog.name()).thenReturn("mocked-catalog");
        Map<String, String> props = new HashMap<>();
        props.put("k1", "v1");
        TestIcebergProperties properties = new TestIcebergProperties(props, mockCatalog);
        properties.warehouse = "s3://bucket/warehouse";
        Catalog result = properties.initializeCatalog("testCatalog", Collections.emptyList());
        Assertions.assertNotNull(result);
        Assertions.assertEquals("mocked-catalog", result.name());
        // Verify that warehouse is included in catalogProps
        Assertions.assertTrue(properties.getCapturedCatalogProps()
                .containsKey(CatalogProperties.WAREHOUSE_LOCATION));
        Assertions.assertEquals("s3://bucket/warehouse",
                properties.getCapturedCatalogProps().get(CatalogProperties.WAREHOUSE_LOCATION));
        Assertions.assertNotNull(properties.getExecutionAuthenticator());
    }

    @Test
    void testInitializeCatalogWithoutWarehouse() {
        Catalog mockCatalog = Mockito.mock(Catalog.class);
        Mockito.when(mockCatalog.name()).thenReturn("no-warehouse");
        TestIcebergProperties properties = new TestIcebergProperties(new HashMap<>(), mockCatalog);
        properties.warehouse = null;
        Catalog result = properties.initializeCatalog("testCatalog", Collections.emptyList());
        Assertions.assertNotNull(result);
        Assertions.assertEquals("no-warehouse", result.name());
        // Verify that warehouse key is not present
        Assertions.assertFalse(properties.getCapturedCatalogProps()
                .containsKey(CatalogProperties.WAREHOUSE_LOCATION));
    }

    @Test
    void testInitializeCatalogThrowsWhenNull() {
        AbstractIcebergProperties properties = new AbstractIcebergProperties(new HashMap<>()) {
            @Override
            public String getIcebergCatalogType() {
                return "test";
            }

            @Override
            protected Catalog initCatalog(String catalogName,
                                          Map<String, String> catalogProps,
                                          List<StorageProperties> storagePropertiesList) {
                return null; // Simulate a failure case
            }
        };

        IllegalStateException ex = Assertions.assertThrows(
                IllegalStateException.class,
                () -> properties.initializeCatalog("testCatalog", Collections.emptyList())
        );
        Assertions.assertEquals("Catalog must not be null after initialization.", ex.getMessage());
    }

    @Test
    void testExecutionAuthenticatorNotNull() {
        Catalog mockCatalog = Mockito.mock(Catalog.class);
        TestIcebergProperties properties = new TestIcebergProperties(new HashMap<>(), mockCatalog);
        Assertions.assertNotNull(properties.executionAuthenticator);
    }
}
