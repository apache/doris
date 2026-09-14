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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergRestConnectivityTesterTest {

    @Test
    public void testUsesNormalCatalogInitializationPath() throws Exception {
        IcebergRestProperties properties = Mockito.mock(IcebergRestProperties.class);
        RESTCatalog catalog = Mockito.mock(RESTCatalog.class);
        StorageProperties storageProperties = Mockito.mock(StorageProperties.class);
        List<StorageProperties> storagePropertiesList = Collections.singletonList(storageProperties);
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/path");

        Mockito.when(properties.initializeCatalog("connectivity-test", storagePropertiesList)).thenReturn(catalog);
        Mockito.when(catalog.properties()).thenReturn(catalogProperties);

        IcebergRestConnectivityTester tester = new IcebergRestConnectivityTester(
                properties, storagePropertiesList);
        tester.testConnection();

        Mockito.verify(properties).initializeCatalog("connectivity-test", storagePropertiesList);
        Mockito.verify(catalog).listNamespaces();
        Mockito.verify(catalog).close();
        Assertions.assertEquals("s3://warehouse/path", tester.getTestLocation());
    }
}
