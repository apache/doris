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

import org.apache.doris.datasource.property.metastore.AbstractIcebergProperties;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;

import java.util.List;
import java.util.Map;

public class IcebergRestConnectivityTester extends AbstractIcebergConnectivityTester {
    // For Polaris REST catalog compatibility
    private static final String DEFAULT_BASE_LOCATION = "default-base-location";

    private String warehouseLocation;
    private final IcebergRestProperties restProperties;
    private final List<StorageProperties> storagePropertiesList;

    public IcebergRestConnectivityTester(AbstractIcebergProperties properties,
            List<StorageProperties> storagePropertiesList) {
        super(properties);
        this.restProperties = (IcebergRestProperties) properties;
        this.storagePropertiesList = storagePropertiesList;
    }

    @Override
    public String getTestType() {
        return "Iceberg REST";
    }

    @Override
    public String getErrorHint() {
        return "Please check Iceberg REST Catalog URI, authentication credentials (OAuth2 or SigV4), "
                + "warehouse (location, catalog name, or S3 Tables ARN), and endpoint connectivity";
    }

    @Override
    public void testConnection() throws Exception {
        try {
            Catalog catalog = restProperties.initializeCatalog("connectivity-test", storagePropertiesList);

            // Validate connection by listing namespaces.
            // This verifies authentication and warehouse configuration.
            ((SupportsNamespaces) catalog).listNamespaces();

            Map<String, String> mergedProps = restProperties.getRestSessionCatalog().properties();
            String location = mergedProps.get(CatalogProperties.WAREHOUSE_LOCATION);
            this.warehouseLocation = validateLocation(location);
            if (this.warehouseLocation == null) {
                location = mergedProps.get(DEFAULT_BASE_LOCATION);
                this.warehouseLocation = validateLocation(location);
            }
        } finally {
            // asCatalog() returns a non-closeable wrapper; the session catalog owns the REST/auth resources.
            restProperties.closeRestSessionCatalog();
        }
    }

    @Override
    public String getTestLocation() {
        // First try to use configured warehouse
        String location = validateLocation(properties.getWarehouse());
        if (location != null) {
            return location;
        }
        // If configured warehouse is not valid, fallback to REST API warehouse (already validated)
        return this.warehouseLocation;
    }
}
