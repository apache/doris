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

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.util.Map;
import java.util.regex.Pattern;

public class IcebergRestConnectivityTester extends AbstractIcebergConnectivityTester {
    // For Polaris REST catalog compatibility
    private static final String DEFAULT_BASE_LOCATION = "default-base-location";
    private static final Pattern LOCATION_PATTERN = Pattern.compile("^(s3|s3a)://.+");

    private String warehouseLocation;

    public IcebergRestConnectivityTester(AbstractIcebergProperties properties) {
        super(properties);
    }

    @Override
    public String getTestType() {
        return "Iceberg REST";
    }

    @Override
    public void testConnection() throws Exception {
        Map<String, String> restProps = ((IcebergRestProperties) properties).getIcebergRestCatalogProperties();

        try (RESTSessionCatalog catalog = new RESTSessionCatalog()) {
            catalog.initialize("connectivity-test", restProps);

            Map<String, String> mergedProps = catalog.properties();
            String location = mergedProps.get(CatalogProperties.WAREHOUSE_LOCATION);
            this.warehouseLocation = validateLocation(location);
            if (this.warehouseLocation == null) {
                location = mergedProps.get(DEFAULT_BASE_LOCATION);
                this.warehouseLocation = validateLocation(location);
            }
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

    /**
     * Validate if the given location is a valid storage URI.
     * This method is specific to IcebergRestConnectivityTester because it needs to
     * validate warehouse locations returned from REST API.
     */
    private String validateLocation(String location) {
        if (StringUtils.isNotBlank(location) && LOCATION_PATTERN.matcher(location).matches()) {
            return location;
        }
        return null;
    }
}
