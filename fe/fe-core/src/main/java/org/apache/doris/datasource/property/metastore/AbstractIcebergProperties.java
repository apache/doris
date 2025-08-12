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

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.StorageProperties;

import lombok.Getter;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;

import java.util.List;
import java.util.Map;

/**
 * @See org.apache.iceberg.CatalogProperties
 */
public abstract class AbstractIcebergProperties extends MetastoreProperties {

    @ConnectorProperty(
            names = {CatalogProperties.WAREHOUSE_LOCATION},
            required = false,
            description = "The location of the Iceberg warehouse. This is where the tables will be stored."
    )
    protected String warehouse;

    @Getter
    protected ExecutionAuthenticator executionAuthenticator = new ExecutionAuthenticator(){};

    public abstract String getIcebergCatalogType();

    protected AbstractIcebergProperties(Map<String, String> props) {
        super(Type.ICEBERG, props);
    }

    /**
     * Iceberg Catalog instance responsible for managing metadata and lifecycle of Iceberg tables.
     * <p>
     * The Catalog is a core component in Iceberg that handles table registration,
     * loading, and metadata management.
     * <p>
     * It is assigned during initialization via the `initialize` method,
     * which calls the abstract `initCatalog` method to create a concrete Catalog instance.
     * This instance is typically configured based on the provided catalog name
     * and a list of storage properties.
     * <p>
     * After initialization, the catalog must not be null; otherwise,
     * an IllegalStateException is thrown to ensure that subsequent operations
     * on Iceberg tables have a valid Catalog reference.
     * <p>
     * Different Iceberg Catalog implementations (such as HadoopCatalog, HiveCatalog,
     * RESTCatalog, etc.) can be flexibly switched and configured
     * by subclasses overriding the `initCatalog` method.
     * <p>
     * This field is used to perform metadata operations like creating, querying,
     * and deleting Iceberg tables.
     */
    public abstract Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList);
}
