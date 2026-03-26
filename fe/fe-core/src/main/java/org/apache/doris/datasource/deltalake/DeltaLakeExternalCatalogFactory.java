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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.datasource.ExternalCatalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Factory for creating Delta Lake external catalog instances based on the
 * configured catalog type.
 *
 * <p>Supported catalog types:
 * <ul>
 *   <li>{@code hms} (default) - Uses Hive Metastore for metadata discovery</li>
 *   <li>{@code unity} - Uses Databricks Unity Catalog REST API for metadata discovery</li>
 * </ul>
 *
 * <p>The catalog type is determined by the {@code deltalake.catalog.type} property.
 * If not specified, defaults to {@code hms} for backward compatibility.
 */
public class DeltaLakeExternalCatalogFactory {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeExternalCatalogFactory.class);

    public static final String DELTALAKE_CATALOG_TYPE = "deltalake.catalog.type";
    public static final String DELTALAKE_HMS = "hms";
    public static final String DELTALAKE_UNITY = "unity";

    private DeltaLakeExternalCatalogFactory() {
        // utility class
    }

    /**
     * Create a Delta Lake external catalog based on the configured catalog type.
     *
     * @param catalogId unique identifier for the catalog
     * @param name      catalog name
     * @param resource  resource name (if any)
     * @param props     catalog properties
     * @param comment   catalog comment
     * @return the appropriate DeltaLakeExternalCatalog subclass
     */
    public static ExternalCatalog createCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        String catalogType = props.getOrDefault(DELTALAKE_CATALOG_TYPE, DELTALAKE_HMS).toLowerCase();
        LOG.info("Creating Delta Lake catalog '{}' with catalog type: {}", name, catalogType);
        switch (catalogType) {
            case DELTALAKE_HMS:
                return new DeltaLakeExternalCatalog(catalogId, name, resource, props, comment);
            case DELTALAKE_UNITY:
                return new DeltaLakeUnityExternalCatalog(catalogId, name, resource, props, comment);
            default:
                throw new IllegalArgumentException(
                        "Unknown deltalake.catalog.type: '" + catalogType + "'. "
                                + "Supported types are: " + DELTALAKE_HMS + ", " + DELTALAKE_UNITY);
        }
    }
}
