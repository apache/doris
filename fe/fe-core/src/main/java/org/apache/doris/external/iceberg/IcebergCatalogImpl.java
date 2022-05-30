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
package org.apache.doris.external.iceberg;

import com.google.common.collect.Maps;
import org.apache.doris.catalog.IcebergProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Iceberg catalog implementation based on {@link org.apache.iceberg.catalog.Catalog}.
 */
public class IcebergCatalogImpl implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(IcebergCatalogImpl.class);

    private String catalogType;
    private Catalog icebergCatalog;

    @Override
    public void initialize(IcebergProperty icebergProperty) {
        this.catalogType = icebergProperty.getCatalogType();
        Map<String, String> properties = Maps.newHashMap(icebergProperty.getCatalogProperties());
        // Hadoop configuration
        Configuration conf = new Configuration();
        this.icebergCatalog = CatalogUtil.loadCatalog(
              icebergProperty.getCatalogImpl(), icebergProperty.getCatalogType(), properties, conf);
    }

    @Override
    public boolean tableExists(TableIdentifier tableIdentifier) {
        return icebergCatalog.tableExists(tableIdentifier);
    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) throws DorisIcebergException {
        try {
            return icebergCatalog.loadTable(tableIdentifier);
        } catch (Exception e) {
            LOG.warn("Failed to load table[{}] from database[{}], with error: {}",
                    tableIdentifier.name(), tableIdentifier.namespace(), e.getMessage());
            throw new DorisIcebergException(String.format("Failed to load table[%s] from database[%s]",
                    tableIdentifier.name(), tableIdentifier.namespace()), e);
        }
    }

    @Override
    public List<TableIdentifier> listTables(String db) throws DorisIcebergException {
        try {
            return icebergCatalog.listTables(Namespace.of(db));
        } catch (Exception e) {
            LOG.warn("Failed to list table in database[{}], with error: {}", db, e.getMessage());
            throw new DorisIcebergException(String.format("Failed to list table in database[%s]", db), e);
        }
    }

    @Override
    public boolean databaseExists(String db) {
        if (icebergCatalog instanceof SupportsNamespaces) {
            return ((SupportsNamespaces) icebergCatalog).namespaceExists(Namespace.of(db));
        } else {
            LOG.warn("Iceberg catalog: {} does not support namespace operation", catalogType);
            throw new DorisIcebergException(
                    String.format("Iceberg catalog: %s does not support namespace operation", catalogType));
        }
    }
}
