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

import org.apache.doris.catalog.IcebergProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HiveCatalog of Iceberg
 */
public class HiveCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(HiveCatalog.class);

    private org.apache.iceberg.hive.HiveCatalog hiveCatalog;

    public HiveCatalog() {
        hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
    }

    @Override
    public void initialize(IcebergProperty icebergProperty) {
        // set hadoop conf
        Configuration conf = new Configuration();
        hiveCatalog.setConf(conf);
        // initialize hive catalog
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("uri", icebergProperty.getHiveMetastoreUris());
        hiveCatalog.initialize("hive", catalogProperties);
    }

    @Override
    public boolean tableExists(TableIdentifier tableIdentifier) {
        return hiveCatalog.tableExists(tableIdentifier);
    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) throws DorisIcebergException {
        try {
            return hiveCatalog.loadTable(tableIdentifier);
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
            return hiveCatalog.listTables(Namespace.of(db));
        } catch (Exception e) {
            LOG.warn("Failed to list table in database[{}], with error: {}", db, e.getMessage());
            throw new DorisIcebergException(String.format("Failed to list table in database[%s]", db), e);
        }
    }

    @Override
    public boolean databaseExists(String db) {
        return hiveCatalog.namespaceExists(Namespace.of(db));
    }
}
