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

package org.apache.doris.datasource.iceberg.hive;

import org.apache.doris.datasource.iceberg.hadoop.IcebergHadoopFileIO;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shade.doris.hive.org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IcebergHiveCatalog extends HiveCompatibleCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergHiveCatalog.class);
    private ClientPool<IMetaStoreClient, TException> clients;
    private boolean listAllTables = false;

    public void initialize(String name, Map<String, String> properties) {
        if (this.conf == null) {
            LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
            this.conf = getConf();
        }
        if (properties.containsKey("uri")) {
            this.conf.set(HiveConf.ConfVars.METASTOREURIS.varname, properties.get("uri"));
        }
        if (properties.containsKey("warehouse")) {
            this.conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
                    LocationUtil.stripTrailingSlash(properties.get("warehouse")));
        }

        this.listAllTables = Boolean.parseBoolean(properties.getOrDefault("list-all-tables", "true"));
        String fileIOImpl = properties.get("io-impl");
        org.apache.hadoop.fs.FileSystem fs;
        try {
            fs = new DFSFileSystem(properties).rawFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        FileIO fileIO = fileIOImpl == null ? new IcebergHadoopFileIO(this.conf, fs)
                : CatalogUtil.loadFileIO(fileIOImpl, properties, this.conf);
        this.clients = new HCachedClientPool(name, this.conf, properties);
        super.initialize(name, fileIO, clients);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        try {
            Database databaseData = this.clients.run((client) ->
                    client.getDatabase(tableIdentifier.namespace().levels()[0]));
            if (databaseData.getLocationUri() != null) {
                return String.format("%s/%s", databaseData.getLocationUri(), tableIdentifier.name());
            }
        } catch (TException e) {
            throw new RuntimeException(String.format("Metastore operation failed for %s", tableIdentifier), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during commit", e);
        }

        String databaseLocation = this.databaseLocation(tableIdentifier.namespace().levels()[0]);
        return String.format("%s/%s", databaseLocation, tableIdentifier.name());
    }

    private String databaseLocation(String databaseName) {
        String warehouseLocation = this.conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
        Preconditions.checkNotNull(warehouseLocation,
                "Warehouse location is not set: hive.metastore.warehouse.dir=null");
        warehouseLocation = LocationUtil.stripTrailingSlash(warehouseLocation);
        return String.format("%s/%s.db", warehouseLocation, databaseName);
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta) {
        super.createNamespace(namespace, meta);
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        return super.listNamespaces(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        return super.loadNamespaceMetadata(namespace);
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        return super.dropNamespace(namespace);
    }

    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        return super.dropTable(identifier, purge);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return super.listTables(namespace);
    }
}
