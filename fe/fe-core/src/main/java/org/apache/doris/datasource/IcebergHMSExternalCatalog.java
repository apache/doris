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

package org.apache.doris.datasource;

import org.apache.doris.catalog.HMSResource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergHMSExternalCatalog extends IcebergExternalCatalog {

    private static final String HMS_TAG = "hms";
    // doris memory catalog
    private final HMSExternalCatalog hmsExternalCatalog;
    // iceberg catalog client
    private HiveCatalog hiveCatalog;

    public IcebergHMSExternalCatalog(long catalogId, String name, String resource, String catalogType,
                                     Map<String, String> props) {
        super(catalogId, name, catalogType);
        hmsExternalCatalog = new HMSExternalCatalog(catalogId, name, resource, props);
    }

    @Override
    protected void init() {
        super.init();
        hmsExternalCatalog.init();
        hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
        Configuration conf = getConfiguration();
        hiveCatalog.setConf(conf);
        // initialize hive catalog
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(HMSResource.HIVE_METASTORE_URIS, hmsExternalCatalog.getHiveMetastoreUris());
        catalogProperties.put(CatalogProperties.URI, hmsExternalCatalog.getHiveMetastoreUris());
        hiveCatalog.initialize(HMS_TAG, catalogProperties);
    }

    private Configuration getConfiguration() {
        Configuration conf = new HdfsConfiguration();
        Map<String, String> catalogProperties = hmsExternalCatalog.getCatalogProperty().getHadoopProperties();
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    @Override
    public String getResource() {
        return hmsExternalCatalog.getResource();
    }

    @Override
    public Table getIcebergTable(String dbName, String tblName) {
        makeSureInitialized();
        return hiveCatalog.loadTable(TableIdentifier.of(dbName, tblName));
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx)   {
        return hmsExternalCatalog.listDatabaseNames(ctx);
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        return hmsExternalCatalog.listTableNames(ctx, dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return hiveCatalog.tableExists(TableIdentifier.of(dbName, tblName));
    }

    @Override
    protected void initLocalObjectsImpl() {}

    public HMSExternalCatalog getHmsExternalCatalog() {
        return hmsExternalCatalog;
    }
}
