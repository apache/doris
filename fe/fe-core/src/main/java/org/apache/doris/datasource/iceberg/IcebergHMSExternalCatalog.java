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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.nereids.exceptions.AnalysisException;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;

import java.io.IOException;
import java.util.Map;

public class IcebergHMSExternalCatalog extends IcebergExternalCatalog {

    private HadoopAuthenticator authenticator;

    public IcebergHMSExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public synchronized HadoopAuthenticator getAuthenticator() {
        if (authenticator == null) {
            AuthenticationConfig config = AuthenticationConfig.getKerberosConfig(getConfiguration());
            authenticator = HadoopAuthenticator.getHadoopAuthenticator(config);
        }
        return authenticator;
    }

    @Override
    protected void initCatalog() {
        icebergCatalogType = ICEBERG_HMS;
        HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
        hiveCatalog.setConf(getConfiguration());
        // initialize hive catalog
        Map<String, String> catalogProperties = catalogProperty.getProperties();
        String metastoreUris = catalogProperty.getOrDefault(HMSProperties.HIVE_METASTORE_URIS, "");
        catalogProperties.put(CatalogProperties.URI, metastoreUris);
        try {
            getAuthenticator().doAsNoReturn(() -> hiveCatalog.initialize(icebergCatalogType, catalogProperties));
        } catch (IOException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        catalog = hiveCatalog;
    }

    public Table loadTable(TableIdentifier of) {
        //        // todo
        //        HiveOperations operations = new HiveOperations(
        //            FileSystemFactory.get()),
        //            catalog.getMetastore(),
        //            database,
        //            table,
        //            location)
        //        return new BaseTable(operations, of.toString());
        Table tbl;
        try {
            tbl = getAuthenticator().doAs(() -> getCatalog().loadTable(of));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Map<String, String> extProps = getProperties();
        initIcebergTableFileIO(tbl, extProps);
        return tbl;
    }
}

