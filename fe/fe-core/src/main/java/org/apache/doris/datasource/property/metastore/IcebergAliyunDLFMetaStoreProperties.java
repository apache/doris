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

import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.dlf.DLFCatalog;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergAliyunDLFMetaStoreProperties extends AbstractIcebergProperties {

    private AliyunDLFBaseProperties baseProperties;


    protected IcebergAliyunDLFMetaStoreProperties(Map<String, String> props) {
        super(props);
        super.initNormalizeAndCheckProps();
        baseProperties = AliyunDLFBaseProperties.of(origProps);
    }

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_DLF;
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {

        DLFCatalog dlfCatalog = new DLFCatalog();
        // @see com.aliyun.datalake.metastore.hive.common.utils.ConfigUtils
        Configuration conf = new Configuration();
        conf.set(DataLakeConfig.CATALOG_ACCESS_KEY_ID, baseProperties.dlfAccessKey);
        conf.set(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET, baseProperties.dlfSecretKey);
        conf.set(DataLakeConfig.CATALOG_ENDPOINT, baseProperties.dlfEndpoint);
        conf.set(DataLakeConfig.CATALOG_REGION_ID, baseProperties.dlfRegion);
        conf.set(DataLakeConfig.CATALOG_SECURITY_TOKEN, baseProperties.dlfSessionToken);
        conf.set(DataLakeConfig.CATALOG_USER_ID, baseProperties.dlfUid);
        conf.set(DataLakeConfig.CATALOG_ID, baseProperties.dlfCatalogId);
        conf.set(DataLakeConfig.CATALOG_PROXY_MODE, baseProperties.dlfProxyMode);
        conf.set("hive.metastore.type", "dlf");
        conf.set("type", "hms");
        dlfCatalog.setConf(conf);
        Map<String, String> catalogProperties = new HashMap<>(origProps);
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        dlfCatalog.initialize(catalogName, catalogProperties);
        return dlfCatalog;
    }
}
