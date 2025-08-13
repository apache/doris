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

import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.hive.HiveCatalogOptions;

import java.util.List;
import java.util.Map;

/**
 * PaimonAliyunDLFMetaStoreProperties
 *
 * <p>This class provides configuration support for using Apache Paimon with
 * Aliyun Data Lake Formation (DLF) as the metastore. Although DLF is not an
 * officially supported metastore type in Paimon, this implementation adapts
 * DLF by treating it as a Hive Metastore (HMS) underneath, enabling
 * interoperability with Paimon's HiveCatalog.
 *
 * <p>Key Characteristics:
 * <ul>
 *   <li>Internally uses HiveCatalog with custom HiveConf configured for Aliyun DLF.</li>
 *   <li>Relies on {@link ProxyMetaStoreClient} to bridge DLF compatibility.</li>
 *   <li>Requires Aliyun OSS as the storage backend. Other storage types are not
 *       currently verified for compatibility.</li>
 * </ul>
 *
 * <p>Note: This is an internal extension and not an officially supported Paimon
 * metastore type. Future compatibility should be validated when upgrading Paimon
 * or changing storage backends.
 *
 * @see org.apache.paimon.hive.HiveCatalog
 * @see org.apache.paimon.catalog.CatalogFactory
 * @see ProxyMetaStoreClient
 */
public class PaimonAliyunDLFMetaStoreProperties extends AbstractPaimonProperties {

    private AliyunDLFBaseProperties baseProperties;

    protected PaimonAliyunDLFMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        baseProperties = AliyunDLFBaseProperties.of(origProps);
    }

    private HiveConf buildHiveConf() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(DataLakeConfig.CATALOG_ACCESS_KEY_ID, baseProperties.dlfAccessKey);
        hiveConf.set(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET, baseProperties.dlfSecretKey);
        hiveConf.set(DataLakeConfig.CATALOG_ENDPOINT, baseProperties.dlfEndpoint);
        hiveConf.set(DataLakeConfig.CATALOG_REGION_ID, baseProperties.dlfRegion);
        hiveConf.set(DataLakeConfig.CATALOG_SECURITY_TOKEN, baseProperties.dlfSessionToken);
        hiveConf.set(DataLakeConfig.CATALOG_USER_ID, baseProperties.dlfUid);
        hiveConf.set(DataLakeConfig.CATALOG_ID, baseProperties.dlfCatalogId);
        hiveConf.set(DataLakeConfig.CATALOG_PROXY_MODE, baseProperties.dlfProxyMode);
        return hiveConf;
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        HiveConf hiveConf = buildHiveConf();
        buildCatalogOptions(storagePropertiesList);
        StorageProperties ossProps = storagePropertiesList.stream()
                .filter(sp -> sp.getType() == StorageProperties.Type.OSS)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Paimon DLF metastore requires OSS storage properties."));

        if (!(ossProps instanceof OSSProperties)) {
            throw new IllegalStateException("Expected OSSProperties type.");
        }
        OSSProperties ossProperties = (OSSProperties) ossProps;
        for (Map.Entry<String, String> entry : ossProperties.getHadoopStorageConfig()) {
            catalogOptions.set(entry.getKey(), entry.getValue());
        }
        hiveConf.addResource(ossProperties.getHadoopStorageConfig());
        CatalogContext catalogContext = CatalogContext.create(catalogOptions, hiveConf);
        return CatalogFactory.createCatalog(catalogContext);
    }

    @Override
    protected void appendCustomCatalogOptions() {
        catalogOptions.set("metastore.client.class", ProxyMetaStoreClient.class.getName());
    }

    @Override
    protected String getMetastoreType() {
        return HiveCatalogOptions.IDENTIFIER;
    }

    @Override
    public String getPaimonCatalogType() {
        return PaimonExternalCatalog.PAIMON_DLF;
    }
}
