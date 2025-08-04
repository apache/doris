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

import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.hive.HiveCatalogOptions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PaimonHMSMetaStoreProperties extends AbstractPaimonProperties {

    private HMSBaseProperties hmsBaseProperties;

    private static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_KEY = "client-pool-cache.eviction-interval-ms";

    private static final String LOCATION_IN_PROPERTIES_KEY = "location-in-properties";

    @ConnectorProperty(
            names = {CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_KEY},
            required = false,
            description = "Setting the client's pool cache eviction interval(ms).")
    private long clientPoolCacheEvictionIntervalMs = TimeUnit.MINUTES.toMillis(5L);

    @ConnectorProperty(
            names = {LOCATION_IN_PROPERTIES_KEY},
            required = false,
            description = "Setting whether to use the location in the properties.")
    private boolean locationInProperties = false;


    @Override
    public String getPaimonCatalogType() {
        return PaimonExternalCatalog.PAIMON_DLF;
    }

    protected PaimonHMSMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        hmsBaseProperties = HMSBaseProperties.of(origProps);
        hmsBaseProperties.initAndCheckParams();
        this.executionAuthenticator = new HadoopExecutionAuthenticator(hmsBaseProperties.getHmsAuthenticator());
    }


    /**
     * Builds the Hadoop Configuration by adding hive-site.xml and storage-specific configs.
     */
    private Configuration buildHiveConfiguration(List<StorageProperties> storagePropertiesList) {
        Configuration conf = hmsBaseProperties.getHiveConf();

        for (StorageProperties sp : storagePropertiesList) {
            if (sp.getHadoopStorageConfig() != null) {
                conf.addResource(sp.getHadoopStorageConfig());
            }
        }
        return conf;
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        Configuration conf = buildHiveConfiguration(storagePropertiesList);
        buildCatalogOptions(storagePropertiesList);
        for (Map.Entry<String, String> entry : conf) {
            catalogOptions.set(entry.getKey(), entry.getValue());
        }
        CatalogContext catalogContext = CatalogContext.create(catalogOptions, conf);
        try {
            return executionAuthenticator.execute(() -> CatalogFactory.createCatalog(catalogContext));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Paimon catalog with HMS metastore", e);
        }

    }

    @Override
    protected String getMetastoreType() {
        //See org.apache.paimon.hive.HiveCatalogFactory
        return HiveCatalogOptions.IDENTIFIER;
    }

    @Override
    protected void appendCustomCatalogOptions() {
        catalogOptions.set(CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_KEY,
                String.valueOf(clientPoolCacheEvictionIntervalMs));
        catalogOptions.set(LOCATION_IN_PROPERTIES_KEY, String.valueOf(locationInProperties));
        catalogOptions.set("uri", hmsBaseProperties.getHiveMetastoreUri());
    }
}
