package org.apache.doris.datasource.property.metastore;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.datasource.property.constants.PaimonProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.hive.HiveCatalogOptions;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.options.description.TextElement;

import java.util.HashMap;
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
        Configuration conf = new Configuration();
        conf.addResource(hmsBaseProperties.getHiveConf());
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
        Options catalogOptions = buildCatalogOptions();
        CatalogContext catalogContext = CatalogContext.create(catalogOptions, conf);
        return CatalogFactory.createCatalog(catalogContext);
    }

    @Override
    protected String getMetastoreType() {
        //See org.apache.paimon.hive.HiveCatalogFactory
        return HiveCatalogOptions.IDENTIFIER;
    }

    @Override
    protected void appendCustomCatalogOptions(Options catalogOptions) {
        catalogOptions.set(CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_KEY, String.valueOf(clientPoolCacheEvictionIntervalMs));
        catalogOptions.set(LOCATION_IN_PROPERTIES_KEY, String.valueOf(locationInProperties));
    }
}
