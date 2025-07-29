package org.apache.doris.datasource.property.metastore;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import org.apache.doris.datasource.property.constants.PaimonProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.hive.HiveCatalogOptions;
import org.apache.paimon.options.Options;

import java.util.List;
import java.util.Map;

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
        buildCatalogOptions();
        
        return null;
    }

    @Override
    protected void appendCustomCatalogOptions(Options catalogOptions) {
        catalogOptions.put(PaimonProperties.PAIMON_METASTORE_CLIENT, ProxyMetaStoreClient.class.getName());
        catalogOptions.put(PaimonProperties.PAIMON_OSS_ENDPOINT,
                properties.get(PaimonProperties.PAIMON_OSS_ENDPOINT));
        catalogOptions.put(PaimonProperties.PAIMON_OSS_ACCESS_KEY,
                properties.get(PaimonProperties.PAIMON_OSS_ACCESS_KEY));
        catalogOptions.put(PaimonProperties.PAIMON_OSS_SECRET_KEY,
                properties.get(PaimonProperties.PAIMON_OSS_SECRET_KEY));
    }

    @Override
    protected String getMetastoreType() {
        return HiveCatalogOptions.IDENTIFIER;
    }
}
