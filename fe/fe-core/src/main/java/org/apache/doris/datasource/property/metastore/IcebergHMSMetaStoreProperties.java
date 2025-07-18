package org.apache.doris.datasource.property.metastore;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @See org.apache.iceberg.hive.HiveCatalog
 */
public class IcebergHMSMetaStoreProperties extends AbstractIcebergProperties {
    public IcebergHMSMetaStoreProperties(Map<String, String> props) {
        super(props);
    }
    @ConnectorProperty(
            names = {HiveCatalog.LIST_ALL_TABLES},
            required = false,
            description = "Whether to list all tables in the catalog. If true, the catalog will list all tables in the "
                    + "catalog, otherwise it will only list the tables that are registered in the catalog.")
    private boolean listAllTables = true;

    private HMSBaseProperties hmsBaseProperties;

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_HMS;
    }
    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        hmsBaseProperties = HMSBaseProperties.of(origProps);
        hmsBaseProperties.initAndCheckParams();
    }

    @Override
    protected Catalog initCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        checkInitialized();

        Configuration conf = buildHiveConfiguration(storagePropertiesList);
        Map<String, String> catalogProps = buildCatalogProperties();

        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(conf);

        try {
            hiveCatalog.initialize(catalogName, catalogProps);
            return hiveCatalog;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize HiveCatalog for Iceberg. "
                    + "CatalogName=" + catalogName + ", warehouse=" + warehouse, e);
        }
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

    /**
     * Constructs HiveCatalog's property map.
     */
    private Map<String, String> buildCatalogProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(HiveCatalog.LIST_ALL_TABLES, String.valueOf(listAllTables));

        if (StringUtils.isNotBlank(warehouse)) {
            props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }

        props.put("uri", hmsBaseProperties.getHiveMetastoreUri());
        props.putAll(origProps); // Keep at end to allow override, but risky if overlaps exist
        return props;
    }

    private void checkInitialized() {
        if (hmsBaseProperties == null) {
            throw new IllegalStateException("HMS properties not initialized. You must call initNormalizeAndCheckProps() first.");
        }
    }
}