package org.apache.doris.datasource.property.metastore;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;

import java.util.HashMap;
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
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        hmsBaseProperties = HMSBaseProperties.of(origProps);
        hmsBaseProperties.initAndCheckParams();
    }

    @Override
    protected Catalog initCatalog() {

        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(hmsBaseProperties.getHiveConf());
        Map<String, String> hmsProps = new HashMap<>();
        hmsProps.put(HiveCatalog.LIST_ALL_TABLES, String.valueOf(listAllTables));
        if (StringUtils.isNotBlank(warehouse)) {
            hmsProps.put("warehouse", warehouse);
        }
        hmsProps.put("uri", hmsBaseProperties.getHiveMetastoreUri());
        hiveCatalog.initialize(
                origProps.getOrDefault(EXTERNAL_CATALOG_NAME, "iceberg_hms_catalog"),
                hmsProps);
        return hiveCatalog;
    }
}
