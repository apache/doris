package org.apache.doris.datasource.property.metastore;

import lombok.Getter;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;

import java.util.List;
import java.util.Map;

/**
 * @See org.apache.iceberg.CatalogProperties
 */
public abstract class AbstractIcebergProperties extends MetastoreProperties{
    
    @ConnectorProperty(
            names = {CatalogProperties.WAREHOUSE_LOCATION},
            required = false,
            description = "The location of the Iceberg warehouse. This is where the tables will be stored."
    )
    protected String warehouse;
    @Getter
    protected Catalog catalog;

    public abstract String getIcebergCatalogType();
    
    protected AbstractIcebergProperties(Type type, Map<String, String> props) {
        super(type, props);
    }

    protected AbstractIcebergProperties(Map<String, String> props) {
        super(props);
    }

    public final void initialize(String catalogName,List<StorageProperties> storagePropertiesList) {
        if (this.catalog == null) {
            this.catalog = initCatalog(catalogName,storagePropertiesList);
            if (this.catalog == null) {
                throw new IllegalStateException("Catalog must not be null after initialization.");
            }
        }
    }

    protected abstract Catalog initCatalog(String catalogName,List<StorageProperties> storagePropertiesList);
}
