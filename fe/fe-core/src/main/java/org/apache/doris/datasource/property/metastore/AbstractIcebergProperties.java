package org.apache.doris.datasource.property.metastore;

import org.apache.iceberg.catalog.Catalog;

import java.util.Map;

public abstract class AbstractIcebergProperties extends MetastoreProperties{

    public static final String EXTERNAL_CATALOG_NAME = "external_catalog.name";
    
    
    protected Catalog catalog;

    
    protected AbstractIcebergProperties(Type type, Map<String, String> props) {
        super(type, props);
    }

    protected AbstractIcebergProperties(Map<String, String> props) {
        super(props);
    }

    public final void initialize() {
        if (this.catalog == null) {
            this.catalog = initCatalog();
            if (this.catalog == null) {
                throw new IllegalStateException("Catalog must not be null after initialization.");
            }
        }
    }

    protected abstract Catalog initCatalog();
}
