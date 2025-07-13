package org.apache.doris.datasource.property.metastore;

import org.apache.iceberg.catalog.Catalog;

import java.util.Map;

public class IcebergHMSMetaStoreProperties extends AbstractIcebergProperties{
    protected IcebergHMSMetaStoreProperties(Type type, Map<String, String> props) {
        super(type, props);
    }

    @Override
    protected Catalog initCatalog() {
        return null;
    }
}
