package org.apache.doris.datasource.property.metastore;

import org.apache.iceberg.catalog.Catalog;

import java.util.Map;

public class IcebergS3TablesMetaStoreProperties extends AbstractIcebergProperties{
    protected IcebergS3TablesMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    protected Catalog initCatalog() {
        return null;
    }
}
