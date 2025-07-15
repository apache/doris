package org.apache.doris.datasource.property.metastore;

import org.apache.iceberg.catalog.Catalog;

import java.util.Map;

public class IcebergFileSystemMetaStoreProperties extends AbstractIcebergProperties{
    protected IcebergFileSystemMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    protected Catalog initCatalog() {
        return null;
    }
}
