package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.options.Options;

import java.util.List;
import java.util.Map;

public class PaimonFileSystemMetaStoreProperties extends AbstractPaimonProperties{
    protected PaimonFileSystemMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        return null;
    }

    @Override
    protected void appendCustomCatalogOptions(Options catalogOptions) {

    }

    @Override
    protected String getMetastoreType() {
        return "";
    }
}
