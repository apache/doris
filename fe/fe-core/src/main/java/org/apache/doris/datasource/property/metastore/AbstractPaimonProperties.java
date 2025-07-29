package org.apache.doris.datasource.property.metastore;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractPaimonProperties extends MetastoreProperties {
    @ConnectorProperty(
            names = {"warehouse"},
            required = false,
            description = "The location of the Iceberg warehouse. This is where the tables will be stored."
    )
    protected String warehouse;


    @Getter
    protected ExecutionAuthenticator executionAuthenticator = new ExecutionAuthenticator() {
    };

    private static final String USER_PROPERTY_PREFIX = "paimon.";

    protected AbstractPaimonProperties(Map<String, String> props) {
        super(props);
    }

    public abstract Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList);


    private void appendCatalogOptions(Options catalogOptions) {
        if (StringUtils.isNotBlank(warehouse)) {
            catalogOptions.set(CatalogOptions.WAREHOUSE.key(), warehouse);
        }
        catalogOptions.set(CatalogOptions.METASTORE.key(), getMetastoreType());
        origProps.forEach((k, v) -> {
            if (k.toLowerCase().startsWith(USER_PROPERTY_PREFIX)) {
                String newKey = k.substring(USER_PROPERTY_PREFIX.length());
                if (StringUtils.isNotBlank(newKey)) {
                    catalogOptions.set(newKey, v);
                }
            }
        });
    }


    /**
     * Build catalog options including common and subclass-specific ones.
     */
    public  Options buildCatalogOptions() {
        Options catalogOptions = new Options();
        appendCatalogOptions(catalogOptions);
        appendCustomCatalogOptions(catalogOptions);
        return catalogOptions;
    }
    


    protected abstract void appendCustomCatalogOptions(Options catalogOptions);

    protected abstract String getMetastoreType();
}
