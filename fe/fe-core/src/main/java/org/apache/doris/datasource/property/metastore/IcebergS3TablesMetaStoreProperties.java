package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.iceberg.s3tables.CustomAwsCredentialsProvider;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.HashMap;
import java.util.Map;

public class IcebergS3TablesMetaStoreProperties extends AbstractIcebergProperties {

    private S3Properties s3Properties;

    protected IcebergS3TablesMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        s3Properties = S3Properties.of(origProps);
        s3Properties.initNormalizeAndCheckProps();
    }

    @Override
    protected Catalog initCatalog() {
        S3TablesCatalog s3TablesCatalog = new S3TablesCatalog();
        Map<String, String> s3TablesCatalogProperties = new HashMap<>();
        s3TablesCatalogProperties.put("client.credentials-provider", CustomAwsCredentialsProvider.class.getName());
        s3TablesCatalogProperties.put("client.credentials-provider.s3.access-key-id", s3Properties.getAccessKey());
        s3TablesCatalogProperties.put("client.credentials-provider.s3.secret-access-key", s3Properties.getSecretKey());
        s3TablesCatalogProperties.put("client.credentials-provider.s3.session-token", s3Properties.getSessionToken());
        s3TablesCatalogProperties.put("client.region", s3Properties.getRegion());
        s3TablesCatalog.initialize(origProps.getOrDefault(EXTERNAL_CATALOG_NAME, ""), s3TablesCatalogProperties);

        return s3TablesCatalog;
    }
}
