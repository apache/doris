package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;

import java.util.HashMap;
import java.util.Map;

public class IcebergGlueMetaStoreProperties extends AbstractIcebergProperties {

    
   
    public AWSGlueMetaStoreBaseProperties glueProperties;
    
    public S3Properties s3Properties;

    public IcebergGlueMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        //System.setProperty("aws.region", "ap-northeast-1");
        glueProperties = AWSGlueMetaStoreBaseProperties.of(origProps);
        glueProperties.checkAndInit();
        s3Properties = S3Properties.of(origProps);
        s3Properties.initNormalizeAndCheckProps();
    }
    
    
    private void initS3Param(Map<String,String> props) {
         //S3FileIo
        props.put(S3FileIOProperties.ACCESS_KEY_ID, s3Properties.getAccessKey());
        props.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Properties.getSecretKey());
        props.put(S3FileIOProperties.ENDPOINT, s3Properties.getEndpoint());
        props.put(S3FileIOProperties.PATH_STYLE_ACCESS, s3Properties.getUsePathStyle());
        props.put(S3FileIOProperties.SESSION_TOKEN, s3Properties.getSessionToken());
    }
    
    private void initGlueParam(Map<String,String> props) {
       props.put(AwsProperties.GLUE_CATALOG_ENDPOINT, glueProperties.glueEndpoint);
        props.put("client.credentials-provider", "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x");
        props.put("client.credentials-provider.glue.access_key", glueProperties.glueAccessKey);
        props.put("client.credentials-provider.glue.secret_key", glueProperties.glueSecretKey);
        props.put("aws.catalog.credentials.provider.factory.class", "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProviderFactory");


    }

    @Override
    protected Catalog initCatalog() {
        GlueCatalog catalog = new GlueCatalog();
        Map<String,String> props = new HashMap<>();
        initS3Param(props);
        initGlueParam(props);
        //props.put("iceberg.catalog.glue.client.region", glueProperties.glueRegion);
        props.put("client.region", glueProperties.glueRegion);
        catalog.initialize("glue", props);
        return catalog;
    }
}
