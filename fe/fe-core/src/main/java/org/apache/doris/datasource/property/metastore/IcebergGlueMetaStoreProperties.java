package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;

import java.util.Map;

public class IcebergGlueMetaStoreProperties extends AbstractIcebergProperties {

   
    public AWSGlueMetaStoreBaseProperties glueProperties;
    
    public S3Properties s3Properties;
    protected IcebergGlueMetaStoreProperties(Type type, Map<String, String> props) {
        super(type, props);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        glueProperties = AWSGlueMetaStoreBaseProperties.of(origProps);
        glueProperties.check();
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
       
    }

    @Override
    protected Catalog initCatalog() {
        GlueCatalog catalog = new GlueCatalog();
        
        
        return null;
    }
}
