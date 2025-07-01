package org.apache.doris.datasource.property.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

public class HMSGlueMetaStoreProperties extends AWSGlueMetaStoreBaseProperties{
    
    /**
     * Base constructor for subclasses to initialize the common state.
     *
     * @param type      metastore type
     * @param origProps original configuration
     */
    protected HMSGlueMetaStoreProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }
    
    private void initHiveConf(){
        HiveConf hiveConf = new HiveConf();
        
    }
}
