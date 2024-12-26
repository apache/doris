package org.apache.doris.datasource;

import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class GlueTest {

    @Test
    public void testLoadWithException() throws Exception {
        final HiveMetaHookLoader DUMMY_HOOK_LOADER = t -> null;
        HiveConf hiveConf = new HiveConf();
        // hiveConf.set("client.credentials-provider",
        //         "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x");
        // hiveConf.set("client.credentials-provider.glue.access_key", "AKIASPAWQE3ITLULR24U");
        // hiveConf.set("client.credentials-provider.glue.secret_key", "");
        // hiveConf.set("aws.region", "ap-northeast-1");
        // hiveConf.set("aws.glue.endpoint", "https://glue.ap-northeast-1.amazonaws.com");
        // hiveConf.set("aws.glue.access-key", "AKIASPAWQE3ITLULR24U");
        // hiveConf.set("aws.glue.secret-key", "");
        // hiveConf.set("AWS_ACCESS_KEY", "AKIASPAWQE3ITLULR24U");
        // hiveConf.set("AWS_SECRET_KEY", "");
        Map<String, String> origProps = Maps.newHashMap();
        origProps.put("glue.endpoint" , "https://glue.ap-northeast-1.amazonaws.com");
        origProps.put("glue.access_key" , "AKIASPAWQE3ITLULR24U");
        origProps.put("glue.secret_key" , "");
        origProps.put("hive.metastore.type", "glue");
        Map<String, String> convertedProps = PropertyConverter.convertToMetaProperties(origProps);
        for (Map.Entry<String, String> entry : convertedProps.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
            hiveConf.set(entry.getKey(), entry.getValue());
        }

        IMetaStoreClient client = RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                AWSCatalogMetastoreClient.class.getName());
    }
}
